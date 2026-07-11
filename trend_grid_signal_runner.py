# coding: utf-8
"""
External trend-grid signal runner.

This script moves the signal calculation out of Big QMT. It talks to the
Big QMT helper over local HTTP, while the helper remains responsible for
executing passorder/cancel_order inside handlebar(ContextInfo).
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import ssl
import struct
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


HELPER_URL = "http://127.0.0.1:9000"
DATA_CENTER_URL = "http://192.168.251.1:18000"
DATA_CENTER_WS_URL = "ws://192.168.251.1:18000/ws/quote"
ENV_FILE = ".env.bigqmt"
STATE_FILE = "logs/trend_grid_signal_state.json"

STRATEGY_NAME = "trend_grid_external"
EXECUTION_PERIOD = "1m"

B_BASE_QTY = 200
MIN_SELL_QTY = 200
MIN_BUY_QTY = 200

BUY_PRICE_TYPE = 0
SELL_PRICE_TYPE = 7


@dataclass
class Bar:
  key: str
  open: float
  high: float
  low: float
  close: float


@dataclass
class Tick:
  key: str
  price: float


@dataclass
class StrategyState:
  trade_date: str = ""
  prev_close: float = 0.0
  pending_peak: float = 0.0
  pending_valley: float = 0.0
  highest_peak: float = 0.0
  lowest_valley: float = 0.0
  last_processed_bar: str = ""
  order_count: int = 0

  def reset_for_day(self, open_price: float, trade_date: str) -> None:
    self.trade_date = trade_date
    self.prev_close = open_price
    self.pending_peak = 0.0
    self.pending_valley = 0.0
    self.highest_peak = open_price
    self.lowest_valley = open_price
    self.last_processed_bar = ""


class HelperClient:
  def __init__(self, base_url: str, password: str, timeout: float = 30.0) -> None:
    self.base_url = base_url.rstrip("/")
    self.password = password
    self.timeout = timeout

  def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
      self.base_url + path,
      data=body,
      headers={
        "Content-Type": "application/json",
        "X-BulletTrade-Password": self.password,
      },
      method="POST",
    )
    try:
      with urllib.request.urlopen(request, timeout=self.timeout) as response:
        return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
      text = exc.read().decode("utf-8", errors="replace")
      raise RuntimeError("helper HTTP %s: %s" % (exc.code, text)) from exc

  def get(self, path: str) -> Dict[str, Any]:
    request = urllib.request.Request(
      self.base_url + path,
      headers={"X-BulletTrade-Password": self.password},
      method="GET",
    )
    with urllib.request.urlopen(request, timeout=self.timeout) as response:
      return json.loads(response.read().decode("utf-8"))

  def positions(self) -> List[Dict[str, Any]]:
    response = self.post("/positions", {})
    return list(_require_ok(response, "positions").get("positions") or [])

  def orders(self) -> List[Dict[str, Any]]:
    response = self.post("/orders", {})
    return list(_require_ok(response, "orders").get("orders") or [])

  def place_order(self, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = self.post("/place_order", payload)
    return _require_ok(response, "place_order")


class BarAggregator:
  def __init__(self, period: str, emit_open_bar: bool = False) -> None:
    self.period_seconds = _period_seconds(period)
    self.emit_open_bar = emit_open_bar
    self.current_bucket: str = ""
    self.current_open = 0.0
    self.current_high = 0.0
    self.current_low = 0.0
    self.current_close = 0.0

  def accept(self, tick: Tick) -> List[Bar]:
    bucket = _bucket_key(tick.key, self.period_seconds)
    if not bucket:
      return []
    if not self.current_bucket:
      self._start_bucket(bucket, tick.price)
      return []
    if bucket == self.current_bucket:
      self._update_bucket(tick.price)
      return []
    bar = self._current_bar()
    self._start_bucket(bucket, tick.price)
    return [bar]

  def latest_open_bar(self) -> Optional[Bar]:
    if self.emit_open_bar and self.current_bucket:
      return self._current_bar()
    return None

  def _start_bucket(self, bucket: str, price: float) -> None:
    self.current_bucket = bucket
    self.current_open = price
    self.current_high = price
    self.current_low = price
    self.current_close = price

  def _update_bucket(self, price: float) -> None:
    self.current_high = max(self.current_high, price)
    self.current_low = min(self.current_low, price)
    self.current_close = price

  def _current_bar(self) -> Bar:
    return Bar(
      key=self.current_bucket,
      open=self.current_open,
      high=self.current_high,
      low=self.current_low,
      close=self.current_close,
    )


class SimpleWebSocket:
  def __init__(self, url: str, timeout: float) -> None:
    self.url = url
    self.timeout = timeout
    self.sock: Optional[socket.socket] = None

  def connect(self) -> None:
    parsed = urllib.parse.urlparse(self.url)
    if parsed.scheme not in ("ws", "wss"):
      raise ValueError("unsupported websocket scheme: %s" % parsed.scheme)
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)
    raw = socket.create_connection((parsed.hostname or "", port), timeout=self.timeout)
    raw.settimeout(self.timeout)
    self.sock = ssl.create_default_context().wrap_socket(raw, server_hostname=parsed.hostname) if parsed.scheme == "wss" else raw
    self._handshake(parsed)

  def close(self) -> None:
    if self.sock is None:
      return
    try:
      self.sock.close()
    finally:
      self.sock = None

  def recv_json(self) -> Dict[str, Any]:
    while True:
      opcode, payload = self._recv_frame()
      if opcode in (1, 2):
        return json.loads(payload.decode("utf-8"))
      if opcode == 8:
        raise RuntimeError("websocket closed by server")
      if opcode == 9:
        self._send_frame(10, payload)

  def _handshake(self, parsed: urllib.parse.ParseResult) -> None:
    assert self.sock is not None
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    path = parsed.path or "/"
    if parsed.query:
      path += "?" + parsed.query
    host = parsed.hostname or ""
    if parsed.port:
      host += ":%s" % parsed.port
    request = (
      "GET %s HTTP/1.1\r\n"
      "Host: %s\r\n"
      "Upgrade: websocket\r\n"
      "Connection: Upgrade\r\n"
      "Sec-WebSocket-Key: %s\r\n"
      "Sec-WebSocket-Version: 13\r\n\r\n"
    ) % (path, host, key)
    self.sock.sendall(request.encode("ascii"))
    header = self._read_http_header()
    if " 101 " not in header.splitlines()[0]:
      raise RuntimeError("websocket handshake failed: %s" % header.splitlines()[0])
    expected = base64.b64encode(
      hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
    ).decode("ascii")
    if expected not in header:
      raise RuntimeError("websocket accept key mismatch")

  def _read_http_header(self) -> str:
    assert self.sock is not None
    data = b""
    while b"\r\n\r\n" not in data:
      chunk = self.sock.recv(4096)
      if not chunk:
        break
      data += chunk
    return data.decode("iso-8859-1", errors="replace")

  def _recv_frame(self) -> Tuple[int, bytes]:
    assert self.sock is not None
    head = self._read_exact(2)
    opcode = head[0] & 0x0F
    length = head[1] & 0x7F
    if length == 126:
      length = struct.unpack("!H", self._read_exact(2))[0]
    elif length == 127:
      length = struct.unpack("!Q", self._read_exact(8))[0]
    if head[1] & 0x80:
      mask = self._read_exact(4)
      payload = bytes(item ^ mask[idx % 4] for idx, item in enumerate(self._read_exact(length)))
    else:
      payload = self._read_exact(length)
    return opcode, payload

  def _read_exact(self, size: int) -> bytes:
    assert self.sock is not None
    data = b""
    while len(data) < size:
      chunk = self.sock.recv(size - len(data))
      if not chunk:
        raise RuntimeError("websocket connection closed")
      data += chunk
    return data

  def _send_frame(self, opcode: int, payload: bytes) -> None:
    assert self.sock is not None
    mask = os.urandom(4)
    head = bytes([0x80 | opcode])
    length = len(payload)
    if length < 126:
      head += bytes([0x80 | length])
    elif length <= 0xFFFF:
      head += bytes([0x80 | 126]) + struct.pack("!H", length)
    else:
      head += bytes([0x80 | 127]) + struct.pack("!Q", length)
    masked = bytes(item ^ mask[idx % 4] for idx, item in enumerate(payload))
    self.sock.sendall(head + mask + masked)


class DataCenterHttpSource:
  def __init__(self, base_url: str, symbol: str, period: str, count: int, timeout: float, emit_open_bar: bool) -> None:
    self.base_url = base_url.rstrip("/")
    self.symbol = symbol
    self.count = count
    self.timeout = timeout
    self.aggregator = BarAggregator(period, emit_open_bar=emit_open_bar)

  def next_bar(self) -> Optional[Bar]:
    payload = _http_get_json(
      "%s/api/v1/quote/l2/trade?symbol=%s&count=%s"
      % (self.base_url, urllib.parse.quote(self.symbol), self.count),
      self.timeout,
    )
    bars: List[Bar] = []
    for tick in _ticks_from_datacenter_payload(payload, self.symbol):
      bars.extend(self.aggregator.accept(tick))
    if bars:
      return bars[-1]
    return self.aggregator.latest_open_bar()


class DataCenterWebSocketSource:
  def __init__(self, ws_url: str, http_source: DataCenterHttpSource, symbol: str, period: str, timeout: float, fallback: bool, emit_open_bar: bool) -> None:
    self.ws_url = _ws_url_with_symbol(ws_url, symbol)
    self.http_source = http_source
    self.symbol = symbol
    self.timeout = timeout
    self.fallback = fallback
    self.socket: Optional[SimpleWebSocket] = None
    self.aggregator = BarAggregator(period, emit_open_bar=emit_open_bar)
    self._bar_queue: List[Bar] = []

  def next_bar(self) -> Optional[Bar]:
    if self._bar_queue:
      return self._bar_queue.pop(0)
    try:
      self._ensure_connected()
      assert self.socket is not None
      while True:
        payload = self.socket.recv_json()
        bars: List[Bar] = []
        for tick in _ticks_from_datacenter_payload(payload, self.symbol):
          bars.extend(self.aggregator.accept(tick))
        if len(bars) > 1:
          self._bar_queue.extend(bars[1:])
        if bars:
          return bars[0]
        open_bar = self.aggregator.latest_open_bar()
        if open_bar is not None:
          return open_bar
    except Exception as exc:
      self._reset_socket()
      if not self.fallback:
        raise
      print("[WARN] websocket unavailable; fallback to HTTP: %s" % exc)
      return self.http_source.next_bar()

  def _ensure_connected(self) -> None:
    if self.socket is not None:
      return
    self.socket = SimpleWebSocket(self.ws_url, timeout=self.timeout)
    self.socket.connect()
    print("[WS] connected %s" % self.ws_url)

  def _reset_socket(self) -> None:
    if self.socket is not None:
      self.socket.close()
      self.socket = None


def _require_ok(response: Dict[str, Any], action: str) -> Dict[str, Any]:
  if not response.get("ok"):
    raise RuntimeError("%s failed: %s" % (action, json.dumps(response, ensure_ascii=False)))
  value = response.get("value")
  return value if isinstance(value, dict) else {}


def _http_get_json(url: str, timeout: float) -> Dict[str, Any]:
  request = urllib.request.Request(url, method="GET")
  with urllib.request.urlopen(request, timeout=timeout) as response:
    return json.loads(response.read().decode("utf-8"))


def _period_seconds(period: str) -> int:
  text = str(period or "1m").strip().lower()
  if text.endswith("s"):
    return max(1, int(float(text[:-1] or "1")))
  if text.endswith("m"):
    return max(1, int(float(text[:-1] or "1") * 60))
  if text in ("minute", "min"):
    return 60
  raise ValueError("unsupported period for tick aggregation: %s" % period)


def _bucket_key(key: str, period_seconds: int) -> str:
  digits = "".join(ch for ch in str(key or "") if ch.isdigit())
  if len(digits) < 14:
    return digits
  yyyy, mm, dd = int(digits[:4]), int(digits[4:6]), int(digits[6:8])
  hh, minute, second = int(digits[8:10]), int(digits[10:12]), int(digits[12:14])
  seconds = hh * 3600 + minute * 60 + second
  bucket_seconds = seconds - seconds % period_seconds
  return "%04d%02d%02d%02d%02d%02d" % (
    yyyy,
    mm,
    dd,
    bucket_seconds // 3600,
    (bucket_seconds % 3600) // 60,
    bucket_seconds % 60,
  )


def _to_datacenter_symbol(security: str) -> str:
  text = str(security or "")
  if text.endswith(".XSHG"):
    return text.replace(".XSHG", ".SH")
  if text.endswith(".XSHE"):
    return text.replace(".XSHE", ".SZ")
  return text


def _ws_url_with_symbol(base_url: str, symbol: str) -> str:
  parsed = urllib.parse.urlparse(base_url)
  query = urllib.parse.parse_qs(parsed.query)
  query["symbols"] = [symbol]
  return urllib.parse.urlunparse(
    parsed._replace(query=urllib.parse.urlencode(query, doseq=True))
  )


def _normalize_tick_key(value: Any) -> str:
  digits = "".join(ch for ch in str(value or "") if ch.isdigit())
  if len(digits) >= 17 and digits.startswith(("19", "20", "21")):
    return digits[:14]
  if len(digits) >= 14 and digits.startswith(("19", "20", "21")):
    return digits[:14]
  if len(digits) == 12 and digits.startswith(("19", "20", "21")):
    return digits + "00"
  if len(digits) >= 10 and not digits.startswith(("19", "20", "21")):
    timestamp = int(digits[:13] if len(digits) >= 13 else digits[:10])
    if len(digits) >= 13:
      timestamp = timestamp // 1000
    return time.strftime("%Y%m%d%H%M%S", time.localtime(timestamp))
  return time.strftime("%Y%m%d%H%M%S")


def _first_present(data: Dict[str, Any], names: List[str]) -> Any:
  for name in names:
    if name in data and data[name] not in (None, ""):
      return data[name]
  return None


def _tick_from_dict(data: Dict[str, Any]) -> Optional[Tick]:
  price = _first_present(data, ["price", "last_price", "trade_price", "成交价", "close"])
  if price in (None, ""):
    return None
  key = _first_present(
    data,
    ["time", "datetime", "timestamp", "trade_time", "timetag", "dt", "created_at", "成交时间"],
  )
  return Tick(key=_normalize_tick_key(key), price=float(price))


def _extract_trade_dicts(value: Any, symbol: str) -> List[Dict[str, Any]]:
  if isinstance(value, list):
    rows: List[Dict[str, Any]] = []
    for item in value:
      rows.extend(_extract_trade_dicts(item, symbol))
    return rows
  if not isinstance(value, dict):
    return []
  if symbol in value:
    return _extract_trade_dicts(value[symbol], symbol)
  rows = []
  for key in ("l2_trade", "trades", "transactions", "records", "items", "data"):
    if key in value:
      rows.extend(_extract_trade_dicts(value[key], symbol))
  if rows:
    return rows
  if _tick_from_dict(value) is not None:
    return [value]
  return []


def _ticks_from_datacenter_payload(payload: Dict[str, Any], symbol: str) -> List[Tick]:
  ticks = []
  for item in _extract_trade_dicts(payload, symbol):
    tick = _tick_from_dict(item)
    if tick is not None:
      ticks.append(tick)
  return sorted(ticks, key=lambda tick: tick.key)


def _load_env(path: Path) -> Dict[str, str]:
  result: Dict[str, str] = {}
  if not path.exists():
    return result
  for raw_line in path.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#") or "=" not in line:
      continue
    key, value = line.split("=", 1)
    result[key.strip()] = value.strip().strip('"').strip("'")
  return result


def _load_state(path: Path) -> StrategyState:
  if not path.exists():
    return StrategyState()
  data = json.loads(path.read_text(encoding="utf-8"))
  return StrategyState(**{k: data.get(k) for k in StrategyState.__dataclass_fields__})


def _save_state(path: Path, state: StrategyState) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(json.dumps(asdict(state), ensure_ascii=False, indent=2), encoding="utf-8")


def _trade_date_from_bar_key(key: str) -> str:
  digits = "".join(ch for ch in str(key) if ch.isdigit())
  return digits[:8] if len(digits) >= 8 else ""


def _position_for(positions: List[Dict[str, Any]], security: str) -> Dict[str, Any]:
  code = security.split(".", 1)[0]
  for item in positions:
    raw = item.get("raw") or {}
    if item.get("security") == security or raw.get("m_strInstrumentID") == code:
      return item
  return {}


def _has_pending_order(orders: List[Dict[str, Any]], security: str) -> bool:
  final_statuses = {53, 54, 55, 56, 57}
  for order in orders:
    if order.get("security") != security:
      continue
    status = int(order.get("raw_status") or -1)
    if status not in final_statuses:
      return True
  return False


def _next_order_remark(state: StrategyState, action: str) -> str:
  state.order_count += 1
  return "%s#%s_%s" % (action, state.order_count, STRATEGY_NAME)


def _send_signal(
  client: HelperClient,
  state: StrategyState,
  action: str,
  security: str,
  side: str,
  amount: int,
  price_type: int,
  dry_run: bool,
) -> None:
  remark = _next_order_remark(state, action)
  payload = {
    "security": security,
    "side": side,
    "amount": amount,
    "price": -1,
    "pr_type": price_type,
    "strategy": STRATEGY_NAME,
    "remark": remark,
  }
  if dry_run:
    print("[DRY] %s" % json.dumps(payload, ensure_ascii=False))
    return
  result = client.place_order(payload)
  print("[ORDER] %s" % json.dumps(result, ensure_ascii=False))


def _process_bar(
  client: HelperClient,
  state: StrategyState,
  security: str,
  bar: Bar,
  dry_run: bool,
) -> None:
  if state.last_processed_bar == bar.key:
    return
  trade_date = _trade_date_from_bar_key(bar.key)
  if trade_date and state.trade_date != trade_date:
    state.reset_for_day(bar.close, trade_date)
  if state.prev_close <= 0:
    state.reset_for_day(bar.close, trade_date)
    return
  positions = client.positions()
  orders = client.orders()
  position = _position_for(positions, security)
  closeable = int(position.get("closeable_amount") or 0)
  pending = _has_pending_order(orders, security)
  _process_signals(client, state, security, bar, closeable, pending, dry_run)
  if state.prev_close < bar.close:
    state.pending_peak = bar.close
  if state.prev_close > bar.close:
    state.pending_valley = bar.close
  state.prev_close = bar.close
  state.last_processed_bar = bar.key


def _process_signals(
  client: HelperClient,
  state: StrategyState,
  security: str,
  bar: Bar,
  closeable: int,
  pending: bool,
  dry_run: bool,
) -> None:
  if pending:
    print("[SKIP] pending order exists for %s" % security)
    return
  if state.pending_peak > 0 and bar.close < state.pending_peak:
    _handle_peak_signal(client, state, security, bar, closeable, dry_run)
    state.highest_peak = max(state.highest_peak, state.pending_peak)
    state.pending_peak = 0.0
  if state.pending_valley > 0 and bar.close > state.pending_valley:
    _handle_valley_signal(client, state, security, bar, dry_run)
    state.lowest_valley = min(state.lowest_valley, state.pending_valley)
    state.pending_valley = 0.0


def _handle_peak_signal(
  client: HelperClient,
  state: StrategyState,
  security: str,
  bar: Bar,
  closeable: int,
  dry_run: bool,
) -> None:
  peak = state.pending_peak
  if peak <= state.highest_peak:
    print("[FILTER] peak %.3f close %.3f did not break high %.3f" % (peak, bar.close, state.highest_peak))
    return
  amount = min(B_BASE_QTY, closeable)
  if amount < MIN_SELL_QTY:
    print("[SKIP] sell amount %s closeable %s" % (amount, closeable))
    return
  print("[SIGNAL] peak %.3f close %.3f -> SELL %s" % (peak, bar.close, amount))
  _send_signal(client, state, "open_short", security, "SELL", amount, SELL_PRICE_TYPE, dry_run)


def _handle_valley_signal(
  client: HelperClient,
  state: StrategyState,
  security: str,
  bar: Bar,
  dry_run: bool,
) -> None:
  valley = state.pending_valley
  if valley >= state.lowest_valley:
    print("[FILTER] valley %.3f close %.3f did not break low %.3f" % (valley, bar.close, state.lowest_valley))
    return
  print("[SIGNAL] valley %.3f close %.3f -> BUY %s" % (valley, bar.close, B_BASE_QTY))
  _send_signal(client, state, "open_long", security, "BUY", B_BASE_QTY, BUY_PRICE_TYPE, dry_run)


def _load_runtime_env(args: argparse.Namespace) -> Dict[str, str]:
  env = _load_env(Path(args.env_file))
  args._runtime_env = env
  return env


def _runtime_env(args: argparse.Namespace) -> Dict[str, str]:
  env = getattr(args, "_runtime_env", None)
  if isinstance(env, dict):
    return env
  return _load_runtime_env(args)


def _build_helper(args: argparse.Namespace) -> HelperClient:
  env = _runtime_env(args)
  password = args.password or env.get("BIG_QMT_GATEWAY_PASSWORD") or ""
  if not password:
    raise RuntimeError("missing helper password")
  helper_url = args.helper_url or env.get("BIG_QMT_GATEWAY_URL") or HELPER_URL
  return HelperClient(helper_url, password, timeout=args.timeout)


def _build_bar_source(args: argparse.Namespace) -> Any:
  symbol = _to_datacenter_symbol(args.security)
  http_source = DataCenterHttpSource(
    args.data_center_url,
    symbol,
    args.period,
    args.count,
    args.data_timeout,
    emit_open_bar=args.use_last_bar,
  )
  if args.data_source == "datacenter-http":
    return http_source
  if args.data_source == "datacenter-ws":
    return DataCenterWebSocketSource(
      args.data_center_ws_url,
      http_source,
      symbol,
      args.period,
      args.data_timeout,
      fallback=args.http_fallback,
      emit_open_bar=args.use_last_bar,
    )
  raise ValueError("unsupported data source: %s" % args.data_source)


def _handle_bar(client: HelperClient, state: StrategyState, args: argparse.Namespace, bar: Optional[Bar]) -> bool:
  if bar is None:
    print("[SKIP] no completed bar")
    return False
  if state.last_processed_bar == bar.key:
    return False
  print(
    "[BAR] %s O=%.3f H=%.3f L=%.3f C=%.3f dry_run=%s"
    % (bar.key, bar.open, bar.high, bar.low, bar.close, args.dry_run)
  )
  _process_bar(client, state, args.security, bar, args.dry_run)
  _save_state(Path(args.state_file), state)
  return True


def run_once(args: argparse.Namespace) -> None:
  client = _build_helper(args)
  source = _build_bar_source(args)
  state = _load_state(Path(args.state_file))
  _handle_bar(client, state, args, source.next_bar())


def run_loop(args: argparse.Namespace) -> None:
  print(
    "[LOOP] security=%s period=%s data_source=%s interval=%ss dry_run=%s"
    % (args.security, args.period, args.data_source, args.interval, args.dry_run)
  )
  client = _build_helper(args)
  source = _build_bar_source(args)
  state = _load_state(Path(args.state_file))
  while True:
    started = time.time()
    bar: Optional[Bar] = None
    processed = False
    try:
      bar = source.next_bar()
      processed = _handle_bar(client, state, args, bar)
    except KeyboardInterrupt:
      print("[STOP] interrupted")
      return
    except Exception as exc:
      print("[ERROR] %s" % exc)
    if args.data_source == "datacenter-ws" and bar is not None and processed:
      continue
    elapsed = time.time() - started
    time.sleep(max(1.0, float(args.interval) - elapsed))


def build_parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(description="Run external trend-grid signals through Big QMT helper.")
  parser.add_argument("--security", required=True, help="Security code, e.g. 688536.XSHG")
  parser.add_argument("--helper-url", default=None, help="Big QMT helper URL. Defaults to BIG_QMT_GATEWAY_URL or 127.0.0.1:9000.")
  parser.add_argument(
    "--data-source",
    choices=["datacenter-ws", "datacenter-http"],
    default="datacenter-ws",
    help="Market data source. Defaults to datacenter WebSocket; HTTP fallback stays enabled unless --no-http-fallback is set.",
  )
  parser.add_argument("--data-center-url", default=DATA_CENTER_URL)
  parser.add_argument("--data-center-ws-url", default=DATA_CENTER_WS_URL)
  parser.add_argument("--data-timeout", type=float, default=10.0)
  parser.add_argument("--no-http-fallback", dest="http_fallback", action="store_false")
  parser.add_argument("--env-file", default=ENV_FILE)
  parser.add_argument("--password", default="")
  parser.add_argument("--period", default=EXECUTION_PERIOD)
  parser.add_argument("--count", type=int, default=30)
  parser.add_argument("--timeout", type=float, default=60.0)
  parser.add_argument("--state-file", default=STATE_FILE)
  parser.add_argument("--use-last-bar", action="store_true")
  parser.add_argument("--live", action="store_true", help="Actually send orders. Default is dry-run.")
  parser.add_argument("--loop", action="store_true", help="Run continuously instead of a single polling pass.")
  parser.add_argument("--interval", type=float, default=60.0, help="Loop polling interval in seconds.")
  parser.set_defaults(http_fallback=True)
  return parser


def main() -> None:
  parser = build_parser()
  args = parser.parse_args()
  args.dry_run = not args.live
  _load_runtime_env(args)
  if args.loop:
    run_loop(args)
  else:
    run_once(args)


if __name__ == "__main__":
  main()
