# QMT Signal Bridge

这个目录是外部信号到大 QMT 内部下单的最小运行包。

## 架构边界

```text
外部行情/信号 runner -> 本地 HTTP 请求 -> Big QMT helper -> QMT passorder/cancel_order
```

- 外部 runner 只负责获取行情、计算信号、发送交易意图。
- Big QMT helper 必须运行在大 QMT 策略环境里，负责实际下单和撤单。
- 外部 Python 不直接调用 QMT 交易 API。

## 文件说明

- `big_qmt_gateway_strategy_sample.py`
  - 复制到大 QMT 策略里运行。
  - 启动本地 HTTP 网关。
  - 提供账户、持仓、订单、成交查询接口。
  - 接收下单/撤单请求，并在 QMT 环境里调用 `passorder` / `cancel_order`。

- `trend_grid_signal_runner.py`
  - 在外部 Python 环境运行。
  - 默认通过数据中心 WebSocket 获取行情。
  - 生成趋势网格信号。
  - 通过 helper 的 HTTP 接口发送下单请求。

- `.env.bigqmt`
  - 外部 runner 的本地连接配置。
  - 从 `.env.bigqmt.example` 复制后按本机 QMT 配置填写。
  - `BIG_QMT_GATEWAY_URL` 必须和 helper 的监听地址端口一致。
  - `BIG_QMT_GATEWAY_PASSWORD` 必须和 helper 的 `GATEWAY_PASSWORD` 一致。

## 启动顺序

1. 在大 QMT 中运行 `big_qmt_gateway_strategy_sample.py`。
2. 确认大 QMT 日志出现：

```text
listen success listen=127.0.0.1:9000
entering tornado ioloop; gateway should keep running
```

3. 在外部终端运行：

```bash
python trend_grid_signal_runner.py --security 688536.XSHG --loop
```

默认是 dry-run，不会真实下单。需要真实下单时增加：

```bash
--live
```

## 关键配置

helper 当前关键配置：

```python
LISTEN_PORT = 9000
GATEWAY_PASSWORD = "123456"
ACCOUNT_ID = "18886101811"
ACCOUNT_TYPE = "credit"
RUN_HTTP_IN_BACKGROUND_THREAD = False
STOP_HTTP_ON_QMT_STOP = False
```

runner 默认配置：

```text
data_source = datacenter-ws
data center ws = ws://192.168.100.4:18000/ws/quote
HTTP fallback = enabled
```

## 验证命令

检查 helper 健康状态：

```powershell
Invoke-WebRequest http://127.0.0.1:9000/health -UseBasicParsing
```

只测试外部信号循环：

```bash
python trend_grid_signal_runner.py --security 688536.XSHG --loop
```

## 注意事项

- `--live` 会发送真实下单请求，使用前先确认账户、证券、价格类型和数量。
- 如果出现 `AUTH_FAILED`，检查 `.env.bigqmt` 和 helper 中的密码是否一致。
- 如果出现 `WinError 10061`，说明 runner 无法连接 helper，先确认大 QMT helper 是否已经监听成功。
- 同一根 bar 只会处理一次，重复推送不会重复打印或重复触发信号。
