# QMT Signal Bridge 使用手册

## 目标

打通外部信号到大 QMT 内部下单流程：

```text
数据中心 WebSocket -> 外部信号 runner -> Big QMT helper -> QMT passorder
```

外部 runner 不直接下单，只发送交易意图。实际下单由运行在大 QMT 内部的 helper 完成。

行情数据只依赖数据服务器。Big QMT helper 只负责交易账户、持仓、订单和下单接口，不负责行情 close 校准。

runner 会把服务器 tick 在本地聚合成 OHLC bar。当前趋势网格信号仍按 bar close 判断，open/high/low 会保留给后续策略扩展使用。

## 运行前检查

确认目录中有这些文件：

```text
qmt_loader.py
big_qmt_gateway_strategy_sample.py
trend_grid_signal_runner.py
.env.bigqmt.example
README.md
USAGE.md
```

首次使用时，复制 `.env.bigqmt.example` 为 `.env.bigqmt`，再填写本机配置。

确认 `.env.bigqmt` 中的配置：

```text
BIG_QMT_GATEWAY_URL=http://127.0.0.1:9000
BIG_QMT_GATEWAY_PORT=9000
BIG_QMT_GATEWAY_PASSWORD=123456
BIG_QMT_GATEWAY_SECRET=change_me_hmac_secret
QMT_ACCOUNT_ID=70051230
QMT_ACCOUNT_TYPE=stock
```

helper 会读取 `.env.bigqmt` 中的端口、密码和账户配置：

```text
BIG_QMT_GATEWAY_PORT=9000
BIG_QMT_GATEWAY_PASSWORD=123456
QMT_ACCOUNT_ID=70051230
QMT_ACCOUNT_TYPE=stock
```

helper 会优先读取系统环境变量，其次读取同目录或当前工作目录下的 `.env.bigqmt`。推荐使用 `qmt_loader.py` 加载项目目录里的 helper，这样 helper 的文件路径会指向项目目录，可以直接读取项目目录中的 `.env.bigqmt`。

## 第一步：启动 Big QMT helper

在大 QMT 中创建或打开专用策略，把 `qmt_loader.py` 的内容放进去运行。

loader 的作用是读取并执行项目目录里的 `big_qmt_gateway_strategy_sample.py`。后续修改 helper 时，只需要修改项目文件并重启 QMT 策略，不需要再整段复制 helper。

启动成功时，日志应出现：

```text
[QMT_LOADER] loaded helper from C:\Users\zhongying\qmt_signal_bridge\big_qmt_gateway_strategy_sample.py
listen success listen=127.0.0.1:9000
entering tornado ioloop; gateway should keep running
```

如果没有出现 `listen success`，外部 runner 无法连接 helper。

## 第二步：验证 helper

在 PowerShell 中执行：

```powershell
Invoke-WebRequest http://127.0.0.1:9000/health -UseBasicParsing
```

如果返回 JSON，说明 helper 已经可访问。

如果返回连接拒绝，说明 helper 没有监听成功。

## 第三步：启动外部 runner

在 `qmt_signal_bridge` 目录运行：

```bash
python trend_grid_signal_runner.py --security 688536.XSHG --loop
```

默认是 dry-run：

```text
dry_run=True
```

dry-run 会计算信号，但不会真实下单。

## 第四步：观察 dry-run

正常启动日志类似：

```text
[LOOP] security=688536.XSHG period=1m data_source=datacenter-ws interval=60.0s dry_run=True
[WS] connected ws://60.190.249.91:18000/ws/quote?symbols=688536.SH
```

新 bar 日志会显示本地聚合后的 OHLC：

```text
[BAR] 20260710133400 O=323.500 H=324.810 L=323.200 C=324.810 dry_run=True
```

如果当前 bar 已经处理过，不会重复打印 `[BAR]`。

状态文件会自动生成：

```text
logs/trend_grid_signal_state.json
```

它用于记录最后处理的 bar，避免重复触发。

## 第五步：真实下单

确认以下事项后再使用 `--live`：

- helper 已启动并通过 `/health` 验证。
- 账户 `ACCOUNT_ID` 和 `ACCOUNT_TYPE` 正确。
- 证券代码正确。
- 下单数量和价格类型符合预期。
- 已在 dry-run 下观察过信号行为。

真实下单命令：

```bash
python trend_grid_signal_runner.py --security 688536.XSHG --loop --live
```

运行后如果触发信号，runner 会调用 helper 的 `/place_order`。

helper 日志中应出现：

```text
passorder begin
passorder returned
```

## 常见问题

### 连接拒绝

现象：

```text
WinError 10061
```

原因：runner 连不上 helper。

处理：

- 确认 Big QMT helper 已运行。
- 确认日志出现 `listen success listen=127.0.0.1:9000`。
- 确认 `.env.bigqmt` 的端口和 helper 的 `LISTEN_PORT` 一致。

### 认证失败

现象：

```text
AUTH_FAILED
gateway password mismatch
```

原因：runner 发送的密码和 helper 不一致。

处理：

- 检查 `.env.bigqmt` 的 `BIG_QMT_GATEWAY_PASSWORD`。
- 检查 helper 的 `GATEWAY_PASSWORD`。
- 两者必须完全一致。

### WebSocket 已连接但没有 BAR

现象：

```text
[WS] connected ...
```

但没有新的 `[BAR]`。

原因：当前数据中心可能持续推送同一根已处理过的 bar。脚本会跳过重复 bar，这是正常行为。

处理：

- 等待新 bar。
- 或临时换一个状态文件测试：

```bash
python trend_grid_signal_runner.py --security 688536.XSHG --loop --state-file logs/test_state.json
```

### 没有真实下单

原因：

- 没有加 `--live`。
- 没有触发信号。
- 有未完成订单，策略跳过。
- 可卖数量不足。

处理：

- 确认命令中包含 `--live`。
- 查看 runner 输出中的 `[SIGNAL]`。
- 查看 helper 日志是否出现 `passorder begin`。

## 安全建议

- 初次运行只使用 dry-run。
- 实盘前先用小数量测试。
- 每次修改账户、端口、密码后，都先验证 `/health`。
- 不要同时运行多个 helper 监听同一个端口。
- 不要在不确认策略状态时使用 `--live`。

## 停止方式

外部 runner：

```text
Ctrl+C
```

Big QMT helper：

在大 QMT 中停止对应策略。

当前配置：

```python
STOP_HTTP_ON_QMT_STOP = False
```

如果 QMT 停止策略后端口仍未释放，需要关闭对应 QMT 策略运行环境或重启 QMT。
