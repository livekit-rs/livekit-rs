# Single-node worker probe (SPEC-0074 A10)

`probe.py` answers a question the Twirp differential cannot express: what a
single-node server (no Redis, no worker connected) does **over time** when a
public method routes into the worker services.

Upstream keeps a real message bus in this mode (`getMessageBus` returns
`psrpc.NewLocalMessageBus` when the Redis client is nil), so an Egress start is
published and waits out the psrpc retry schedule before the API answers
`503 unavailable` / `no response from servers`. Methods that go through the
store handles (`getEgressStore` and friends return nil without Redis) answer
`500 internal` / `... not connected (redis required)` immediately.

## Usage

```sh
python3 -I probe.py --binary /abs/livekit-server --port 18890 \
    --check [--timeout 90] [--only Egress.StartEgress,Ingress.ListIngress] [--out out.json]
```

The probe starts the binary with a fixed config (devkey / test secret, unique
HTTP + TCP + UDP ports, `node_ip: 127.0.0.1`), creates room `r1` first (a start
request resolves its room through the store before it publishes), then calls
each method once and records `status`, `body` and `elapsed`.
计时使用单调时钟；setup 必须成功，所有调用后再次 ListRooms 确认房间和服务仍可用。
正常结束或异常时对自有进程组发 SIGTERM，5 秒仍未退出才 SIGKILL，并等待回收。

对官方与候选使用不同端口、相同配置和完整方法集分别运行。
`--check` 校验实际 HTTP 状态、完整 JSON 和时序：Start 为 17–40 秒，
UpdateStream/Stop 为 2.5–6 秒，CreateRoom 自动录制等待为 9–15 秒且
`active_recording=false`。窗口允许原版随机 backoff；不要求毫秒相等。
客户端 timeout 默认 90 秒，避免 20 秒落在重试中途。

## Scope

- `Egress` start variants: no worker → 503 after the retry schedule.
- `Egress` list/update/stop, `Ingress` create/list/update/delete, `SIP`
  `CreateSIPParticipant`: nil store → the original's disconnected/panic
  responses.
- `RoomService.CreateRoom` with an `egress` block: the automatic launcher runs
  in the same mode, so the create waits like the original.

`Ingress.UpdateIngress`/`Ingress.DeleteIngress` 在原版进入 panic recovery。
实际 Twirp `writeError` 设置 Content-Length 并 Flush，公网正文为完整 JSON；
ResponseRecorder 中的 PANIC 追加文本不是线缆契约。探针不截断正文、不仅比较
status/prefix；实际 connection reset 必须记录为失败，不能归一化为成功。

## 复核原始证据

在 `livekit-rs-server/` 执行：

```sh
python3 -I -B tests/interop/egress-no-worker/verify.py \
    ../livekit-rs-docs/evidence/2026-10-30/a10-local-bus \
    --ledger ../livekit-rs-docs/specs/0011-functional-parity-todo.json
```

复核两端完整 16 方法、15 条错误正文逐字相等、房间响应的既有随机字段规范化，
重新从 raw 响应计算全部 538 条差分判据。60 条 Start 变体不得有 transport error，
其中每侧 18 条须是真实 503；其余差异完整列出而非抹平。7 个负例确保空集、超时、
立即失败、正文尾缀、错误正文和伪录制不能通过。另统计 workspace 日志和全量台账，
确认关联 API 父项仍为 PARTIAL。协议 handler 往返测试不代表真实媒体 worker 成功。

## Result (2026-10-30)

`livekit-rs-docs/evidence/2026-10-30/a10-local-bus/` holds both probe outputs
and the Twirp differential report produced from the same binaries.
