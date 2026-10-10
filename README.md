# websocket-client-sdk

零依赖的浏览器 WebSocket SDK，把「一条长连接」该有的事情一次补齐。

在原生 `WebSocket` 之上封装 **连接管理 / 自动重连 / 心跳保活 / 命名事件订阅 / 请求应答（ack）/ 断线队列**，向上暴露一套 socket.io 风格 API，向下保持对「裸 WebSocket」服务端的完全兼容。

## 为什么需要它

原生 `WebSocket` 只提供了最小的收发原语，真实业务里缺的都是这些：

1. **没有连接状态概念**——`readyState` 只有四个值，无法表达"正在重连""已放弃"这类业务真正关心的状态；
2. **断了不会自己回来**——网络抖动、服务端重启、休眠唤醒都会断，必须自己写重连；
3. **断线感知滞后**——TCP 半开连接下 `onclose` 可能长时间不触发，只能靠应用层心跳判活；
4. **只有 `onmessage` 一个事件口**——所有消息挤在一起，需要自己按业务字段分发和订阅；
5. **没有请求-应答语义**——想"发一个请求并等它对应的结果"就得手写 id 映射与超时；
6. **断线期间的消息会直接丢**——`send` 在非 OPEN 状态下静默失败。

本 SDK 的对策是：**用一个显式状态机驱动的客户端实例承载这些职责**——`reconnecting` 时按指数退避 + 抖动重连，心跳超时即判定链路假死并主动断开重连，消息按命名事件分发，`emit` 可带 ack 回调或 `emitWithAck` 拿 Promise，未连接时的发送先入队、连上后按序补发。

## 安装

```bash
npm i websocket-client-sdk
```

运行时零依赖，浏览器直接可用（需要 `WebSocket`、`Promise`、`setTimeout` 等标准能力）。仅构建期需要 `tsup` / `typescript`。

## 快速开始

```ts
import { createSocket } from 'websocket-client-sdk'

const socket = createSocket({ url: 'wss://example.com/ws' })

socket.on('connect', () => console.log('已连接'))
socket.on('disconnect', ({ code, reason }) => console.log('断开', code, reason))
socket.on('chat', (msg) => console.log('收到 chat', msg))
socket.on('statechange', ({ state }) => console.log('状态 →', state))

socket.emit('chat', { text: 'hello' })
```

请求-应答（服务端回传后 `resolve`）：

```ts
const reply = await socket.emitWithAck('time')
console.log(reply)
```

也可以回应对端发来的、要求应答的事件：

```ts
socket.on('server:ask', (payload, ack) => {
  console.log('服务端提问', payload)
  ack({ answer: 'ok' }) // 只会真正回传一次，重复调用会被忽略
})
```

不想自动连接时：

```ts
const socket = createSocket({ url: 'wss://example.com/ws', autoConnect: false })
socket.connect()
```

携带鉴权参数（会拼进连接地址，握手时一并送达）：

```ts
const socket = createSocket({
  url: 'wss://example.com/ws',
  auth: { token: 'xxx', room: 'lobby' },
})
// 实际连接 wss://example.com/ws?token=xxx&room=lobby
```

传函数则每次连接/重连前重新求值，可返回 Promise，天然支持短期 token 轮换：

```ts
const socket = createSocket({
  url: 'wss://example.com/ws',
  auth: async () => ({ token: await fetchFreshToken() }),
})
```

## 配置项（SocketOptions）

| 字段 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `url` | `string` | — | **必填**。`ws://` 或 `wss://` 地址 |
| `auth` | `SocketAuth` | — | 鉴权数据，以查询参数形式附加到连接地址上；传函数时**每次连接/重连前重新求值**（可异步） |
| `protocols` | `string \| string[]` | — | 传给原生构造函数的子协议 |
| `autoConnect` | `boolean` | `true` | 创建后立即连接 |
| `connectTimeout` | `number` | `20000` | 建立连接（握手）的超时（毫秒），超时则中止本次连接并触发重连；设 `0` 关闭 |
| `reconnection` | `boolean` | `true` | 是否自动重连 |
| `reconnectionAttempts` | `number` | `Infinity` | 最大重连次数，超出后触发 `reconnect_failed` |
| `reconnectionDelay` | `number` | `1000` | 首次重连延迟（毫秒），此后按 2 的幂递增 |
| `reconnectionDelayMax` | `number` | `5000` | 单次重连延迟上限（毫秒） |
| `randomizationFactor` | `number` | `0.5` | 抖动系数（0~1），打散大量客户端的重连时刻 |
| `heartbeatInterval` | `number` | `25000` | 心跳间隔（毫秒），设 `0` 关闭心跳 |
| `heartbeatTimeout` | `number` | `10000` | 发出 ping 后等待 pong 的超时（毫秒），超时判定假死并断开 |
| `ackTimeout` | `number` | `10000` | 等待 ack 应答的超时（毫秒），**计时从消息真正写出后才开始** |
| `queueWhileDisconnected` | `boolean` | `true` | 未连接时是否缓存待发消息 |
| `maxQueueSize` | `number` | `100` | 断线队列上限，超出时丢弃最旧的消息（若该消息在等 ack，会立刻回调超时） |
| `binaryType` | `BinaryType` | `'arraybuffer'` | 接收二进制数据的方式 |

重连延迟按 `min(reconnectionDelayMax, reconnectionDelay × 2^(attempt-1))` 计算，再叠加 `±randomizationFactor` 的随机抖动。

## 事件（SocketEventMap）

```ts
socket.on('error', (err) => console.error(err.code, err.message))
socket.on('pong', ({ latency }) => console.log('心跳延迟', latency))
```

| 事件 | payload | 触发时机 |
| --- | --- | --- |
| `connect` | `undefined` | 连接建立（含首次与重连成功） |
| `disconnect` | `{ code, reason, wasClean }` | 连接断开，主动与异常都会触发 |
| `connect_error` | `SocketError` | 首次连接失败 |
| `error` | `SocketError` | 运行期错误（发送失败、心跳超时等） |
| `reconnect_attempt` | `{ attempt: number }` | 即将发起第 `attempt` 次重连 |
| `reconnect` | `{ attempt: number }` | 重连成功，`attempt` 为累计尝试次数 |
| `reconnect_error` | `SocketError` | 某一次重连尝试失败 |
| `reconnect_failed` | `undefined` | 重试次数耗尽，已放弃重连 |
| `ping` | `undefined` | 收到对端心跳 |
| `pong` | `{ latency: number }` | 心跳往返完成，`latency` 为毫秒 |
| `message` | `unknown` | 收到**非本协议帧**的原始消息（文本或二进制） |
| `statechange` | `{ state: SocketState }` | 状态机变化 |

除了上述内置事件，任意自定义事件名都可用 `on` / `once` / `off` 订阅，由服务端下推触发。

`SocketState` 取值：`idle` / `connecting` / `open` / `reconnecting` / `closing` / `closed`。

错误码常量（`SocketErrorCode`）：

| 码 | 含义 |
| --- | --- |
| `CONNECT_ERROR` | 连接建立失败 |
| `CONNECT_TIMEOUT` | 握手超时（超过 `connectTimeout` 仍未 `open`） |
| `SEND_ERROR` | 消息发送失败 |
| `HEARTBEAT_TIMEOUT` | 心跳超时，链路疑似假死 |
| `ACK_TIMEOUT` | 等待应答超时 |
| `CLOSED` | 实例已销毁，操作被拒绝 |

## API

### 属性

| 成员 | 类型 | 说明 |
| --- | --- | --- |
| `currentState` | `SocketState` | 当前连接状态 |
| `connected` | `boolean` | 是否处于 `open` |
| `url` | `string` | 配置里传入的原始地址（不含 `auth` 拼出的查询参数） |
| `raw` | `WebSocket \| null` | 底层原生实例，高级用法可读写 |
| `queueSize` | `number` | 断线期间积压的待发消息数 |

### 方法

| 方法 | 说明 |
| --- | --- |
| `connect(): this` | 建立连接；幂等，已连接或连接中直接返回 |
| `disconnect(code?, reason?): this` | 主动断开，**不再自动重连**（默认 `1000 / 'client disconnect'`） |
| `destroy(): void` | 销毁实例：断开、清空监听、拒绝所有待处理 ack |
| `on(event, fn): () => void` | 订阅事件，返回取消订阅函数 |
| `once(event, fn): () => void` | 订阅一次 |
| `off(event, fn): void` | 取消订阅 |
| `emit(event, data?, ack?): this` | 发送命名事件；传 `ack` 时等待对端应答 |
| `emitWithAck(event, data?): Promise<unknown>` | 发送事件并等待应答，超时 reject |
| `send(data?): this` | 等价于 `emit('message', data)` |

```ts
import { createSocket, WebSocketClient } from 'websocket-client-sdk'
import type { SocketOptions, SocketEventMap, SocketState, SocketError, AckCallback, SocketAuth } from 'websocket-client-sdk'
```

## 协议

SDK 在 WebSocket 之上定义了一个极简 JSON 帧，用于承载事件、应答与心跳：

| 帧 | 结构 | 含义 |
| --- | --- | --- |
| 事件 | `{ t: 0, e: '事件名', d: 数据, a?: ackId }` | 发送一个命名事件，`a` 存在时要求对端应答 |
| 应答 | `{ t: 1, a: ackId, d: 应答数据 }` | 对某个事件的应答 |
| ping | `{ t: 2 }` | 心跳探测 |
| pong | `{ t: 3 }` | 心跳回应 |

**裸 WebSocket 兼容**：当收到的数据不是 JSON、或缺少数字型 `t` 字段时，会被当作非本协议帧，原样通过 `message` 事件透传（二进制数据同样直接透传）。因此本 SDK 可以直接接到任何普通 WebSocket 服务端上，只是拿不到事件分发、ack 与心跳这些增强能力。

## 演示（demo）

仓库内自带一个零依赖的演示：`demo/server.mjs` 同时托管静态页面与 `/ws` 协议端点。

```bash
npm run demo:build    # 用 esbuild 打包 demo/main.ts → demo/.build/main.js
npm run demo:server   # 启动演示服务（默认 http://localhost:8787）
```

浏览器打开 `http://localhost:8787` 即可在页面上验证：

1. **连接管理**——connect / disconnect / destroy / 销毁重建，观察状态机流转；
2. **自动重连**——停掉服务端，观察 `reconnect_attempt` → `reconnect` 按指数退避触发，延迟上限生效；
3. **心跳保活**——`pong` 持续更新延迟计数，停服后触发 `HEARTBEAT_TIMEOUT` 并自动重连；
4. **事件订阅**——服务端下推的 `server:hello` / `server:tick` / `server:event` 等事件逐条 on / off；
5. **双向 ack**——`emitWithAck` 等待服务端应答，同时回应服务端 `server:ask` 的 ack；
6. **断线队列**——断开状态下发送多条消息，观察队列累积，连接恢复后按序补发；
7. **动态鉴权**——`auth` 会拼进连接地址的查询串，服务端校验 `token`，缺失或非法直接返回 401 拒绝握手；勾选「动态求值」后每次连接/重连都会重新取 token。

> demo 服务端对 `/ws` 的握手做鉴权：默认 token `valid-token` 在静态白名单内，`demo-token-` 前缀视为合法的动态 token，其余一律 401。因此把 token 改成任意非法值即可看到 `connect_error` 与拒绝日志。

## 开发

```bash
npm run typecheck   # 类型检查
npm run build       # 构建 ESM + CJS + d.ts
npm run dev         # watch 构建
npm run demo:build  # 打包 demo
npm run demo:watch  # watch 打包 demo
npm run demo:server # 启动演示服务端
```

## 已知边界

- 面向浏览器环境，依赖全局 `WebSocket`；Node 端需自行提供兼容实现。
- 心跳是**应用层** ping/pong，需要服务端配合回应 `{ t: 3 }`，否则会持续判定超时。
- ack 基于内存中的自增 id 映射，进程内唯一；不使用 ack 时该机制不产生任何开销。
- ack 超时**从消息真正发出后开始计时**：断线排队阶段不计时，因此排再久也不会「还没发就超时」。若消息因队列溢出被丢弃或发送抛错，等待中的 ack 会立即以失败终结，不会挂起。
- 断线队列只保证**顺序**，超出 `maxQueueSize` 会丢弃最旧的消息，不做持久化。
- `connectTimeout` 覆盖的是原生握手阶段：超时后主动关闭该 socket 并走正常重连流程，抛出的错误码为 `CONNECT_TIMEOUT`（首次连接走 `connect_error`，重连期间走 `reconnect_error`）。若浏览器原生实现本身已有超时，两者互不冲突。
- `auth` 只是把字段拼成 URL 查询参数交给服务端，**具体鉴权逻辑由服务端自行解析**；值为 `undefined` / `null` 的字段会被忽略，其余值按 `String()` 写入（对象会得到 `[object Object]`，请先自行序列化）。求值失败时抛出 `CONNECT_ERROR`（`message` 为「鉴权参数获取失败」）并进入正常重连流程。

## License

MIT
