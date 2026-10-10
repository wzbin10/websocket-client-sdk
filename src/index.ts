/**
 * websocket-client-sdk
 *
 * 零依赖的浏览器 WebSocket SDK，在原生 WebSocket 之上补齐：
 *   - 连接管理：connect / disconnect / destroy，显式状态机
 *   - 自动重连：指数退避 + 随机抖动，可配置重试次数与延迟上限
 *   - 心跳保活：应用层 ping/pong，超时判定假死并强制重连
 *   - 消息订阅：socket.io 风格的命名事件 on / once / off / emit
 *   - 请求应答：emit 带 ack 回调，或 emitWithAck 返回 Promise
 *   - 断线队列：未连上时消息先入队，连接恢复后按序补发
 *
 * @example
 * ```ts
 * import { createSocket } from 'websocket-client-sdk'
 *
 * const socket = createSocket({ url: 'wss://example.com/ws' })
 * socket.on('connect', () => console.log('connected'))
 * socket.on('chat', (msg) => console.log('收到消息', msg))
 * socket.emit('chat', { text: 'hello' })
 * ```
 */

/* ------------------------------------------------------------------ *
 * 协议：本 SDK 在 WebSocket 之上定义一个极简 JSON 帧
 *   { t: 0, e: '事件名', d: 数据, a?: ackId }  发送一个命名事件
 *   { t: 1, a: ackId, d: 应答数据 }            对某个事件的应答
 *   { t: 2 }                                   心跳 ping
 *   { t: 3 }                                   心跳 pong
 * 非 JSON 或不符合该结构的帧会以原始数据透传到 'message' 事件，
 * 因此本 SDK 也能直接对接「裸 WebSocket」服务端。
 * ------------------------------------------------------------------ */

const PKT = {
  EVENT: 0,
  ACK: 1,
  PING: 2,
  PONG: 3,
} as const

interface Packet {
  t: number
  e?: string
  d?: unknown
  a?: number
}

/** 接收到的、要求应答的事件，回传应答数据的函数 */
export type AckCallback = (response?: unknown) => void

/** 错误码 */
export type SocketErrorCode =
  | 'CONNECT_ERROR'
  | 'CONNECT_TIMEOUT'
  | 'SEND_ERROR'
  | 'HEARTBEAT_TIMEOUT'
  | 'ACK_TIMEOUT'
  | 'CLOSED'

export interface SocketError {
  code: SocketErrorCode
  message: string
  cause?: unknown
}

/** 连接状态机 */
export type SocketState =
  /** 已创建，尚未连接 */
  | 'idle'
  /** 正在建立连接 */
  | 'connecting'
  /** 已连接 */
  | 'open'
  /** 断线，等待下一次重连 */
  | 'reconnecting'
  /** 正在关闭 */
  | 'closing'
  /** 已关闭，且不再重连 */
  | 'closed'

/** 内置生命周期事件及负载 */
export interface SocketEventMap {
  /** 连接建立 */
  connect: undefined
  /** 连接断开（用户主动或异常均会触发） */
  disconnect: { code: number; reason: string; wasClean: boolean }
  /** 首次连接失败 */
  connect_error: SocketError
  /** 运行期错误 */
  error: SocketError
  /** 即将发起第 attempt 次重连 */
  reconnect_attempt: { attempt: number }
  /** 重连成功 */
  reconnect: { attempt: number }
  /** 某次重连失败 */
  reconnect_error: SocketError
  /** 重试次数耗尽，放弃重连 */
  reconnect_failed: undefined
  /** 收到对端心跳 */
  ping: undefined
  /** 心跳往返完成，latency 为毫秒 */
  pong: { latency: number }
  /** 收到非本协议帧的原始消息 */
  message: unknown
  /** 状态变化 */
  statechange: { state: SocketState }
}

export type SocketEventName = keyof SocketEventMap

/** 鉴权数据：会被序列化进连接地址的查询串 */
export type SocketAuthData = Record<string, unknown>

/**
 * 动态鉴权：每次建立连接前求值，可返回 Promise（例如先刷新 token 再连），
 * 因此重连时会重新取值，天然支持 token 轮换。
 */
export type SocketAuthResolver = () => SocketAuthData | Promise<SocketAuthData>

export type SocketAuth = SocketAuthData | SocketAuthResolver

export interface SocketOptions {
  /** 服务端地址，ws:// 或 wss:// */
  url: string
  /**
   * 鉴权数据，会以查询参数形式附加到连接地址上，握手时一并送达。
   * 传函数时每次连接/重连前重新求值（可异步），适合携带短期 token。
   */
  auth?: SocketAuth
  /** 子协议 */
  protocols?: string | string[]
  /** 创建后立即连接，默认 true */
  autoConnect?: boolean
  /** 建立连接的超时（毫秒），从握手开始计时，超时则中止本次连接并触发重连；设为 0 关闭，默认 20000 */
  connectTimeout?: number
  /** 是否自动重连，默认 true */
  reconnection?: boolean
  /** 最大重连次数，默认 Infinity */
  reconnectionAttempts?: number
  /** 首次重连延迟（毫秒），默认 1000 */
  reconnectionDelay?: number
  /** 重连延迟上限（毫秒），默认 5000 */
  reconnectionDelayMax?: number
  /** 抖动系数（0~1），默认 0.5，用于打散同时重连 */
  randomizationFactor?: number
  /** 心跳间隔（毫秒），设为 0 关闭心跳，默认 25000 */
  heartbeatInterval?: number
  /** 等待 pong 的超时（毫秒），默认 10000 */
  heartbeatTimeout?: number
  /** 等待 ack 应答的超时（毫秒），默认 10000 */
  ackTimeout?: number
  /** 断线期间是否缓存待发消息，默认 true */
  queueWhileDisconnected?: boolean
  /** 断线队列上限，超出时丢弃最旧的消息，默认 100 */
  maxQueueSize?: number
  /** 接收二进制数据的方式，默认 'arraybuffer' */
  binaryType?: BinaryType
}

interface NormalizedSocketOptions extends Required<Omit<SocketOptions, 'protocols' | 'auth'>> {
  protocols?: string | string[]
  auth?: SocketAuth
}

const DEFAULTS = {
  autoConnect: true,
  connectTimeout: 20000,
  reconnection: true,
  reconnectionAttempts: Number.POSITIVE_INFINITY,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 5000,
  randomizationFactor: 0.5,
  heartbeatInterval: 25000,
  heartbeatTimeout: 10000,
  ackTimeout: 10000,
  queueWhileDisconnected: true,
  maxQueueSize: 100,
  binaryType: 'arraybuffer' as BinaryType,
}

function normalizeOptions(options: SocketOptions): NormalizedSocketOptions {
  if (!options || !options.url) {
    throw new Error('[websocket-client-sdk] options.url 是必填项')
  }
  return { ...DEFAULTS, ...options }
}

function makeError(code: SocketErrorCode, message: string, cause?: unknown): SocketError {
  return { code, message, cause }
}

/**
 * 把鉴权数据合并为连接地址上的查询参数，保留地址中原有的查询串。
 * 值为 undefined / null 的字段忽略；其余按字符串写入（对象会得到 [object Object]，请自行先序列化）。
 */
function appendAuthToUrl(url: string, data: SocketAuthData | undefined): string {
  if (!data) return url
  const entries = Object.entries(data).filter(([, v]) => v !== undefined && v !== null)
  if (entries.length === 0) return url

  let parsed: URL
  try {
    parsed = new URL(url)
  } catch {
    // 地址无法解析（如相对路径）时退化为手工拼接
    const sep = url.includes('?') ? '&' : '?'
    return url + sep + entries.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`).join('&')
  }
  for (const [k, v] of entries) parsed.searchParams.set(k, String(v))
  return parsed.toString()
}

/* ------------------------------------------------------------------ *
 * 内部事件总线
 * ------------------------------------------------------------------ */

/** 收窄为「任意参数」以便承载带 ack 参数的事件监听器 */
type AnyFn = (...args: any[]) => void

class Emitter {
  private listeners = new Map<string, Set<AnyFn>>()

  on(event: string, fn: AnyFn): () => void {
    let set = this.listeners.get(event)
    if (!set) {
      set = new Set()
      this.listeners.set(event, set)
    }
    set.add(fn)
    return () => this.off(event, fn)
  }

  off(event: string, fn: AnyFn): void {
    const set = this.listeners.get(event)
    if (!set) return
    set.delete(fn)
    if (set.size === 0) this.listeners.delete(event)
  }

  emit(event: string, ...args: unknown[]): void {
    const set = this.listeners.get(event)
    if (!set || set.size === 0) return
    // 复制一份，允许监听器内部执行 off
    for (const fn of [...set]) {
      try {
        fn(...args)
      } catch (err) {
        console.error(`[websocket-client-sdk] "${event}" 的监听器抛出异常:`, err)
      }
    }
  }

  /** 清空所有监听，destroy 时调用 */
  clear(): void {
    this.listeners.clear()
  }
}

interface PendingAck {
  /** 等待应答的事件名，仅用于超时文案 */
  event: string
  resolve: (value: unknown) => void
  reject: (err: SocketError) => void
  /** 超时计时器在消息真正发出后才启动，尚未发出时为 null */
  timer: ReturnType<typeof setTimeout> | null
}

/* ------------------------------------------------------------------ *
 * 客户端
 * ------------------------------------------------------------------ */

/**
 * 一个 WebSocketClient 实例 = 一条长连接。
 *
 * 生命周期：
 *   connect → connecting → open → (异常断开) → reconnecting → open …
 *                         ↓ disconnect()
 *                       closing → closed
 */
export class WebSocketClient {
  private readonly options: NormalizedSocketOptions
  private readonly emitter = new Emitter()

  private ws: WebSocket | null = null
  private state: SocketState = 'idle'
  /** 用户主动断开，禁止自动重连 */
  private manuallyClosed = false
  private destroyed = false

  private reconnectAttempts = 0
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null

  /** 握手超时计时器，仅在 connecting 阶段存在 */
  private connectTimer: ReturnType<typeof setTimeout> | null = null

  /** 连接序号：用于作废「鉴权参数求值期间被断开」的过期连接尝试 */
  private connectSeq = 0

  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private pongTimer: ReturnType<typeof setTimeout> | null = null
  private pingSentAt = 0

  private readonly queue: Packet[] = []
  private readonly pendingAcks = new Map<number, PendingAck>()
  private ackId = 0

  constructor(options: SocketOptions) {
    this.options = normalizeOptions(options)
    if (this.options.autoConnect) this.connect()
  }

  // ---------------------------------------------------------- 公开属性

  /** 当前连接状态 */
  get currentState(): SocketState {
    return this.state
  }

  /** 是否已连接 */
  get connected(): boolean {
    return this.state === 'open'
  }

  /** 连接地址 */
  get url(): string {
    return this.options.url
  }

  /** 底层原生实例，需要高级用法时可读写（谨慎） */
  get raw(): WebSocket | null {
    return this.ws
  }

  /** 断线期间积压的待发消息数 */
  get queueSize(): number {
    return this.queue.length
  }

  // ---------------------------------------------------------- 连接管理

  /** 建立连接（幂等；已在连接中或已连接时直接返回） */
  connect(): this {
    if (this.destroyed) {
      throw new Error('[websocket-client-sdk] 实例已销毁，无法重新连接')
    }
    if (this.state === 'open' || this.state === 'connecting') return this
    this.manuallyClosed = false
    this.reconnectAttempts = 0
    this.openSocket()
    return this
  }

  /** 主动断开连接，不再自动重连 */
  disconnect(code = 1000, reason = 'client disconnect'): this {
    if (this.destroyed) return this
    this.manuallyClosed = true
    this.clearReconnectTimer()
    this.clearConnectTimer()
    this.stopHeartbeat()

    const ws = this.ws
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      this.setState('closing')
      ws.close(code, reason)
    } else {
      this.setState('closed')
    }
    return this
  }

  /** 销毁实例：断开连接、清空监听与所有待处理请求 */
  destroy(): void {
    if (this.destroyed) return
    this.disconnect()
    this.destroyed = true
    this.clearReconnectTimer()
    this.clearConnectTimer()
    this.stopHeartbeat()

    const err = makeError('CLOSED', '实例已销毁')
    for (const pending of this.pendingAcks.values()) {
      if (pending.timer !== null) clearTimeout(pending.timer)
      pending.reject(err)
    }
    this.pendingAcks.clear()
    this.queue.length = 0
    this.emitter.clear()
  }

  // ---------------------------------------------------------- 消息订阅

  /** 订阅事件，返回取消订阅函数 */
  on<K extends SocketEventName>(
    event: K,
    fn: (payload: SocketEventMap[K]) => void,
  ): () => void
  on(event: string, fn: (payload: unknown, ack?: AckCallback) => void): () => void
  on(event: string, fn: AnyFn): () => void {
    return this.emitter.on(event, fn)
  }

  /** 订阅一次性事件 */
  once<K extends SocketEventName>(
    event: K,
    fn: (payload: SocketEventMap[K]) => void,
  ): () => void
  once(event: string, fn: (payload: unknown, ack?: AckCallback) => void): () => void
  once(event: string, fn: AnyFn): () => void {
    const off = this.on(event, ((...args: unknown[]) => {
      off()
      ;(fn as AnyFn)(...args)
    }) as AnyFn)
    return off
  }

  /** 取消订阅 */
  off(event: string, fn: AnyFn): void {
    this.emitter.off(event, fn)
  }

  /**
   * 发送一个命名事件。
   * @param ack 可选应答回调；服务端回传时会以应答数据调用
   */
  emit(event: string, data?: unknown, ack?: AckCallback): this {
    if (ack) {
      const id = ++this.ackId
      // 先登记待应答，计时放到消息真正发出之后（见 armAckTimer）
      this.pendingAcks.set(id, {
        event,
        resolve: (v) => ack(v),
        reject: () => ack(undefined),
        timer: null,
      })
      this.deliver({ t: PKT.EVENT, e: event, d: data, a: id })
    } else {
      this.deliver({ t: PKT.EVENT, e: event, d: data })
    }
    return this
  }

  /** 发送命名事件并等待应答（Promise 版 ack） */
  emitWithAck(event: string, data?: unknown): Promise<unknown> {
    return new Promise<unknown>((resolve, reject) => {
      if (this.destroyed) {
        reject(makeError('CLOSED', '实例已销毁'))
        return
      }
      const id = ++this.ackId
      this.pendingAcks.set(id, { event, resolve, reject, timer: null })
      this.deliver({ t: PKT.EVENT, e: event, d: data, a: id })
    })
  }

  /** socket.io 风格别名：等价于 emit('message', data) */
  send(data?: unknown): this {
    return this.emit('message', data)
  }

  // ---------------------------------------------------------- 内部：连接

  private openSocket(): void {
    if (this.destroyed || this.manuallyClosed) return
    this.clearReconnectTimer()
    this.setState('connecting')

    // 鉴权参数可能异步求值（如先刷新 token），用序号作废期间被断开/重建的连接尝试
    const seq = ++this.connectSeq
    void this.prepareAndOpen(seq)
  }

  private async prepareAndOpen(seq: number): Promise<void> {
    let url: string
    try {
      url = await this.resolveConnectUrl()
    } catch (err) {
      if (seq !== this.connectSeq || this.destroyed || this.manuallyClosed) return
      this.reportConnectError(makeError('CONNECT_ERROR', '鉴权参数获取失败', err))
      this.scheduleReconnect()
      return
    }
    if (seq !== this.connectSeq || this.destroyed || this.manuallyClosed) return
    this.openSocketWithUrl(url)
  }

  /** 求值鉴权数据并合并进连接地址（每次连接/重连都会重新求值） */
  private async resolveConnectUrl(): Promise<string> {
    const { auth } = this.options
    if (!auth) return this.options.url
    const data = typeof auth === 'function' ? await auth() : auth
    return appendAuthToUrl(this.options.url, data)
  }

  private openSocketWithUrl(url: string): void {
    if (this.destroyed || this.manuallyClosed) return

    let ws: WebSocket
    try {
      ws = this.options.protocols
        ? new WebSocket(url, this.options.protocols)
        : new WebSocket(url)
    } catch (err) {
      // 构造即失败（如 url 非法）
      this.reportConnectError(makeError('CONNECT_ERROR', 'WebSocket 创建失败', err))
      this.scheduleReconnect()
      return
    }

    ws.binaryType = this.options.binaryType
    ws.onopen = () => this.handleOpen()
    ws.onmessage = (ev) => this.handleMessage(ev)
    ws.onerror = () => this.handleSocketError()
    ws.onclose = (ev) => this.handleClose(ev)
    this.ws = ws
    this.armConnectTimer(ws)
  }

  /**
   * 握手超时保护：某些网络环境下 TCP 已连通但升级握手一直无响应，
   * 既不会触发 onopen 也不一定触发 onerror，连接会永久停在 connecting。
   * 超时后主动关闭该 socket，交给 onclose → scheduleReconnect 走正常重连流程。
   */
  private armConnectTimer(ws: WebSocket): void {
    this.clearConnectTimer()
    const { connectTimeout } = this.options
    if (connectTimeout <= 0) return
    this.connectTimer = setTimeout(() => {
      this.connectTimer = null
      if (this.ws !== ws || ws.readyState !== WebSocket.CONNECTING) return
      this.reportConnectError(
        makeError('CONNECT_TIMEOUT', `建立连接超时（${connectTimeout}ms）`),
      )
      ws.close()
    }, connectTimeout)
  }

  private clearConnectTimer(): void {
    if (this.connectTimer !== null) {
      clearTimeout(this.connectTimer)
      this.connectTimer = null
    }
  }

  private handleOpen(): void {
    this.clearConnectTimer()
    const attempts = this.reconnectAttempts
    this.reconnectAttempts = 0
    this.setState('open')
    this.startHeartbeat()
    this.flushQueue()
    this.emitter.emit('connect', undefined)
    if (attempts > 0) this.emitter.emit('reconnect', { attempt: attempts })
  }

  private handleSocketError(): void {
    // 浏览器出于安全考虑不暴露错误细节
    this.reportConnectError(makeError('CONNECT_ERROR', 'WebSocket 连接发生错误'))
  }

  private reportConnectError(err: SocketError): void {
    // 用重连计数而非 state 判断：openSocket 会把 state 置为 connecting，
    // 若以 state 判断，重连期间的错误会被误报成首次连接的 connect_error。
    if (this.reconnectAttempts > 0) this.emitter.emit('reconnect_error', err)
    else this.emitter.emit('connect_error', err)
  }

  private handleClose(ev: CloseEvent): void {
    this.clearConnectTimer()
    this.stopHeartbeat()
    this.ws = null
    this.emitter.emit('disconnect', {
      code: ev.code,
      reason: ev.reason,
      wasClean: ev.wasClean,
    })

    if (this.manuallyClosed || this.destroyed) {
      this.setState('closed')
      return
    }
    if (this.options.reconnection) this.scheduleReconnect()
    else this.setState('closed')
  }

  // ---------------------------------------------------------- 内部：重连

  private scheduleReconnect(): void {
    const attempt = this.reconnectAttempts + 1
    if (attempt > this.options.reconnectionAttempts) {
      this.emitter.emit('reconnect_failed', undefined)
      this.setState('closed')
      return
    }
    this.reconnectAttempts = attempt
    this.setState('reconnecting')
    this.emitter.emit('reconnect_attempt', { attempt })
    this.reconnectTimer = setTimeout(() => this.openSocket(), this.reconnectDelay(attempt))
  }

  /** 指数退避 + 随机抖动，避免断线后大量客户端同时重连 */
  private reconnectDelay(attempt: number): number {
    const { reconnectionDelay, reconnectionDelayMax, randomizationFactor } = this.options
    const base = Math.min(reconnectionDelayMax, reconnectionDelay * Math.pow(2, attempt - 1))
    const jitter = base * randomizationFactor * (Math.random() * 2 - 1)
    return Math.max(0, Math.round(base + jitter))
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  // ---------------------------------------------------------- 内部：心跳

  private startHeartbeat(): void {
    const { heartbeatInterval } = this.options
    if (heartbeatInterval <= 0) return
    this.heartbeatTimer = setInterval(() => this.sendPing(), heartbeatInterval)
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
    if (this.pongTimer !== null) {
      clearTimeout(this.pongTimer)
      this.pongTimer = null
    }
  }

  private sendPing(): void {
    if (!this.connected) return
    this.pingSentAt = Date.now()
    this.sendPacket({ t: PKT.PING })
    if (this.pongTimer !== null) clearTimeout(this.pongTimer)
    this.pongTimer = setTimeout(() => {
      // 超时未收到 pong，判定链路假死，主动断开以触发重连
      const err = makeError('HEARTBEAT_TIMEOUT', '心跳超时，连接可能已断开')
      this.emitter.emit('error', err)
      this.ws?.close(4000, 'heartbeat timeout')
    }, this.options.heartbeatTimeout)
  }

  private handlePong(): void {
    if (this.pongTimer !== null) {
      clearTimeout(this.pongTimer)
      this.pongTimer = null
    }
    this.emitter.emit('pong', { latency: Date.now() - this.pingSentAt })
  }

  // ---------------------------------------------------------- 内部：收发

  private handleMessage(ev: MessageEvent): void {
    const raw = ev.data
    if (typeof raw !== 'string') {
      // 二进制数据原样透传
      this.emitter.emit('message', raw)
      return
    }
    let packet: Packet | null = null
    try {
      packet = JSON.parse(raw) as Packet
    } catch {
      packet = null
    }
    if (!packet || typeof packet.t !== 'number') {
      // 非本 SDK 协议帧，按原始文本消息透传
      this.emitter.emit('message', raw)
      return
    }
    this.handlePacket(packet)
  }

  private handlePacket(packet: Packet): void {
    switch (packet.t) {
      case PKT.EVENT:
        this.dispatchEvent(packet)
        break
      case PKT.ACK:
        this.resolveAck(packet)
        break
      case PKT.PING:
        this.emitter.emit('ping', undefined)
        this.sendPacket({ t: PKT.PONG })
        break
      case PKT.PONG:
        this.handlePong()
        break
      default:
        break
    }
  }

  private dispatchEvent(packet: Packet): void {
    if (!packet.e) return
    if (packet.a == null) {
      this.emitter.emit(packet.e, packet.d)
      return
    }
    // 对端要求应答：向监听器注入 ack 回调，且只允许回传一次
    const ackId = packet.a
    let replied = false
    const ack: AckCallback = (response) => {
      if (replied) return
      replied = true
      this.sendPacket({ t: PKT.ACK, a: ackId, d: response })
    }
    this.emitter.emit(packet.e, packet.d, ack)
  }

  private resolveAck(packet: Packet): void {
    if (packet.a == null) return
    const pending = this.pendingAcks.get(packet.a)
    if (!pending) return
    if (pending.timer !== null) clearTimeout(pending.timer)
    this.pendingAcks.delete(packet.a)
    pending.resolve(packet.d)
  }

  /**
   * 投递一个包：已连接直接发出，未连接时按配置入队。
   * 注意 ack 的超时计时不在这里启动，而是等消息真正写出去之后（见 armAckTimer），
   * 否则断线排队时长一旦超过 ackTimeout，消息还没发就被判定为超时。
   */
  private deliver(packet: Packet): void {
    const ws = this.ws
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      if (!this.options.queueWhileDisconnected) {
        this.abortAck(packet.a, makeError('SEND_ERROR', `未连接，"${packet.e}" 发送失败`))
        return
      }
      this.queue.push(packet)
      if (this.queue.length > this.options.maxQueueSize) {
        const dropped = this.queue.shift()!
        this.abortAck(dropped.a, makeError('ACK_TIMEOUT', `断线队列已满，"${dropped.e}" 被丢弃`))
      }
      return
    }
    this.sendPacket(packet)
  }

  /** 连接恢复后按序补发 */
  private flushQueue(): void {
    while (this.queue.length > 0) {
      this.sendPacket(this.queue.shift()!)
    }
  }

  private sendPacket(packet: Packet): void {
    const ws = this.ws
    if (!ws || ws.readyState !== WebSocket.OPEN) return
    try {
      ws.send(JSON.stringify(packet))
      this.armAckTimer(packet.a)
    } catch (err) {
      const error = makeError('SEND_ERROR', '消息发送失败', err)
      this.abortAck(packet.a, error)
      this.emitter.emit('error', error)
    }
  }

  /** 消息真正发出后才开始计 ack 超时 */
  private armAckTimer(ackId?: number): void {
    if (ackId == null) return
    const pending = this.pendingAcks.get(ackId)
    if (!pending || pending.timer !== null) return
    pending.timer = setTimeout(() => {
      this.pendingAcks.delete(ackId)
      pending.reject(makeError('ACK_TIMEOUT', `等待 "${pending.event}" 的应答超时`))
    }, this.options.ackTimeout)
  }

  /** 消息未能送达（被丢弃或发送失败）时立即终结其待应答，避免回调/Promise 永久挂起 */
  private abortAck(ackId: number | undefined, err: SocketError): void {
    if (ackId == null) return
    const pending = this.pendingAcks.get(ackId)
    if (!pending) return
    if (pending.timer !== null) clearTimeout(pending.timer)
    this.pendingAcks.delete(ackId)
    pending.reject(err)
  }

  private setState(state: SocketState): void {
    if (this.state === state) return
    this.state = state
    this.emitter.emit('statechange', { state })
  }
}

/** 创建 WebSocket 客户端 */
export function createSocket(options: SocketOptions): WebSocketClient {
  return new WebSocketClient(options)
}

export default createSocket
