/**
 * websocket-client-sdk demo
 *
 * 目标：把 SDK 的每一项能力都做成可点击、可观察的验证入口 ——
 *   ① 连接管理：connect / disconnect / destroy / 重建的幂等与状态机流转；
 *   ② 自动重连：断开后 reconnect_attempt 按指数退避触发、成功后 emit reconnect；
 *   ③ 心跳保活：pong 延迟实时刷新，把服务端停掉可见 HEARTBEAT_TIMEOUT → 自动重连；
 *   ④ 消息订阅：内置生命周期事件统计 + 服务端下推事件逐条 on / off；
 *   ⑤ 请求应答：emit+ack 回调、emitWithAck(Promise)、服务端 server:ask 的双向 ack；
 *   ⑥ 断线队列：未连接时 emit 的消息入队，连接恢复后按序补发；
 *   ⑦ 动态鉴权：auth 拼进查询串，静态对象 / 函数式（每次连接重新求值）两种形态，服务端校验并拒绝非法 token。
 *   ⑧ 实时列表推送：服务端每秒广播地铁车次全量快照、到站时补发增量事件，表格随推送实时刷新。
 *
 * 本文件由 esbuild 打包为 demo/.build/main.js，由 demo/index.html 以 module 方式加载。
 */

import {
  createSocket,
  type AckCallback,
  type SocketAuth,
  type SocketEventName,
  type SocketState,
  type WebSocketClient,
} from '../src/index'

// ---------------------------------------------------------------- DOM 辅助

const $ = <T extends HTMLElement>(sel: string): T => {
  const el = document.querySelector<T>(sel)
  if (!el) throw new Error(`demo 页面缺少元素：${sel}`)
  return el
}

const panelLog = $('#panel-log')

// 顶栏统计
const statState = $('#stat-state')
const statAttempts = $('#stat-attempts')
const statLatency = $('#stat-latency')
const statQueue = $('#stat-queue')
const statPing = $('#stat-ping')
const statPong = $('#stat-pong')

// 连接管理卡片
const optUrl = $<HTMLInputElement>('#opt-url')
const optAuth = $<HTMLInputElement>('#opt-auth')
const optAuthUser = $<HTMLInputElement>('#opt-auth-user')
const optAuthToken = $<HTMLInputElement>('#opt-auth-token')
const optAuthDynamic = $<HTMLInputElement>('#opt-auth-dynamic')
const optReconnection = $<HTMLInputElement>('#opt-reconnection')
const optQueue = $<HTMLInputElement>('#opt-queue')
const optHeartbeat = $<HTMLInputElement>('#opt-heartbeat')
const optDelay = $<HTMLInputElement>('#opt-delay')
const optDelayMax = $<HTMLInputElement>('#opt-delaymax')
const optAttempts = $<HTMLInputElement>('#opt-attempts')
const chipState = $('#chip-state')
const chipUrl = $('#chip-url')
const chipAuth = $('#chip-auth')

// 发送卡片
const emitEv = $<HTMLInputElement>('#emit-ev')
const emitData = $<HTMLTextAreaElement>('#emit-data')
const emitQueue = $('#emit-queue')
const emitSent = $('#emit-sent')
const chipAck = $('#chip-ack')

// 订阅卡片
const subBody = $('#sub-rows')
const chipListeners = $('#chip-listeners')

// 内置事件卡片
const builtinBody = $('#builtin-rows')
const chipBuiltin = $('#chip-builtin')

// 地铁卡片
const metroBody = $('#metro-rows')
const chipMetro = $('#chip-metro')
const metroArrival = $('#metro-arrival')
const metroUpdated = $('#metro-updated')

// ---------------------------------------------------------------- 日志

function log(message: string, cls = ''): void {
  const line = document.createElement('div')
  line.className = cls ? `log-line ${cls}` : 'log-line'
  const t = new Date().toLocaleTimeString('zh-CN', { hour12: false })
  line.textContent = `${t} ${message}`
  line.title = line.textContent
  panelLog.appendChild(line)
  while (panelLog.childElementCount > 400) panelLog.removeChild(panelLog.firstElementChild as Element)
  panelLog.scrollTop = panelLog.scrollHeight
}

/** 负载序列化：列表里只展示一行摘要 */
function short(payload: unknown, max = 64): string {
  if (payload === undefined) return '–'
  let text: string
  try {
    text = typeof payload === 'string' ? payload : JSON.stringify(payload)
  } catch {
    text = String(payload)
  }
  if (text === undefined) text = String(payload)
  return text.length > max ? `${text.slice(0, max)}…` : text
}

function setChip(el: HTMLElement, text: string, tone: '' | 'ok' | 'warn' | 'err' | 'info' = ''): void {
  el.textContent = text
  el.className = tone ? `chip ${tone}` : 'chip'
}

const stateTone = (state: SocketState): '' | 'ok' | 'warn' | 'err' | 'info' => {
  if (state === 'open') return 'ok'
  if (state === 'reconnecting' || state === 'closing') return 'warn'
  if (state === 'closed') return 'err'
  return 'info'
}

// ---------------------------------------------------------------- 实例

let socket: WebSocketClient | null = null
let sentCount = 0
let pingCount = 0
let pongCount = 0

const num = (input: HTMLInputElement, fallback: number): number => {
  const value = Number(input.value)
  return Number.isFinite(value) ? value : fallback
}

/** 动态 auth 求值次数：仅用于 demo 里生成可区分的轮换 token */
let authEvalCount = 0

/** 按当前 UI 组装 auth：静态对象 或 每次连接重新求值的函数（演示 token 轮换） */
function readAuth(): SocketAuth | undefined {
  if (!optAuth.checked) return undefined
  const user = optAuthUser.value.trim()
  const token = optAuthToken.value.trim()
  if (!optAuthDynamic.checked) {
    return { user, token }
  }
  return () => {
    authEvalCount += 1
    // 每次连接/重连前都会走到这里重新取 token
    const rotating = { user, token: `demo-token-${Date.now()}-${authEvalCount}` }
    log(`auth 函数第 ${authEvalCount} 次求值 → ${short(rotating)}`, 'log-ctl')
    return rotating
  }
}

function readOptions(): Parameters<typeof createSocket>[0] {
  const attempts = Number(optAttempts.value)
  return {
    url: optUrl.value.trim(),
    auth: readAuth(),
    // 由按钮显式触发连接，避免页面一打开就连
    autoConnect: false,
    reconnection: optReconnection.checked,
    queueWhileDisconnected: optQueue.checked,
    heartbeatInterval: num(optHeartbeat, 5000),
    reconnectionDelay: num(optDelay, 500),
    reconnectionDelayMax: num(optDelayMax, 5000),
    reconnectionAttempts: Number.isFinite(attempts) ? attempts : Number.POSITIVE_INFINITY,
  }
}

// ---------------------------------------------------------------- 内置事件

const BUILTIN_EVENTS: SocketEventName[] = [
  'connect',
  'disconnect',
  'connect_error',
  'error',
  'reconnect_attempt',
  'reconnect',
  'reconnect_error',
  'reconnect_failed',
  'ping',
  'pong',
  'message',
  'statechange',
]

interface BuiltinRow {
  name: SocketEventName
  count: number
  last: string
  countCell: HTMLElement
  lastCell: HTMLElement
}

const builtinRows: BuiltinRow[] = []

function buildBuiltinRows(): void {
  builtinBody.innerHTML = ''
  builtinRows.length = 0
  for (const name of BUILTIN_EVENTS) {
    const tr = document.createElement('tr')
    const tdName = document.createElement('td')
    tdName.className = 'ev'
    tdName.textContent = name
    const tdCount = document.createElement('td')
    tdCount.textContent = '0'
    const tdLast = document.createElement('td')
    tdLast.className = 'pay'
    tdLast.textContent = '–'
    tr.append(tdName, tdCount, tdLast)
    builtinBody.appendChild(tr)
    builtinRows.push({ name, count: 0, last: '–', countCell: tdCount, lastCell: tdLast })
  }
}

function markBuiltin(name: SocketEventName, payload?: unknown): void {
  const row = builtinRows.find((item) => item.name === name)
  if (!row) return
  row.count += 1
  row.countCell.textContent = String(row.count)
  if (payload !== undefined) {
    row.last = short(payload, 40)
    row.lastCell.textContent = row.last
    row.lastCell.title = row.last
  }
  chipBuiltin.textContent = `${name} × ${row.count}`
}

/** 订阅全部内置生命周期事件，驱动统计与日志 */
function bindBuiltin(sock: WebSocketClient): void {
  sock.on('connect', () => {
    markBuiltin('connect')
    log('● connect 连接建立', 'log-connect')
  })

  sock.on('disconnect', (payload) => {
    markBuiltin('disconnect', payload)
    log(`● disconnect code=${payload.code} reason="${payload.reason}" clean=${payload.wasClean}`, 'log-disconnect')
  })

  sock.on('connect_error', (err) => {
    markBuiltin('connect_error', err.message)
    log(`● connect_error [${err.code}] ${err.message}`, 'log-error')
  })

  sock.on('error', (err) => {
    markBuiltin('error', err.message)
    log(`● error [${err.code}] ${err.message}`, 'log-error')
  })

  sock.on('reconnect_attempt', (payload) => {
    markBuiltin('reconnect_attempt', payload.attempt)
    statAttempts.textContent = String(payload.attempt)
    log(`↻ reconnect_attempt 第 ${payload.attempt} 次（指数退避 + 抖动）`, 'log-reconnect')
  })

  sock.on('reconnect', (payload) => {
    markBuiltin('reconnect', payload.attempt)
    log(`↻ reconnect 重连成功（共尝试 ${payload.attempt} 次）`, 'log-reconnect')
  })

  sock.on('reconnect_error', (err) => {
    markBuiltin('reconnect_error', err.message)
    log(`↻ reconnect_error [${err.code}] ${err.message}`, 'log-error')
  })

  sock.on('reconnect_failed', () => {
    markBuiltin('reconnect_failed')
    log('↻ reconnect_failed 重试次数耗尽，放弃重连', 'log-error')
  })

  sock.on('ping', () => {
    markBuiltin('ping')
    pingCount += 1
    statPing.textContent = String(pingCount)
  })

  sock.on('pong', (payload) => {
    markBuiltin('pong', payload.latency)
    pongCount += 1
    statPong.textContent = String(pongCount)
    statLatency.textContent = `${payload.latency} ms`
    log(`♡ pong 往返 ${payload.latency}ms`, 'log-heartbeat')
  })

  sock.on('message', (payload) => {
    markBuiltin('message', payload)
    log(`◀ message（非协议帧透传）${short(payload)}`, 'log-event')
  })

  sock.on('statechange', (payload) => {
    markBuiltin('statechange', payload.state)
    statState.textContent = payload.state
    setChip(chipState, payload.state, stateTone(payload.state))
  })
}

// ---------------------------------------------------------------- 事件订阅卡片

const SUB_EVENTS = [
  'server:hello',
  'server:tick',
  'server:ask',
  'echo:reply',
  'time:reply',
  'server:event',
  'metro:trains',
  'metro:arrival',
]

interface SubRow {
  event: string
  enabled: boolean
  off: (() => void) | null
  toggle: HTMLInputElement
  lastCell: HTMLElement
}

const subRows: SubRow[] = []

function buildSubRows(): void {
  subBody.innerHTML = ''
  subRows.length = 0
  for (const event of SUB_EVENTS) {
    const tr = document.createElement('tr')
    const tdName = document.createElement('td')
    tdName.className = 'ev'
    tdName.textContent = event

    const tdToggle = document.createElement('td')
    const toggle = document.createElement('input')
    toggle.type = 'checkbox'
    toggle.checked = true
    tdToggle.appendChild(toggle)

    const tdLast = document.createElement('td')
    tdLast.className = 'pay'
    tdLast.textContent = '–'

    tr.append(tdName, tdToggle, tdLast)
    subBody.appendChild(tr)

    const row: SubRow = { event, enabled: true, off: null, toggle, lastCell: tdLast }
    toggle.addEventListener('change', () => {
      row.enabled = toggle.checked
      if (socket) applySubscription(row, socket)
      log(`${row.enabled ? '订阅' : '取消订阅'} ${event}`, 'log-ctl')
      refreshListenerChip()
    })
    subRows.push(row)
  }
}

function applySubscription(row: SubRow, sock: WebSocketClient): void {
  row.off?.()
  row.off = null
  if (!row.enabled) {
    row.lastCell.textContent = '–'
    return
  }
  row.off = sock.on(row.event, (payload: unknown, ack?: AckCallback) => {
    row.lastCell.textContent = short(payload)
    row.lastCell.title = short(payload, 200)
    log(`◀ ${row.event} ${short(payload)}`, 'log-event')

    // 地铁实时数据：驱动 ⑤ 号卡片的列表渲染
    if (row.event === 'metro:trains') {
      renderMetro(payload as MetroSnapshot)
    } else if (row.event === 'metro:arrival') {
      renderArrival(payload as { trainId: string; station: string; direction: string; at: number })
    }

    // 服务端 server:ask 要求应答：用注入的 ack 回调回传
    if (ack) {
      const reply = { answer: new Date().toISOString(), from: 'demo-client' }
      ack(reply)
      setChip(chipAck, `ack 已回传 ${short(reply.answer, 20)}`, 'ok')
      log(`▶ 回应 server:ask 的 ack：${short(reply)}`, 'log-event')
    }
  })
}

function refreshListenerChip(): void {
  const count = subRows.filter((row) => row.enabled).length
  setChip(chipListeners, `${count} 个监听`, 'info')
}

// ---------------------------------------------------------------- 地铁卡片

interface MetroTrain {
  id: string
  line: string
  direction: string
  currentStation: string
  nextStation: string
  status: 'running' | 'arriving' | 'dwelling'
  etaSeconds: number
  passengers: number
}

interface MetroSnapshot {
  at: number
  line: string
  trains: MetroTrain[]
}

const STATUS_TEXT: Record<MetroTrain['status'], string> = {
  running: '区间运行',
  arriving: '即将进站',
  dwelling: '站内停靠',
}

/** 行缓存：按车次 id 复用 DOM，避免每秒重建整张表 */
const metroRows = new Map<string, { tr: HTMLTableRowElement; cells: Record<string, HTMLTableCellElement> }>()

function getMetroRow(trainId: string): { tr: HTMLTableRowElement; cells: Record<string, HTMLTableCellElement> } {
  const cached = metroRows.get(trainId)
  if (cached) return cached

  const tr = document.createElement('tr')
  const keys = ['id', 'direction', 'current', 'next', 'status', 'eta', 'passengers']
  const cells: Record<string, HTMLTableCellElement> = {}
  for (const key of keys) {
    const td = document.createElement('td')
    td.textContent = '–'
    tr.appendChild(td)
    cells[key] = td
  }
  cells.id.className = 'ev num'
  cells.eta.className = 'num'
  cells.passengers.className = 'num'

  const row = { tr, cells }
  metroRows.set(trainId, row)
  return row
}

function renderMetro(snapshot: MetroSnapshot): void {
  if (metroBody.firstElementChild?.tagName === 'TR' && !metroRows.size) metroBody.innerHTML = ''

  for (const train of snapshot.trains) {
    const { tr, cells } = getMetroRow(train.id)
    cells.id.textContent = train.id
    cells.direction.textContent = train.direction
    cells.current.textContent = train.currentStation
    cells.next.textContent = train.nextStation
    cells.status.textContent = STATUS_TEXT[train.status] ?? train.status
    cells.status.className = `st-${train.status}`
    cells.eta.textContent = `${train.etaSeconds}s`
    cells.passengers.textContent = `${train.passengers} 人`
    if (!tr.isConnected) metroBody.appendChild(tr)
  }

  // 服务端已不再下推的车次（如有）移除
  for (const [id, row] of metroRows) {
    if (!snapshot.trains.some((train) => train.id === id)) {
      row.tr.remove()
      metroRows.delete(id)
    }
  }

  setChip(chipMetro, `${snapshot.line} · ${snapshot.trains.length} 列车`, 'info')
  metroUpdated.textContent = new Date(snapshot.at).toLocaleTimeString('zh-CN', { hour12: false })
}

function renderArrival(payload: { trainId: string; station: string; direction: string; at: number }): void {
  metroArrival.textContent = `${payload.trainId} → ${payload.station}（${payload.direction}）`
}

// ---------------------------------------------------------------- 生命周期

/** 销毁现有实例（不打印日志），保留 UI 与订阅状态 */
function teardown(): void {
  if (socket) {
    socket.destroy()
    socket = null
  }
  for (const row of subRows) {
    row.off = null
    row.lastCell.textContent = '–'
  }
  // 清空地铁列表：断开后不再有推送，恢复空态占位
  metroRows.clear()
  metroBody.innerHTML = '<tr><td colspan="7" style="color:#8b98a5">尚未收到数据，点击「连接」后开始接收推送。</td></tr>'
  metroArrival.textContent = '–'
  metroUpdated.textContent = '–'
  setChip(chipMetro, '等待数据', '')
}

/** 按当前配置新建实例并重新接线（可选立即连接） */
function mount(shouldConnect: boolean): void {
  teardown()

  const opts = readOptions()
  if (!opts.url) {
    log('url 为空，无法创建实例', 'log-error')
    return
  }

  const sock = createSocket(opts)
  socket = sock

  sentCount = 0
  pingCount = 0
  pongCount = 0
  emitSent.textContent = '0'
  statPing.textContent = '0'
  statPong.textContent = '0'
  statAttempts.textContent = '0'
  statLatency.textContent = '–'
  for (const row of builtinRows) {
    row.count = 0
    row.countCell.textContent = '0'
    row.last = '–'
    row.lastCell.textContent = '–'
  }

  bindBuiltin(sock)
  for (const row of subRows) applySubscription(row, sock)
  refreshListenerChip()

  chipUrl.textContent = sock.url
  const authText = !opts.auth
    ? '未启用'
    : optAuthDynamic.checked
      ? '函数式（每次连接重新求值）'
      : `对象式 ${optAuthUser.value.trim()}/${optAuthToken.value.trim()}`
  setChip(chipAuth, `auth ${authText}`, opts.auth ? 'info' : '')
  statState.textContent = sock.currentState
  setChip(chipState, sock.currentState, stateTone(sock.currentState))

  log(
    `实例已就绪 url=${sock.url} auth=${authText} 自动重连=${opts.reconnection} 断线排队=${opts.queueWhileDisconnected} 心跳=${opts.heartbeatInterval}ms`,
    'log-ctl',
  )
  log('提示：公开属性 url 是配置里的原始地址（不含 auth 拼出的查询参数）；服务端实际收到的参数见 server:hello 的负载', 'log-ctl')

  if (shouldConnect) sock.connect()
}

// ---------------------------------------------------------------- 发送卡片

/** 读取文本域负载：优先按 JSON 解析，失败则当纯字符串发送 */
function readPayload(): unknown {
  const raw = emitData.value.trim()
  if (!raw) return undefined
  try {
    return JSON.parse(raw)
  } catch {
    return raw
  }
}

function currentEvent(): string {
  const name = emitEv.value.trim()
  return name || 'echo'
}

$('#emit-send').addEventListener('click', () => {
  if (!socket) return
  const event = currentEvent()
  const payload = readPayload()
  socket.emit(event, payload)
  sentCount += 1
  emitSent.textContent = String(sentCount)
  const queued = !socket.connected
  log(`▶ emit "${event}" ${short(payload)}${queued ? '（未连接 → 入队）' : ''}`, 'log-event')
})

$('#emit-ack').addEventListener('click', () => {
  if (!socket) return
  const event = currentEvent()
  const payload = readPayload()
  setChip(chipAck, 'ack 等待中…', 'warn')
  socket.emit(event, payload, (response) => {
    sentCount += 1
    emitSent.textContent = String(sentCount)
    if (response === undefined) {
      setChip(chipAck, 'ack 超时', 'err')
      log(`✕ emit "${event}" 的 ack 超时`, 'log-error')
    } else {
      setChip(chipAck, `ack ✓ ${short(response, 24)}`, 'ok')
      log(`✔ emit "${event}" 收到 ack：${short(response)}`, 'log-event')
    }
  })
  log(`▶ emit+ack "${event}" ${short(payload)}`, 'log-event')
})

$('#emit-promise').addEventListener('click', () => {
  if (!socket) return
  const event = currentEvent()
  const payload = readPayload()
  setChip(chipAck, 'emitWithAck 等待中…', 'warn')
  log(`▶ emitWithAck "${event}" ${short(payload)}`, 'log-event')
  socket
    .emitWithAck(event, payload)
    .then((response) => {
      sentCount += 1
      emitSent.textContent = String(sentCount)
      setChip(chipAck, `ack ✓ ${short(response, 24)}`, 'ok')
      log(`✔ emitWithAck "${event}" resolve：${short(response)}`, 'log-event')
    })
    .catch((err) => {
      setChip(chipAck, 'ack 超时', 'err')
      log(`✕ emitWithAck "${event}" reject [${err.code}] ${err.message}`, 'log-error')
    })
})

$('#emit-queue-test').addEventListener('click', () => {
  if (!socket) return
  for (let i = 1; i <= 5; i += 1) {
    socket.emit('echo', { seq: i, at: Date.now() })
  }
  if (socket.connected) {
    log('已连接，5 条消息即时发出（断开后再试可见入队与补发）', 'log-ctl')
  } else {
    log(`未连接，5 条消息已入队（当前队列 ${socket.queueSize}），连接恢复后按序补发`, 'log-ctl')
  }
})

// ---------------------------------------------------------------- 顶栏控制

$('#ctl-connect').addEventListener('click', () => {
  if (!socket) {
    mount(true)
    return
  }
  socket.connect()
  log('connect()', 'log-ctl')
})

$('#ctl-disconnect').addEventListener('click', () => {
  socket?.disconnect()
  log('disconnect() 主动断开，不再自动重连', 'log-ctl')
})

$('#ctl-destroy').addEventListener('click', () => {
  if (!socket) return
  socket.destroy()
  socket = null
  statState.textContent = 'destroyed'
  setChip(chipState, 'destroyed', 'err')
  log('destroy() 实例已销毁，监听与待处理请求全部清空', 'log-ctl')
})

$('#ctl-rebuild').addEventListener('click', () => {
  log('销毁并重建实例', 'log-ctl')
  mount(true)
})

$('#ctl-connect-opts').addEventListener('click', () => {
  // 配置项在构造期生效，url 变更必须新建实例
  const changedUrl = socket != null && socket.url !== optUrl.value.trim()
  if (!socket || changedUrl) {
    log('按当前配置新建实例并连接', 'log-ctl')
    mount(true)
  } else {
    socket.connect()
    log('connect()（沿用当前配置）', 'log-ctl')
  }
})

$('#ctl-apply').addEventListener('click', () => {
  const wasConnected = socket?.connected ?? false
  log('重建以应用当前配置', 'log-ctl')
  mount(wasConnected)
})

$('#ctl-clear').addEventListener('click', () => {
  panelLog.innerHTML = ''
})

// ---------------------------------------------------------------- 统计轮询

function refreshStats(): void {
  statQueue.textContent = String(socket?.queueSize ?? 0)
  emitQueue.textContent = String(socket?.queueSize ?? 0)
  if (!socket) return
  statState.textContent = socket.currentState
  setChip(chipState, socket.currentState, stateTone(socket.currentState))
}

// ---------------------------------------------------------------- 启动

buildBuiltinRows()
buildSubRows()
refreshListenerChip()
mount(false)

setInterval(refreshStats, 500)
refreshStats()

log('demo 就绪：点击「连接」开始；服务端请用 npm run demo:server 启动', 'log-ctl')
log('验证要点：① 断开看指数退避重连 ② 停掉服务端看 HEARTBEAT_TIMEOUT ③ 未连接时发消息看补发 ④ 看双向 ack', 'log-ctl')
