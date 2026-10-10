/**
 * websocket-client-sdk 演示服务端（零依赖）
 *
 * 同时承担两件事：
 *   1. 静态托管 demo 目录（index.html / .build/main.js），因此无需再起 python http.server；
 *   2. 在 /ws 路径上实现 SDK 的极简 JSON 帧协议，用于演示连接、心跳、重连、事件与 ack。
 *
 * 启动：npm run demo:server
 */

import { createServer } from 'node:http'
import { createHash } from 'node:crypto'
import { readFile } from 'node:fs/promises'
import { extname, join, normalize } from 'node:path'
import { fileURLToPath } from 'node:url'

const PORT = Number(process.env.PORT) || 8787
const DEMO_DIR = fileURLToPath(new URL('.', import.meta.url))

/* --------------------------------------------------------------- 协议常量 */

const PKT = { EVENT: 0, ACK: 1, PING: 2, PONG: 3 }

/** 握手阶段校验鉴权参数：演示 SDK 的 auth 会被拼进连接地址的查询串 */
const AUTH = {
  /** 静态 token 白名单 */
  staticTokens: new Set(['valid-token']),
  /** 动态 token 前缀：demo 客户端每次连接生成一个新的，用于演示 token 轮换 */
  dynamicPrefix: 'demo-token-',
}

/** 从握手请求里解析鉴权信息；token 合法返回对象，否则返回 { error } */
function readAuth(req) {
  let searchParams
  try {
    searchParams = new URL(req.url, `http://${req.headers.host ?? 'localhost'}`).searchParams
  } catch {
    return { error: '连接地址无法解析' }
  }
  const auth = {}
  for (const [key, value] of searchParams) auth[key] = value

  const token = searchParams.get('token')
  if (!token) return { error: '缺少 token 查询参数' }
  if (!AUTH.staticTokens.has(token) && !token.startsWith(AUTH.dynamicPrefix)) {
    return { error: `token 非法：${token}` }
  }
  return { auth }
}

/** 拒绝握手：回一个普通 HTTP 响应，浏览器 WebSocket 会以 onerror/onclose 结束 */
function rejectUpgrade(socket, status, message) {
  const body = `${status} ${message}`
  socket.write(
    [
      `HTTP/1.1 ${status} ${message}`,
      'Content-Type: text/plain; charset=utf-8',
      `Content-Length: ${Buffer.byteLength(body)}`,
      'Connection: close',
      '\r\n',
      body,
    ].join('\r\n'),
  )
  socket.destroy()
}

/* --------------------------------------------------------- WebSocket 编解码 */

/** 组装一个服务端 → 客户端的帧（服务端发出的帧不加掩码） */
function encodeFrame(payload, opcode = 0x1) {
  const data = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), 'utf8')
  const { length } = data
  let header
  if (length < 126) {
    header = Buffer.alloc(2)
    header[1] = length
  } else if (length < 65536) {
    header = Buffer.alloc(4)
    header[1] = 126
    header.writeUInt16BE(length, 2)
  } else {
    header = Buffer.alloc(10)
    header[1] = 127
    header.writeBigUInt64BE(BigInt(length), 2)
  }
  header[0] = 0x80 | opcode
  return Buffer.concat([header, data])
}

/** 从缓冲区里切出尽可能多的完整帧，返回帧数组与剩余半包 */
function decodeFrames(buffer) {
  const frames = []
  let offset = 0
  while (offset + 2 <= buffer.length) {
    const b0 = buffer[offset]
    const b1 = buffer[offset + 1]
    const opcode = b0 & 0x0f
    const masked = (b1 & 0x80) !== 0
    let length = b1 & 0x7f
    let cursor = offset + 2

    if (length === 126) {
      if (cursor + 2 > buffer.length) break
      length = buffer.readUInt16BE(cursor)
      cursor += 2
    } else if (length === 127) {
      if (cursor + 8 > buffer.length) break
      length = Number(buffer.readBigUInt64BE(cursor))
      cursor += 8
    }

    const maskLength = masked ? 4 : 0
    if (cursor + maskLength + length > buffer.length) break

    let payload = buffer.subarray(cursor + maskLength, cursor + maskLength + length)
    if (masked) {
      const mask = buffer.subarray(cursor, cursor + 4)
      payload = Buffer.from(payload)
      for (let i = 0; i < payload.length; i += 1) payload[i] ^= mask[i % 4]
    }

    frames.push({ opcode, payload })
    offset = cursor + maskLength + length
  }
  return { frames, rest: buffer.subarray(offset) }
}

/* ------------------------------------------------------- 地铁到站实时模拟 */

/** 线路站点：上行按数组顺序，下行反向（广州地铁 21 号线：员村 → 增城广场） */
const METRO_LINE = {
  name: '广州地铁21号线',
  stations: [
    '员村', '天河公园', '棠东', '黄村', '大观南路', '天河智慧城', '神舟路', '科学城', '苏元',
    '水西', '长平', '金坑', '镇龙西', '镇龙', '中新', '坑贝', '凤岗', '朱村', '山田', '钟岗', '增城广场',
  ],
}

const wrapIndex = (index) => ((index % METRO_LINE.stations.length) + METRO_LINE.stations.length) % METRO_LINE.stations.length

let trainSeq = 0

/** 造一列车：index 为当前所在站，direction 为 +1 上行 / -1 下行 */
function makeTrain(index, direction) {
  trainSeq += 1
  return {
    id: `M${String(trainSeq).padStart(4, '0')}`,
    index: wrapIndex(index),
    direction,
    status: 'running',
    etaSeconds: 6 + Math.floor(Math.random() * 18),
    dwellSeconds: 0,
    passengers: 80 + Math.floor(Math.random() * 220),
  }
}

/** 全局共享的一份模拟状态：所有客户端看到同一条时间线 */
const trains = [makeTrain(0, 1), makeTrain(3, 1), makeTrain(5, -1), makeTrain(8, -1)]

/** 推进 1 秒，返回本拍「到站」的车次列表 */
function advanceMetro() {
  const arrivals = []
  for (const train of trains) {
    if (train.status === 'dwelling') {
      train.dwellSeconds -= 1
      if (train.dwellSeconds <= 0) {
        // 发车：重新在区间内运行
        train.passengers = Math.max(30, Math.min(320, train.passengers + Math.round((Math.random() - 0.45) * 120)))
        train.etaSeconds = 18 + Math.floor(Math.random() * 40)
        train.status = 'running'
      }
      continue
    }
    train.etaSeconds -= 1
    if (train.etaSeconds > 0) {
      // 最后 3 秒标记为「进站中」，便于前端区分
      train.status = train.etaSeconds <= 3 ? 'arriving' : 'running'
      continue
    }
    // 抵达下一站，开始停站开门
    train.index = wrapIndex(train.index + train.direction)
    train.etaSeconds = 0
    train.dwellSeconds = 8 + Math.floor(Math.random() * 8)
    train.status = 'dwelling'
    arrivals.push(train)
  }
  return arrivals
}

function metroSnapshot() {
  return {
    at: Date.now(),
    line: METRO_LINE.name,
    trains: trains.map((train) => ({
      id: train.id,
      line: METRO_LINE.name,
      direction: train.direction > 0 ? '上行' : '下行',
      currentStation: METRO_LINE.stations[train.index],
      nextStation: METRO_LINE.stations[wrapIndex(train.index + train.direction)],
      status: train.status,
      etaSeconds: train.status === 'dwelling' ? train.dwellSeconds : train.etaSeconds,
      passengers: train.passengers,
    })),
  }
}

/* ------------------------------------------------------------------ 会话 */

let clientSeq = 0

/** 已连接客户端的下推函数集合：地铁模拟按全局节拍向所有客户端广播同一份数据 */
const clients = new Set()

/** 向所有在线客户端广播一个事件 */
function broadcast(event, payload) {
  const packet = { t: PKT.EVENT, e: event, d: payload }
  for (const push of clients) push(packet)
}

function handleConnection(socket, auth = {}) {
  const id = ++clientSeq
  const tag = `[client#${id}]`
  let buffer = Buffer.alloc(0)
  let closed = false
  /** 服务端主动下推的定时器集合，连接关闭时统一清理 */
  const timers = []

  const send = (text) => {
    if (!closed) socket.write(encodeFrame(text))
  }
  const sendPacket = (packet) => send(JSON.stringify(packet))

  // 注册到广播集合：地铁模拟按全局节拍向所有客户端推送
  clients.add(sendPacket)

  const cleanup = () => {
    if (closed) return
    closed = true
    clients.delete(sendPacket)
    for (const timer of timers) clearInterval(timer)
    timers.length = 0
    console.log(`${tag} 已断开`)
  }

  console.log(
    `${tag} 已连接 鉴权参数=${JSON.stringify(auth)}`,
  )

  socket.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk])
    const { frames, rest } = decodeFrames(buffer)
    buffer = rest

    for (const frame of frames) {
      if (frame.opcode === 0x8) {
        socket.write(encodeFrame(Buffer.alloc(0), 0x8))
        socket.end()
        cleanup()
        return
      }
      if (frame.opcode === 0x9) {
        socket.write(encodeFrame(frame.payload, 0xa))
        continue
      }
      if (frame.opcode === 0xa) continue
      if (frame.opcode !== 0x1) continue

      let packet = null
      try {
        packet = JSON.parse(frame.payload.toString('utf8'))
      } catch {
        packet = null
      }
      if (!packet || typeof packet.t !== 'number') continue
      handlePacket(packet)
    }
  })

  socket.on('close', cleanup)
  socket.on('error', cleanup)

  function handlePacket(packet) {
    switch (packet.t) {
      case PKT.EVENT: {
        const name = packet.e
        console.log(`${tag} 收到事件 "${name}"`, packet.d ?? '')
        if (typeof packet.a === 'number') {
          // 对端要求应答：回一个 ACK 帧
          sendPacket({ t: PKT.ACK, a: packet.a, d: { ok: true, echo: packet.d ?? null, at: Date.now() } })
        }
        if (name === 'echo') {
          sendPacket({ t: PKT.EVENT, e: 'echo:reply', d: packet.d ?? null })
        } else if (name === 'time') {
          sendPacket({ t: PKT.EVENT, e: 'time:reply', d: new Date().toISOString() })
        } else if (name !== 'message') {
          sendPacket({ t: PKT.EVENT, e: 'server:event', d: { name, payload: packet.d ?? null } })
        }
        break
      }
      case PKT.PING:
        sendPacket({ t: PKT.PONG })
        break
      case PKT.PONG:
        // 对上服务端心跳的回应，此处静默即可
        break
      case PKT.ACK:
        console.log(`${tag} 收到对 ack#${packet.a} 的应答`, packet.d ?? '')
        break
      default:
        break
    }
  }

  // ① 服务端定时下推事件，用于演示事件订阅
  sendPacket({ t: PKT.EVENT, e: 'server:hello', d: { clientId: id, at: Date.now(), auth } })
  // 新连接立即收到一份地铁全量快照，随后跟随全局节拍增量刷新
  sendPacket({ t: PKT.EVENT, e: 'metro:trains', d: metroSnapshot() })
  timers.push(
    setInterval(() => {
      sendPacket({ t: PKT.EVENT, e: 'server:tick', d: { at: Date.now() } })
    }, 3000),
  )

  // ② 服务端定时发心跳 ping，客户端的 'ping' 事件会自动回 pong
  timers.push(
    setInterval(() => {
      sendPacket({ t: PKT.PING })
    }, 15000),
  )

  // ③ 演示「对端要求应答」：客户端在 'server:ask' 上用 ack 回调回传结果
  timers.push(
    setTimeout(() => {
      let ackId = 1
      const ask = setInterval(() => {
        sendPacket({ t: PKT.EVENT, e: 'server:ask', d: { question: '现在几点？' }, a: ackId })
        console.log(`${tag} 向客户端发出 server:ask（等待 ack#${ackId}）`)
        ackId += 1
      }, 20000)
      timers.push(ask)
    }, 8000),
  )
}

/* ------------------------------------------------------------ 静态文件托管 */

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
}

async function serveStatic(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`)
  const pathname = url.pathname === '/' ? '/index.html' : url.pathname
  // 阻断 ../ 穿越，只允许访问 demo 目录内的文件
  const target = normalize(join(DEMO_DIR, pathname))
  if (!target.startsWith(normalize(DEMO_DIR))) {
    res.writeHead(403).end('Forbidden')
    return
  }
  try {
    const file = await readFile(target)
    res.writeHead(200, { 'Content-Type': MIME[extname(target)] ?? 'application/octet-stream' })
    res.end(file)
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' })
    res.end('404 Not Found（请先执行 npm run demo:build 生成 .build/main.js）')
  }
}

/* ------------------------------------------------------------------ 启动 */

// 地铁模拟全局节拍：每秒推进一次，向所有在线客户端广播全量快照；
// 有车到站时额外补一条 metro:arrival 事件（演示「全量快照 + 增量事件」两种下推形态）。
setInterval(() => {
  const arrivals = advanceMetro()
  if (clients.size === 0) return
  broadcast('metro:trains', metroSnapshot())
  for (const train of arrivals) {
    const station = METRO_LINE.stations[train.index]
    console.log(`[metro] ${train.id} 到站 ${station}（${train.direction > 0 ? '上行' : '下行'}）`)
    broadcast('metro:arrival', {
      at: Date.now(),
      line: METRO_LINE.name,
      trainId: train.id,
      direction: train.direction > 0 ? '上行' : '下行',
      station,
      nextStation: METRO_LINE.stations[wrapIndex(train.index + train.direction)],
      passengers: train.passengers,
    })
  }
}, 1000)

const server = createServer((req, res) => {
  if (req.url === '/favicon.ico') {
    res.writeHead(204).end()
    return
  }
  serveStatic(req, res)
})

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key']
  if (req.headers.upgrade?.toLowerCase() !== 'websocket' || !key) {
    socket.destroy()
    return
  }

  // 握手阶段校验 auth：SDK 传函数式 auth 时，每次连接/重连都会重新求值并拼到查询串上
  const { auth, error } = readAuth(req)
  if (error) {
    console.log(`[upgrade] 拒绝握手：${error}（${req.url}）`)
    rejectUpgrade(socket, 401, 'Unauthorized')
    return
  }

  const accept = createHash('sha1')
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest('base64')

  socket.write(
    [
      'HTTP/1.1 101 Switching Protocols',
      'Upgrade: websocket',
      'Connection: Upgrade',
      `Sec-WebSocket-Accept: ${accept}`,
      '\r\n',
    ].join('\r\n'),
  )
  socket.setNoDelay(true)
  handleConnection(socket, auth)
})

server.listen(PORT, () => {
  console.log(`websocket-client-sdk demo 已启动`)
  console.log(`  页面：http://localhost:${PORT}/`)
  console.log(`  协议：ws://localhost:${PORT}/ws`)
})
