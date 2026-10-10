/**
 * 临时代码：验证服务端地铁到站实时模拟的下推行为。
 * 断言：
 *  1) 握手成功；
 *  2) server:hello 之后立刻收到一份 metro:trains（新连接无空窗）；
 *  3) 随后每秒收到一次 metro:trains，且 trains.length === 4；
 *  4) 约 1 分钟内能收到 metro:arrival，字段齐全。
 */
import { createSocket } from '../../dist/index.js'

const results = []
const ok = (name, pass, extra = '') => {
  results.push({ name, pass, extra })
  console.log(`${pass ? 'PASS' : 'FAIL'}  ${name}${extra ? ` :: ${extra}` : ''}`)
}

const sock = createSocket({
  url: 'ws://localhost:8787/ws',
  auth: { user: 'demo-user', token: 'valid-token' },
  reconnection: false,
})

let helloTrainSnapshot = null
let trainsCount = 0
let snapshotTimes = []
let arrival = null
let firstSnapshotAt = 0

sock.on('connect', () => ok('握手成功 connect', true))

sock.on('server:hello', () => {
  ok('收到 server:hello', true)
  firstSnapshotAt = Date.now()
})

sock.on('metro:trains', (payload) => {
  trainsCount += 1
  snapshotTimes.push(Date.now())
  if (!helloTrainSnapshot && Date.now() - firstSnapshotAt < 50) helloTrainSnapshot = payload
  if (trainsCount === 1) {
    ok('首个 metro:trains 紧跟在 server:hello 之后', Date.now() - firstSnapshotAt < 50, `Δ=${Date.now() - firstSnapshotAt}ms`)
    ok('快照含 4 列车', payload?.trains?.length === 4, `len=${payload?.trains?.length}`)
    const t = payload?.trains?.[0]
    ok(
      '快照字段齐全',
      !!t && ['id', 'line', 'direction', 'currentStation', 'nextStation', 'status', 'etaSeconds', 'passengers'].every((k) => t[k] !== undefined),
      JSON.stringify(t),
    )
  }
})

sock.on('metro:arrival', (payload) => {
  if (arrival) return
  arrival = payload
  ok(
    '收到 metro:arrival 且字段齐全',
    !!payload.trainId && !!payload.station && !!payload.direction && !!payload.nextStation && typeof payload.at === 'number',
    JSON.stringify(payload),
  )
})

sock.connect()

const started = Date.now()
const timer = setInterval(() => {
  if ((arrival && snapshotTimes.length >= 6) || Date.now() - started > 60000) {
    clearInterval(timer)
    // 首个快照是连接瞬间立即下发，不落在全局 1 秒节拍上，统计间隔时跳过
    const ticks = snapshotTimes.slice(1)
    const span = ticks.length > 1 ? (ticks.at(-1) - ticks[0]) / (ticks.length - 1) : 0
    ok('每秒稳定收到 metro:trains', ticks.length >= 4 && span >= 800 && span <= 1200, `count=${snapshotTimes.length} 平均间隔=${Math.round(span)}ms`)
    ok('收到到站事件 metro:arrival', !!arrival, arrival ? arrival.station : `${Math.round((Date.now() - started) / 1000)}s 内未收到`)

    sock.destroy()
    const failed = results.filter((r) => !r.pass)
    console.log(`\n结果：${results.length - failed.length}/${results.length} 通过`)
    setTimeout(() => process.exit(failed.length ? 1 : 0), 200)
  }
}, 500)
