// src/index.ts
var PKT = {
  EVENT: 0,
  ACK: 1,
  PING: 2,
  PONG: 3
};
var DEFAULTS = {
  autoConnect: true,
  connectTimeout: 2e4,
  reconnection: true,
  reconnectionAttempts: Number.POSITIVE_INFINITY,
  reconnectionDelay: 1e3,
  reconnectionDelayMax: 5e3,
  randomizationFactor: 0.5,
  heartbeatInterval: 25e3,
  heartbeatTimeout: 1e4,
  ackTimeout: 1e4,
  queueWhileDisconnected: true,
  maxQueueSize: 100,
  binaryType: "arraybuffer"
};
function normalizeOptions(options) {
  if (!options || !options.url) {
    throw new Error("[websocket-client-sdk] options.url \u662F\u5FC5\u586B\u9879");
  }
  return { ...DEFAULTS, ...options };
}
function makeError(code, message, cause) {
  return { code, message, cause };
}
function appendAuthToUrl(url, data) {
  if (!data) return url;
  const entries = Object.entries(data).filter(([, v]) => v !== void 0 && v !== null);
  if (entries.length === 0) return url;
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    const sep = url.includes("?") ? "&" : "?";
    return url + sep + entries.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`).join("&");
  }
  for (const [k, v] of entries) parsed.searchParams.set(k, String(v));
  return parsed.toString();
}
var Emitter = class {
  constructor() {
    this.listeners = /* @__PURE__ */ new Map();
  }
  on(event, fn) {
    let set = this.listeners.get(event);
    if (!set) {
      set = /* @__PURE__ */ new Set();
      this.listeners.set(event, set);
    }
    set.add(fn);
    return () => this.off(event, fn);
  }
  off(event, fn) {
    const set = this.listeners.get(event);
    if (!set) return;
    set.delete(fn);
    if (set.size === 0) this.listeners.delete(event);
  }
  emit(event, ...args) {
    const set = this.listeners.get(event);
    if (!set || set.size === 0) return;
    for (const fn of [...set]) {
      try {
        fn(...args);
      } catch (err) {
        console.error(`[websocket-client-sdk] "${event}" \u7684\u76D1\u542C\u5668\u629B\u51FA\u5F02\u5E38:`, err);
      }
    }
  }
  /** 清空所有监听，destroy 时调用 */
  clear() {
    this.listeners.clear();
  }
};
var WebSocketClient = class {
  constructor(options) {
    this.emitter = new Emitter();
    this.ws = null;
    this.state = "idle";
    /** 用户主动断开，禁止自动重连 */
    this.manuallyClosed = false;
    this.destroyed = false;
    this.reconnectAttempts = 0;
    this.reconnectTimer = null;
    /** 握手超时计时器，仅在 connecting 阶段存在 */
    this.connectTimer = null;
    /** 连接序号：用于作废「鉴权参数求值期间被断开」的过期连接尝试 */
    this.connectSeq = 0;
    this.heartbeatTimer = null;
    this.pongTimer = null;
    this.pingSentAt = 0;
    this.queue = [];
    this.pendingAcks = /* @__PURE__ */ new Map();
    this.ackId = 0;
    this.options = normalizeOptions(options);
    if (this.options.autoConnect) this.connect();
  }
  // ---------------------------------------------------------- 公开属性
  /** 当前连接状态 */
  get currentState() {
    return this.state;
  }
  /** 是否已连接 */
  get connected() {
    return this.state === "open";
  }
  /** 连接地址 */
  get url() {
    return this.options.url;
  }
  /** 底层原生实例，需要高级用法时可读写（谨慎） */
  get raw() {
    return this.ws;
  }
  /** 断线期间积压的待发消息数 */
  get queueSize() {
    return this.queue.length;
  }
  // ---------------------------------------------------------- 连接管理
  /** 建立连接（幂等；已在连接中或已连接时直接返回） */
  connect() {
    if (this.destroyed) {
      throw new Error("[websocket-client-sdk] \u5B9E\u4F8B\u5DF2\u9500\u6BC1\uFF0C\u65E0\u6CD5\u91CD\u65B0\u8FDE\u63A5");
    }
    if (this.state === "open" || this.state === "connecting") return this;
    this.manuallyClosed = false;
    this.reconnectAttempts = 0;
    this.openSocket();
    return this;
  }
  /** 主动断开连接，不再自动重连 */
  disconnect(code = 1e3, reason = "client disconnect") {
    if (this.destroyed) return this;
    this.manuallyClosed = true;
    this.clearReconnectTimer();
    this.clearConnectTimer();
    this.stopHeartbeat();
    const ws = this.ws;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) {
      this.setState("closing");
      ws.close(code, reason);
    } else {
      this.setState("closed");
    }
    return this;
  }
  /** 销毁实例：断开连接、清空监听与所有待处理请求 */
  destroy() {
    if (this.destroyed) return;
    this.disconnect();
    this.destroyed = true;
    this.clearReconnectTimer();
    this.clearConnectTimer();
    this.stopHeartbeat();
    const err = makeError("CLOSED", "\u5B9E\u4F8B\u5DF2\u9500\u6BC1");
    for (const pending of this.pendingAcks.values()) {
      if (pending.timer !== null) clearTimeout(pending.timer);
      pending.reject(err);
    }
    this.pendingAcks.clear();
    this.queue.length = 0;
    this.emitter.clear();
  }
  on(event, fn) {
    return this.emitter.on(event, fn);
  }
  once(event, fn) {
    const off = this.on(event, ((...args) => {
      off();
      fn(...args);
    }));
    return off;
  }
  /** 取消订阅 */
  off(event, fn) {
    this.emitter.off(event, fn);
  }
  /**
   * 发送一个命名事件。
   * @param ack 可选应答回调；服务端回传时会以应答数据调用
   */
  emit(event, data, ack) {
    if (ack) {
      const id = ++this.ackId;
      this.pendingAcks.set(id, {
        event,
        resolve: (v) => ack(v),
        reject: () => ack(void 0),
        timer: null
      });
      this.deliver({ t: PKT.EVENT, e: event, d: data, a: id });
    } else {
      this.deliver({ t: PKT.EVENT, e: event, d: data });
    }
    return this;
  }
  /** 发送命名事件并等待应答（Promise 版 ack） */
  emitWithAck(event, data) {
    return new Promise((resolve, reject) => {
      if (this.destroyed) {
        reject(makeError("CLOSED", "\u5B9E\u4F8B\u5DF2\u9500\u6BC1"));
        return;
      }
      const id = ++this.ackId;
      this.pendingAcks.set(id, { event, resolve, reject, timer: null });
      this.deliver({ t: PKT.EVENT, e: event, d: data, a: id });
    });
  }
  /** socket.io 风格别名：等价于 emit('message', data) */
  send(data) {
    return this.emit("message", data);
  }
  // ---------------------------------------------------------- 内部：连接
  openSocket() {
    if (this.destroyed || this.manuallyClosed) return;
    this.clearReconnectTimer();
    this.setState("connecting");
    const seq = ++this.connectSeq;
    void this.prepareAndOpen(seq);
  }
  async prepareAndOpen(seq) {
    let url;
    try {
      url = await this.resolveConnectUrl();
    } catch (err) {
      if (seq !== this.connectSeq || this.destroyed || this.manuallyClosed) return;
      this.reportConnectError(makeError("CONNECT_ERROR", "\u9274\u6743\u53C2\u6570\u83B7\u53D6\u5931\u8D25", err));
      this.scheduleReconnect();
      return;
    }
    if (seq !== this.connectSeq || this.destroyed || this.manuallyClosed) return;
    this.openSocketWithUrl(url);
  }
  /** 求值鉴权数据并合并进连接地址（每次连接/重连都会重新求值） */
  async resolveConnectUrl() {
    const { auth } = this.options;
    if (!auth) return this.options.url;
    const data = typeof auth === "function" ? await auth() : auth;
    return appendAuthToUrl(this.options.url, data);
  }
  openSocketWithUrl(url) {
    if (this.destroyed || this.manuallyClosed) return;
    let ws;
    try {
      ws = this.options.protocols ? new WebSocket(url, this.options.protocols) : new WebSocket(url);
    } catch (err) {
      this.reportConnectError(makeError("CONNECT_ERROR", "WebSocket \u521B\u5EFA\u5931\u8D25", err));
      this.scheduleReconnect();
      return;
    }
    ws.binaryType = this.options.binaryType;
    ws.onopen = () => this.handleOpen();
    ws.onmessage = (ev) => this.handleMessage(ev);
    ws.onerror = () => this.handleSocketError();
    ws.onclose = (ev) => this.handleClose(ev);
    this.ws = ws;
    this.armConnectTimer(ws);
  }
  /**
   * 握手超时保护：某些网络环境下 TCP 已连通但升级握手一直无响应，
   * 既不会触发 onopen 也不一定触发 onerror，连接会永久停在 connecting。
   * 超时后主动关闭该 socket，交给 onclose → scheduleReconnect 走正常重连流程。
   */
  armConnectTimer(ws) {
    this.clearConnectTimer();
    const { connectTimeout } = this.options;
    if (connectTimeout <= 0) return;
    this.connectTimer = setTimeout(() => {
      this.connectTimer = null;
      if (this.ws !== ws || ws.readyState !== WebSocket.CONNECTING) return;
      this.reportConnectError(
        makeError("CONNECT_TIMEOUT", `\u5EFA\u7ACB\u8FDE\u63A5\u8D85\u65F6\uFF08${connectTimeout}ms\uFF09`)
      );
      ws.close();
    }, connectTimeout);
  }
  clearConnectTimer() {
    if (this.connectTimer !== null) {
      clearTimeout(this.connectTimer);
      this.connectTimer = null;
    }
  }
  handleOpen() {
    this.clearConnectTimer();
    const attempts = this.reconnectAttempts;
    this.reconnectAttempts = 0;
    this.setState("open");
    this.startHeartbeat();
    this.flushQueue();
    this.emitter.emit("connect", void 0);
    if (attempts > 0) this.emitter.emit("reconnect", { attempt: attempts });
  }
  handleSocketError() {
    this.reportConnectError(makeError("CONNECT_ERROR", "WebSocket \u8FDE\u63A5\u53D1\u751F\u9519\u8BEF"));
  }
  reportConnectError(err) {
    if (this.reconnectAttempts > 0) this.emitter.emit("reconnect_error", err);
    else this.emitter.emit("connect_error", err);
  }
  handleClose(ev) {
    this.clearConnectTimer();
    this.stopHeartbeat();
    this.ws = null;
    this.emitter.emit("disconnect", {
      code: ev.code,
      reason: ev.reason,
      wasClean: ev.wasClean
    });
    if (this.manuallyClosed || this.destroyed) {
      this.setState("closed");
      return;
    }
    if (this.options.reconnection) this.scheduleReconnect();
    else this.setState("closed");
  }
  // ---------------------------------------------------------- 内部：重连
  scheduleReconnect() {
    const attempt = this.reconnectAttempts + 1;
    if (attempt > this.options.reconnectionAttempts) {
      this.emitter.emit("reconnect_failed", void 0);
      this.setState("closed");
      return;
    }
    this.reconnectAttempts = attempt;
    this.setState("reconnecting");
    this.emitter.emit("reconnect_attempt", { attempt });
    this.reconnectTimer = setTimeout(() => this.openSocket(), this.reconnectDelay(attempt));
  }
  /** 指数退避 + 随机抖动，避免断线后大量客户端同时重连 */
  reconnectDelay(attempt) {
    const { reconnectionDelay, reconnectionDelayMax, randomizationFactor } = this.options;
    const base = Math.min(reconnectionDelayMax, reconnectionDelay * Math.pow(2, attempt - 1));
    const jitter = base * randomizationFactor * (Math.random() * 2 - 1);
    return Math.max(0, Math.round(base + jitter));
  }
  clearReconnectTimer() {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }
  // ---------------------------------------------------------- 内部：心跳
  startHeartbeat() {
    const { heartbeatInterval } = this.options;
    if (heartbeatInterval <= 0) return;
    this.heartbeatTimer = setInterval(() => this.sendPing(), heartbeatInterval);
  }
  stopHeartbeat() {
    if (this.heartbeatTimer !== null) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.pongTimer !== null) {
      clearTimeout(this.pongTimer);
      this.pongTimer = null;
    }
  }
  sendPing() {
    if (!this.connected) return;
    this.pingSentAt = Date.now();
    this.sendPacket({ t: PKT.PING });
    if (this.pongTimer !== null) clearTimeout(this.pongTimer);
    this.pongTimer = setTimeout(() => {
      const err = makeError("HEARTBEAT_TIMEOUT", "\u5FC3\u8DF3\u8D85\u65F6\uFF0C\u8FDE\u63A5\u53EF\u80FD\u5DF2\u65AD\u5F00");
      this.emitter.emit("error", err);
      this.ws?.close(4e3, "heartbeat timeout");
    }, this.options.heartbeatTimeout);
  }
  handlePong() {
    if (this.pongTimer !== null) {
      clearTimeout(this.pongTimer);
      this.pongTimer = null;
    }
    this.emitter.emit("pong", { latency: Date.now() - this.pingSentAt });
  }
  // ---------------------------------------------------------- 内部：收发
  handleMessage(ev) {
    const raw = ev.data;
    if (typeof raw !== "string") {
      this.emitter.emit("message", raw);
      return;
    }
    let packet = null;
    try {
      packet = JSON.parse(raw);
    } catch {
      packet = null;
    }
    if (!packet || typeof packet.t !== "number") {
      this.emitter.emit("message", raw);
      return;
    }
    this.handlePacket(packet);
  }
  handlePacket(packet) {
    switch (packet.t) {
      case PKT.EVENT:
        this.dispatchEvent(packet);
        break;
      case PKT.ACK:
        this.resolveAck(packet);
        break;
      case PKT.PING:
        this.emitter.emit("ping", void 0);
        this.sendPacket({ t: PKT.PONG });
        break;
      case PKT.PONG:
        this.handlePong();
        break;
      default:
        break;
    }
  }
  dispatchEvent(packet) {
    if (!packet.e) return;
    if (packet.a == null) {
      this.emitter.emit(packet.e, packet.d);
      return;
    }
    const ackId = packet.a;
    let replied = false;
    const ack = (response) => {
      if (replied) return;
      replied = true;
      this.sendPacket({ t: PKT.ACK, a: ackId, d: response });
    };
    this.emitter.emit(packet.e, packet.d, ack);
  }
  resolveAck(packet) {
    if (packet.a == null) return;
    const pending = this.pendingAcks.get(packet.a);
    if (!pending) return;
    if (pending.timer !== null) clearTimeout(pending.timer);
    this.pendingAcks.delete(packet.a);
    pending.resolve(packet.d);
  }
  /**
   * 投递一个包：已连接直接发出，未连接时按配置入队。
   * 注意 ack 的超时计时不在这里启动，而是等消息真正写出去之后（见 armAckTimer），
   * 否则断线排队时长一旦超过 ackTimeout，消息还没发就被判定为超时。
   */
  deliver(packet) {
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      if (!this.options.queueWhileDisconnected) {
        this.abortAck(packet.a, makeError("SEND_ERROR", `\u672A\u8FDE\u63A5\uFF0C"${packet.e}" \u53D1\u9001\u5931\u8D25`));
        return;
      }
      this.queue.push(packet);
      if (this.queue.length > this.options.maxQueueSize) {
        const dropped = this.queue.shift();
        this.abortAck(dropped.a, makeError("ACK_TIMEOUT", `\u65AD\u7EBF\u961F\u5217\u5DF2\u6EE1\uFF0C"${dropped.e}" \u88AB\u4E22\u5F03`));
      }
      return;
    }
    this.sendPacket(packet);
  }
  /** 连接恢复后按序补发 */
  flushQueue() {
    while (this.queue.length > 0) {
      this.sendPacket(this.queue.shift());
    }
  }
  sendPacket(packet) {
    const ws = this.ws;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    try {
      ws.send(JSON.stringify(packet));
      this.armAckTimer(packet.a);
    } catch (err) {
      const error = makeError("SEND_ERROR", "\u6D88\u606F\u53D1\u9001\u5931\u8D25", err);
      this.abortAck(packet.a, error);
      this.emitter.emit("error", error);
    }
  }
  /** 消息真正发出后才开始计 ack 超时 */
  armAckTimer(ackId) {
    if (ackId == null) return;
    const pending = this.pendingAcks.get(ackId);
    if (!pending || pending.timer !== null) return;
    pending.timer = setTimeout(() => {
      this.pendingAcks.delete(ackId);
      pending.reject(makeError("ACK_TIMEOUT", `\u7B49\u5F85 "${pending.event}" \u7684\u5E94\u7B54\u8D85\u65F6`));
    }, this.options.ackTimeout);
  }
  /** 消息未能送达（被丢弃或发送失败）时立即终结其待应答，避免回调/Promise 永久挂起 */
  abortAck(ackId, err) {
    if (ackId == null) return;
    const pending = this.pendingAcks.get(ackId);
    if (!pending) return;
    if (pending.timer !== null) clearTimeout(pending.timer);
    this.pendingAcks.delete(ackId);
    pending.reject(err);
  }
  setState(state) {
    if (this.state === state) return;
    this.state = state;
    this.emitter.emit("statechange", { state });
  }
};
function createSocket(options) {
  return new WebSocketClient(options);
}

// demo/main.ts
var $ = (sel) => {
  const el = document.querySelector(sel);
  if (!el) throw new Error(`demo \u9875\u9762\u7F3A\u5C11\u5143\u7D20\uFF1A${sel}`);
  return el;
};
var panelLog = $("#panel-log");
var statState = $("#stat-state");
var statAttempts = $("#stat-attempts");
var statLatency = $("#stat-latency");
var statQueue = $("#stat-queue");
var statPing = $("#stat-ping");
var statPong = $("#stat-pong");
var optUrl = $("#opt-url");
var optAuth = $("#opt-auth");
var optAuthUser = $("#opt-auth-user");
var optAuthToken = $("#opt-auth-token");
var optAuthDynamic = $("#opt-auth-dynamic");
var optReconnection = $("#opt-reconnection");
var optQueue = $("#opt-queue");
var optHeartbeat = $("#opt-heartbeat");
var optDelay = $("#opt-delay");
var optDelayMax = $("#opt-delaymax");
var optAttempts = $("#opt-attempts");
var chipState = $("#chip-state");
var chipUrl = $("#chip-url");
var chipAuth = $("#chip-auth");
var emitEv = $("#emit-ev");
var emitData = $("#emit-data");
var emitQueue = $("#emit-queue");
var emitSent = $("#emit-sent");
var chipAck = $("#chip-ack");
var subBody = $("#sub-rows");
var chipListeners = $("#chip-listeners");
var builtinBody = $("#builtin-rows");
var chipBuiltin = $("#chip-builtin");
var metroBody = $("#metro-rows");
var chipMetro = $("#chip-metro");
var metroArrival = $("#metro-arrival");
var metroUpdated = $("#metro-updated");
function log(message, cls = "") {
  const line = document.createElement("div");
  line.className = cls ? `log-line ${cls}` : "log-line";
  const t = (/* @__PURE__ */ new Date()).toLocaleTimeString("zh-CN", { hour12: false });
  line.textContent = `${t} ${message}`;
  line.title = line.textContent;
  panelLog.appendChild(line);
  while (panelLog.childElementCount > 400) panelLog.removeChild(panelLog.firstElementChild);
  panelLog.scrollTop = panelLog.scrollHeight;
}
function short(payload, max = 64) {
  if (payload === void 0) return "\u2013";
  let text;
  try {
    text = typeof payload === "string" ? payload : JSON.stringify(payload);
  } catch {
    text = String(payload);
  }
  if (text === void 0) text = String(payload);
  return text.length > max ? `${text.slice(0, max)}\u2026` : text;
}
function setChip(el, text, tone = "") {
  el.textContent = text;
  el.className = tone ? `chip ${tone}` : "chip";
}
var stateTone = (state) => {
  if (state === "open") return "ok";
  if (state === "reconnecting" || state === "closing") return "warn";
  if (state === "closed") return "err";
  return "info";
};
var socket = null;
var sentCount = 0;
var pingCount = 0;
var pongCount = 0;
var num = (input, fallback) => {
  const value = Number(input.value);
  return Number.isFinite(value) ? value : fallback;
};
var authEvalCount = 0;
function readAuth() {
  if (!optAuth.checked) return void 0;
  const user = optAuthUser.value.trim();
  const token = optAuthToken.value.trim();
  if (!optAuthDynamic.checked) {
    return { user, token };
  }
  return () => {
    authEvalCount += 1;
    const rotating = { user, token: `demo-token-${Date.now()}-${authEvalCount}` };
    log(`auth \u51FD\u6570\u7B2C ${authEvalCount} \u6B21\u6C42\u503C \u2192 ${short(rotating)}`, "log-ctl");
    return rotating;
  };
}
function readOptions() {
  const attempts = Number(optAttempts.value);
  return {
    url: optUrl.value.trim(),
    auth: readAuth(),
    // 由按钮显式触发连接，避免页面一打开就连
    autoConnect: false,
    reconnection: optReconnection.checked,
    queueWhileDisconnected: optQueue.checked,
    heartbeatInterval: num(optHeartbeat, 5e3),
    reconnectionDelay: num(optDelay, 500),
    reconnectionDelayMax: num(optDelayMax, 5e3),
    reconnectionAttempts: Number.isFinite(attempts) ? attempts : Number.POSITIVE_INFINITY
  };
}
var BUILTIN_EVENTS = [
  "connect",
  "disconnect",
  "connect_error",
  "error",
  "reconnect_attempt",
  "reconnect",
  "reconnect_error",
  "reconnect_failed",
  "ping",
  "pong",
  "message",
  "statechange"
];
var builtinRows = [];
function buildBuiltinRows() {
  builtinBody.innerHTML = "";
  builtinRows.length = 0;
  for (const name of BUILTIN_EVENTS) {
    const tr = document.createElement("tr");
    const tdName = document.createElement("td");
    tdName.className = "ev";
    tdName.textContent = name;
    const tdCount = document.createElement("td");
    tdCount.textContent = "0";
    const tdLast = document.createElement("td");
    tdLast.className = "pay";
    tdLast.textContent = "\u2013";
    tr.append(tdName, tdCount, tdLast);
    builtinBody.appendChild(tr);
    builtinRows.push({ name, count: 0, last: "\u2013", countCell: tdCount, lastCell: tdLast });
  }
}
function markBuiltin(name, payload) {
  const row = builtinRows.find((item) => item.name === name);
  if (!row) return;
  row.count += 1;
  row.countCell.textContent = String(row.count);
  if (payload !== void 0) {
    row.last = short(payload, 40);
    row.lastCell.textContent = row.last;
    row.lastCell.title = row.last;
  }
  chipBuiltin.textContent = `${name} \xD7 ${row.count}`;
}
function bindBuiltin(sock) {
  sock.on("connect", () => {
    markBuiltin("connect");
    log("\u25CF connect \u8FDE\u63A5\u5EFA\u7ACB", "log-connect");
  });
  sock.on("disconnect", (payload) => {
    markBuiltin("disconnect", payload);
    log(`\u25CF disconnect code=${payload.code} reason="${payload.reason}" clean=${payload.wasClean}`, "log-disconnect");
  });
  sock.on("connect_error", (err) => {
    markBuiltin("connect_error", err.message);
    log(`\u25CF connect_error [${err.code}] ${err.message}`, "log-error");
  });
  sock.on("error", (err) => {
    markBuiltin("error", err.message);
    log(`\u25CF error [${err.code}] ${err.message}`, "log-error");
  });
  sock.on("reconnect_attempt", (payload) => {
    markBuiltin("reconnect_attempt", payload.attempt);
    statAttempts.textContent = String(payload.attempt);
    log(`\u21BB reconnect_attempt \u7B2C ${payload.attempt} \u6B21\uFF08\u6307\u6570\u9000\u907F + \u6296\u52A8\uFF09`, "log-reconnect");
  });
  sock.on("reconnect", (payload) => {
    markBuiltin("reconnect", payload.attempt);
    log(`\u21BB reconnect \u91CD\u8FDE\u6210\u529F\uFF08\u5171\u5C1D\u8BD5 ${payload.attempt} \u6B21\uFF09`, "log-reconnect");
  });
  sock.on("reconnect_error", (err) => {
    markBuiltin("reconnect_error", err.message);
    log(`\u21BB reconnect_error [${err.code}] ${err.message}`, "log-error");
  });
  sock.on("reconnect_failed", () => {
    markBuiltin("reconnect_failed");
    log("\u21BB reconnect_failed \u91CD\u8BD5\u6B21\u6570\u8017\u5C3D\uFF0C\u653E\u5F03\u91CD\u8FDE", "log-error");
  });
  sock.on("ping", () => {
    markBuiltin("ping");
    pingCount += 1;
    statPing.textContent = String(pingCount);
  });
  sock.on("pong", (payload) => {
    markBuiltin("pong", payload.latency);
    pongCount += 1;
    statPong.textContent = String(pongCount);
    statLatency.textContent = `${payload.latency} ms`;
    log(`\u2661 pong \u5F80\u8FD4 ${payload.latency}ms`, "log-heartbeat");
  });
  sock.on("message", (payload) => {
    markBuiltin("message", payload);
    log(`\u25C0 message\uFF08\u975E\u534F\u8BAE\u5E27\u900F\u4F20\uFF09${short(payload)}`, "log-event");
  });
  sock.on("statechange", (payload) => {
    markBuiltin("statechange", payload.state);
    statState.textContent = payload.state;
    setChip(chipState, payload.state, stateTone(payload.state));
  });
}
var SUB_EVENTS = [
  "server:hello",
  "server:tick",
  "server:ask",
  "echo:reply",
  "time:reply",
  "server:event",
  "metro:trains",
  "metro:arrival"
];
var subRows = [];
function buildSubRows() {
  subBody.innerHTML = "";
  subRows.length = 0;
  for (const event of SUB_EVENTS) {
    const tr = document.createElement("tr");
    const tdName = document.createElement("td");
    tdName.className = "ev";
    tdName.textContent = event;
    const tdToggle = document.createElement("td");
    const toggle = document.createElement("input");
    toggle.type = "checkbox";
    toggle.checked = true;
    tdToggle.appendChild(toggle);
    const tdLast = document.createElement("td");
    tdLast.className = "pay";
    tdLast.textContent = "\u2013";
    tr.append(tdName, tdToggle, tdLast);
    subBody.appendChild(tr);
    const row = { event, enabled: true, off: null, toggle, lastCell: tdLast };
    toggle.addEventListener("change", () => {
      row.enabled = toggle.checked;
      if (socket) applySubscription(row, socket);
      log(`${row.enabled ? "\u8BA2\u9605" : "\u53D6\u6D88\u8BA2\u9605"} ${event}`, "log-ctl");
      refreshListenerChip();
    });
    subRows.push(row);
  }
}
function applySubscription(row, sock) {
  row.off?.();
  row.off = null;
  if (!row.enabled) {
    row.lastCell.textContent = "\u2013";
    return;
  }
  row.off = sock.on(row.event, (payload, ack) => {
    row.lastCell.textContent = short(payload);
    row.lastCell.title = short(payload, 200);
    log(`\u25C0 ${row.event} ${short(payload)}`, "log-event");
    if (row.event === "metro:trains") {
      renderMetro(payload);
    } else if (row.event === "metro:arrival") {
      renderArrival(payload);
    }
    if (ack) {
      const reply = { answer: (/* @__PURE__ */ new Date()).toISOString(), from: "demo-client" };
      ack(reply);
      setChip(chipAck, `ack \u5DF2\u56DE\u4F20 ${short(reply.answer, 20)}`, "ok");
      log(`\u25B6 \u56DE\u5E94 server:ask \u7684 ack\uFF1A${short(reply)}`, "log-event");
    }
  });
}
function refreshListenerChip() {
  const count = subRows.filter((row) => row.enabled).length;
  setChip(chipListeners, `${count} \u4E2A\u76D1\u542C`, "info");
}
var STATUS_TEXT = {
  running: "\u533A\u95F4\u8FD0\u884C",
  arriving: "\u5373\u5C06\u8FDB\u7AD9",
  dwelling: "\u7AD9\u5185\u505C\u9760"
};
var metroRows = /* @__PURE__ */ new Map();
function getMetroRow(trainId) {
  const cached = metroRows.get(trainId);
  if (cached) return cached;
  const tr = document.createElement("tr");
  const keys = ["id", "direction", "current", "next", "status", "eta", "passengers"];
  const cells = {};
  for (const key of keys) {
    const td = document.createElement("td");
    td.textContent = "\u2013";
    tr.appendChild(td);
    cells[key] = td;
  }
  cells.id.className = "ev num";
  cells.eta.className = "num";
  cells.passengers.className = "num";
  const row = { tr, cells };
  metroRows.set(trainId, row);
  return row;
}
function renderMetro(snapshot) {
  if (metroBody.firstElementChild?.tagName === "TR" && !metroRows.size) metroBody.innerHTML = "";
  for (const train of snapshot.trains) {
    const { tr, cells } = getMetroRow(train.id);
    cells.id.textContent = train.id;
    cells.direction.textContent = train.direction;
    cells.current.textContent = train.currentStation;
    cells.next.textContent = train.nextStation;
    cells.status.textContent = STATUS_TEXT[train.status] ?? train.status;
    cells.status.className = `st-${train.status}`;
    cells.eta.textContent = `${train.etaSeconds}s`;
    cells.passengers.textContent = `${train.passengers} \u4EBA`;
    if (!tr.isConnected) metroBody.appendChild(tr);
  }
  for (const [id, row] of metroRows) {
    if (!snapshot.trains.some((train) => train.id === id)) {
      row.tr.remove();
      metroRows.delete(id);
    }
  }
  setChip(chipMetro, `${snapshot.line} \xB7 ${snapshot.trains.length} \u5217\u8F66`, "info");
  metroUpdated.textContent = new Date(snapshot.at).toLocaleTimeString("zh-CN", { hour12: false });
}
function renderArrival(payload) {
  metroArrival.textContent = `${payload.trainId} \u2192 ${payload.station}\uFF08${payload.direction}\uFF09`;
}
function teardown() {
  if (socket) {
    socket.destroy();
    socket = null;
  }
  for (const row of subRows) {
    row.off = null;
    row.lastCell.textContent = "\u2013";
  }
  metroRows.clear();
  metroBody.innerHTML = '<tr><td colspan="7" style="color:#8b98a5">\u5C1A\u672A\u6536\u5230\u6570\u636E\uFF0C\u70B9\u51FB\u300C\u8FDE\u63A5\u300D\u540E\u5F00\u59CB\u63A5\u6536\u63A8\u9001\u3002</td></tr>';
  metroArrival.textContent = "\u2013";
  metroUpdated.textContent = "\u2013";
  setChip(chipMetro, "\u7B49\u5F85\u6570\u636E", "");
}
function mount(shouldConnect) {
  teardown();
  const opts = readOptions();
  if (!opts.url) {
    log("url \u4E3A\u7A7A\uFF0C\u65E0\u6CD5\u521B\u5EFA\u5B9E\u4F8B", "log-error");
    return;
  }
  const sock = createSocket(opts);
  socket = sock;
  sentCount = 0;
  pingCount = 0;
  pongCount = 0;
  emitSent.textContent = "0";
  statPing.textContent = "0";
  statPong.textContent = "0";
  statAttempts.textContent = "0";
  statLatency.textContent = "\u2013";
  for (const row of builtinRows) {
    row.count = 0;
    row.countCell.textContent = "0";
    row.last = "\u2013";
    row.lastCell.textContent = "\u2013";
  }
  bindBuiltin(sock);
  for (const row of subRows) applySubscription(row, sock);
  refreshListenerChip();
  chipUrl.textContent = sock.url;
  const authText = !opts.auth ? "\u672A\u542F\u7528" : optAuthDynamic.checked ? "\u51FD\u6570\u5F0F\uFF08\u6BCF\u6B21\u8FDE\u63A5\u91CD\u65B0\u6C42\u503C\uFF09" : `\u5BF9\u8C61\u5F0F ${optAuthUser.value.trim()}/${optAuthToken.value.trim()}`;
  setChip(chipAuth, `auth ${authText}`, opts.auth ? "info" : "");
  statState.textContent = sock.currentState;
  setChip(chipState, sock.currentState, stateTone(sock.currentState));
  log(
    `\u5B9E\u4F8B\u5DF2\u5C31\u7EEA url=${sock.url} auth=${authText} \u81EA\u52A8\u91CD\u8FDE=${opts.reconnection} \u65AD\u7EBF\u6392\u961F=${opts.queueWhileDisconnected} \u5FC3\u8DF3=${opts.heartbeatInterval}ms`,
    "log-ctl"
  );
  log("\u63D0\u793A\uFF1A\u516C\u5F00\u5C5E\u6027 url \u662F\u914D\u7F6E\u91CC\u7684\u539F\u59CB\u5730\u5740\uFF08\u4E0D\u542B auth \u62FC\u51FA\u7684\u67E5\u8BE2\u53C2\u6570\uFF09\uFF1B\u670D\u52A1\u7AEF\u5B9E\u9645\u6536\u5230\u7684\u53C2\u6570\u89C1 server:hello \u7684\u8D1F\u8F7D", "log-ctl");
  if (shouldConnect) sock.connect();
}
function readPayload() {
  const raw = emitData.value.trim();
  if (!raw) return void 0;
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}
function currentEvent() {
  const name = emitEv.value.trim();
  return name || "echo";
}
$("#emit-send").addEventListener("click", () => {
  if (!socket) return;
  const event = currentEvent();
  const payload = readPayload();
  socket.emit(event, payload);
  sentCount += 1;
  emitSent.textContent = String(sentCount);
  const queued = !socket.connected;
  log(`\u25B6 emit "${event}" ${short(payload)}${queued ? "\uFF08\u672A\u8FDE\u63A5 \u2192 \u5165\u961F\uFF09" : ""}`, "log-event");
});
$("#emit-ack").addEventListener("click", () => {
  if (!socket) return;
  const event = currentEvent();
  const payload = readPayload();
  setChip(chipAck, "ack \u7B49\u5F85\u4E2D\u2026", "warn");
  socket.emit(event, payload, (response) => {
    sentCount += 1;
    emitSent.textContent = String(sentCount);
    if (response === void 0) {
      setChip(chipAck, "ack \u8D85\u65F6", "err");
      log(`\u2715 emit "${event}" \u7684 ack \u8D85\u65F6`, "log-error");
    } else {
      setChip(chipAck, `ack \u2713 ${short(response, 24)}`, "ok");
      log(`\u2714 emit "${event}" \u6536\u5230 ack\uFF1A${short(response)}`, "log-event");
    }
  });
  log(`\u25B6 emit+ack "${event}" ${short(payload)}`, "log-event");
});
$("#emit-promise").addEventListener("click", () => {
  if (!socket) return;
  const event = currentEvent();
  const payload = readPayload();
  setChip(chipAck, "emitWithAck \u7B49\u5F85\u4E2D\u2026", "warn");
  log(`\u25B6 emitWithAck "${event}" ${short(payload)}`, "log-event");
  socket.emitWithAck(event, payload).then((response) => {
    sentCount += 1;
    emitSent.textContent = String(sentCount);
    setChip(chipAck, `ack \u2713 ${short(response, 24)}`, "ok");
    log(`\u2714 emitWithAck "${event}" resolve\uFF1A${short(response)}`, "log-event");
  }).catch((err) => {
    setChip(chipAck, "ack \u8D85\u65F6", "err");
    log(`\u2715 emitWithAck "${event}" reject [${err.code}] ${err.message}`, "log-error");
  });
});
$("#emit-queue-test").addEventListener("click", () => {
  if (!socket) return;
  for (let i = 1; i <= 5; i += 1) {
    socket.emit("echo", { seq: i, at: Date.now() });
  }
  if (socket.connected) {
    log("\u5DF2\u8FDE\u63A5\uFF0C5 \u6761\u6D88\u606F\u5373\u65F6\u53D1\u51FA\uFF08\u65AD\u5F00\u540E\u518D\u8BD5\u53EF\u89C1\u5165\u961F\u4E0E\u8865\u53D1\uFF09", "log-ctl");
  } else {
    log(`\u672A\u8FDE\u63A5\uFF0C5 \u6761\u6D88\u606F\u5DF2\u5165\u961F\uFF08\u5F53\u524D\u961F\u5217 ${socket.queueSize}\uFF09\uFF0C\u8FDE\u63A5\u6062\u590D\u540E\u6309\u5E8F\u8865\u53D1`, "log-ctl");
  }
});
$("#ctl-connect").addEventListener("click", () => {
  if (!socket) {
    mount(true);
    return;
  }
  socket.connect();
  log("connect()", "log-ctl");
});
$("#ctl-disconnect").addEventListener("click", () => {
  socket?.disconnect();
  log("disconnect() \u4E3B\u52A8\u65AD\u5F00\uFF0C\u4E0D\u518D\u81EA\u52A8\u91CD\u8FDE", "log-ctl");
});
$("#ctl-destroy").addEventListener("click", () => {
  if (!socket) return;
  socket.destroy();
  socket = null;
  statState.textContent = "destroyed";
  setChip(chipState, "destroyed", "err");
  log("destroy() \u5B9E\u4F8B\u5DF2\u9500\u6BC1\uFF0C\u76D1\u542C\u4E0E\u5F85\u5904\u7406\u8BF7\u6C42\u5168\u90E8\u6E05\u7A7A", "log-ctl");
});
$("#ctl-rebuild").addEventListener("click", () => {
  log("\u9500\u6BC1\u5E76\u91CD\u5EFA\u5B9E\u4F8B", "log-ctl");
  mount(true);
});
$("#ctl-connect-opts").addEventListener("click", () => {
  const changedUrl = socket != null && socket.url !== optUrl.value.trim();
  if (!socket || changedUrl) {
    log("\u6309\u5F53\u524D\u914D\u7F6E\u65B0\u5EFA\u5B9E\u4F8B\u5E76\u8FDE\u63A5", "log-ctl");
    mount(true);
  } else {
    socket.connect();
    log("connect()\uFF08\u6CBF\u7528\u5F53\u524D\u914D\u7F6E\uFF09", "log-ctl");
  }
});
$("#ctl-apply").addEventListener("click", () => {
  const wasConnected = socket?.connected ?? false;
  log("\u91CD\u5EFA\u4EE5\u5E94\u7528\u5F53\u524D\u914D\u7F6E", "log-ctl");
  mount(wasConnected);
});
$("#ctl-clear").addEventListener("click", () => {
  panelLog.innerHTML = "";
});
function refreshStats() {
  statQueue.textContent = String(socket?.queueSize ?? 0);
  emitQueue.textContent = String(socket?.queueSize ?? 0);
  if (!socket) return;
  statState.textContent = socket.currentState;
  setChip(chipState, socket.currentState, stateTone(socket.currentState));
}
buildBuiltinRows();
buildSubRows();
refreshListenerChip();
mount(false);
setInterval(refreshStats, 500);
refreshStats();
log("demo \u5C31\u7EEA\uFF1A\u70B9\u51FB\u300C\u8FDE\u63A5\u300D\u5F00\u59CB\uFF1B\u670D\u52A1\u7AEF\u8BF7\u7528 npm run demo:server \u542F\u52A8", "log-ctl");
log("\u9A8C\u8BC1\u8981\u70B9\uFF1A\u2460 \u65AD\u5F00\u770B\u6307\u6570\u9000\u907F\u91CD\u8FDE \u2461 \u505C\u6389\u670D\u52A1\u7AEF\u770B HEARTBEAT_TIMEOUT \u2462 \u672A\u8FDE\u63A5\u65F6\u53D1\u6D88\u606F\u770B\u8865\u53D1 \u2463 \u770B\u53CC\u5411 ack", "log-ctl");
//# sourceMappingURL=main.js.map
