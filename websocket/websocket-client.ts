/**
 * `randomUUID` 来自 Node.js 内置的 `node:crypto` 模块。
 * 用于生成符合 RFC 4122 标准的 UUID v4 字符串，格式如：
 *   "550e8400-e29b-41d4-a716-446655440000"
 * 每次调用都会生成一个全局唯一的随机字符串，用作消息的 msg_id。
 * 注意：这是 Node.js 原生 API，不需要安装任何第三方库。
 */
import { randomUUID } from "node:crypto";
import WebSocket from "ws";
import type {
  AGPEnvelope,
  AGPMethod,
  WebSocketClientConfig,
  ConnectionState,
  WebSocketClientCallbacks,
  PromptMessage,
  CancelMessage,
  UpdatePayload,
  PromptResponsePayload,
  ContentBlock,
  ToolCall,
} from "./types.js";

// ============================================
// WebSocket 客户端核心
// ============================================
// 负责 WebSocket 连接管理、消息收发、自动重连、心跳保活

/**
 * WebSocket 客户端
 * @description
 * 连接到 AGP WebSocket 服务端，处理双向通信：
 * - 接收下行消息：session.prompt / session.cancel
 * - 发送上行消息：session.update / session.promptResponse
 * - 自动重连：连接断开后自动尝试重连（指数退避策略）
 * - 心跳保活：定期发送 WebSocket 原生 ping 帧，防止服务端因空闲超时断开连接
 * - 消息去重：通过 msg_id 实现幂等处理，避免重复消息被处理两次
 */
export class WechatAccessWebSocketClient {
  private config: Required<Omit<WebSocketClientConfig, "token" | "gatewayPort">> & { token?: string; gatewayPort?: string };
  private callbacks: WebSocketClientCallbacks;

  /**
   * ws 库的 WebSocket 实例。
   * 类型写作 `WebSocket.WebSocket` 是因为 ws 库的默认导出是类本身，
   * 而 `WebSocket.WebSocket` 是其实例类型（TypeScript 类型系统的要求）。
   * 未连接时为 null。
   */
  private ws: WebSocket | null = null;

  /** 当前连接状态 */
  private state: ConnectionState = "disconnected";

  /**
   * 重连定时器句柄。
   * `ReturnType<typeof setTimeout>` 是 TypeScript 推荐的写法，
   * 可以同时兼容 Node.js（返回 Timeout 对象）和浏览器（返回 number）环境。
   */
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * 心跳定时器句柄。
   * `ReturnType<typeof setInterval>` 同上，兼容 Node.js 和浏览器。
   */
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  /** 当前已尝试的重连次数 */
  private reconnectAttempts = 0;

  /**
   * 已处理的消息 ID 集合（用于去重）。
   * 使用 Set 而非数组，查找时间复杂度为 O(1)。
   * 当消息因网络问题被重发时，通过检查 msg_id 是否已存在来避免重复处理。
   */
  private processedMsgIds = new Set<string>();

  /** 消息 ID 缓存定期清理定时器（防止 Set 无限增长导致内存泄漏） */
  private msgIdCleanupTimer: ReturnType<typeof setInterval> | null = null;

  /** 上次收到 pong 的时间戳（用于检测连接假死） */
  private lastPongTime = Date.now();

  /** 系统唤醒检测定时器 */
  private wakeupCheckTimer: ReturnType<typeof setInterval> | null = null;

  /** 唤醒检测：上次 tick 的时间戳 */
  private lastTickTime = Date.now();

  /** 消息 ID 缓存的最大容量，超过此值时触发清理 */
  private static readonly MAX_MSG_ID_CACHE = 1000;

  /** 从 config.url 中解析出端口号，用于日志前缀 */
  private get port(): string {
    return this.config.gatewayPort ?? 'unknown';
  }

  /** 带端口号的日志前缀 */
  private get logPrefix(): string {
    return `[wechat-access-ws:${this.port}]`;
  }

  constructor(config: WebSocketClientConfig, callbacks: WebSocketClientCallbacks = {}) {
    this.config = {
      url: config.url,
      guid: config.guid ?? '',
      userId: config.userId ?? '',
      token: config.token,
      gatewayPort: config.gatewayPort,
      reconnectInterval: config.reconnectInterval ?? 3000,
      maxReconnectAttempts: config.maxReconnectAttempts ?? 0,
      // 默认 20s发一次心跳，小于服务端 1 分钟的空闲超时时间
      heartbeatInterval: config.heartbeatInterval ?? 20000,
    };
    this.callbacks = callbacks;
  }

  /**
   * 启动 WebSocket 连接
   * @description
   * 如果当前已连接或正在连接中，则直接返回，避免重复建立连接。
   * 同时启动消息 ID 缓存的定期清理任务。
   */
  start = (): void => {
    if (this.state === "connected" || this.state === "connecting") {
      console.log(`${this.logPrefix} 已连接或正在连接，跳过`);
      return;
    }
    this.connect();
    this.startMsgIdCleanup();
  };

  /**
   * 停止 WebSocket 连接
   * @description
   * 主动断开连接时调用。会：
   * 1. 将状态设为 "disconnected"（阻止断开后触发自动重连）
   * 2. 清理所有定时器（重连、心跳、消息 ID 清理）
   * 3. 清空消息 ID 缓存
   * 4. 关闭 WebSocket 连接
   */
  stop = (): void => {
    console.log(`${this.logPrefix} 正在停止...`);
    this.state = "disconnected";
    this.clearReconnectTimer();
    this.clearHeartbeat();
    this.clearWakeupDetection();
    this.clearMsgIdCleanup();
    this.processedMsgIds.clear();

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    console.log(`${this.logPrefix} 已停止`);
  };

  /**
   * 获取当前连接状态
   * @returns "disconnected" | "connecting" | "connected" | "reconnecting"
   */
  getState = (): ConnectionState => this.state;

  /**
   * 更新事件回调
   * @description 使用对象展开合并，只更新传入的回调，保留未传入的原有回调
   */
  setCallbacks = (callbacks: Partial<WebSocketClientCallbacks>): void => {
    this.callbacks = { ...this.callbacks, ...callbacks };
  };

  /**
   * 发送 session.update 消息 — 流式中间更新（文本块）
   * @param sessionId - 所属 Session ID
   * @param promptId - 所属 Turn ID
   * @param content - 文本内容块（type: "text"）
   * @description
   * 在 Agent 生成回复的过程中，将增量文本实时推送给服务端，
   * 服务端再转发给用户端展示流式输出效果。
   */
  sendMessageChunk = (sessionId: string, promptId: string, content: ContentBlock, guid?: string, userId?: string): void => {
    console.log(`${this.logPrefix} [sendMessageChunk] sessionId=${sessionId}, promptId=${promptId}, guid=${guid}, userId=${userId}, content=${JSON.stringify(content).substring(0, 200)}`);
    const payload: UpdatePayload = {
      session_id: sessionId,
      prompt_id: promptId,
      update_type: "message_chunk",
      content,
    };
    this.sendEnvelope("session.update", payload, guid, userId);
  };

  /**
   * 发送 session.update 消息 — 工具调用开始
   * @param sessionId - 所属 Session ID
   * @param promptId - 所属 Turn ID
   * @param toolCall - 工具调用信息（包含 tool_call_id、title、kind、status）
   * @description
   * 当 Agent 开始调用某个工具时发送，通知服务端展示工具调用状态。
   */
  sendToolCall = (sessionId: string, promptId: string, toolCall: ToolCall, guid?: string, userId?: string): void => {
    console.log(`${this.logPrefix} [sendToolCall] sessionId=${sessionId}, promptId=${promptId}, guid=${guid}, userId=${userId}, toolCall=${JSON.stringify(toolCall)}`);
    const payload: UpdatePayload = {
      session_id: sessionId,
      prompt_id: promptId,
      update_type: "tool_call",
      tool_call: toolCall,
    };
    this.sendEnvelope("session.update", payload, guid, userId);
  };

  /**
   * 发送 session.update 消息 — 工具调用状态变更
   * @param sessionId - 所属 Session ID
   * @param promptId - 所属 Turn ID
   * @param toolCall - 更新后的工具调用信息（status 变为 completed/failed）
   * @description
   * 当工具执行完成或失败时发送，通知服务端更新工具调用的展示状态。
   */
  sendToolCallUpdate = (sessionId: string, promptId: string, toolCall: ToolCall, guid?: string, userId?: string): void => {
    console.log(`${this.logPrefix} [sendToolCallUpdate] sessionId=${sessionId}, promptId=${promptId}, guid=${guid}, userId=${userId}, toolCall=${JSON.stringify(toolCall)}`);
    const payload: UpdatePayload = {
      session_id: sessionId,
      prompt_id: promptId,
      update_type: "tool_call_update",
      tool_call: toolCall,
    };
    this.sendEnvelope("session.update", payload, guid, userId);
  };

  /**
   * 发送 session.promptResponse 消息 — 最终结果
   * @param payload - 包含 stop_reason、content、error 等最终结果信息
   * @description
   * Agent 处理完成后发送，告知服务端本次 Turn 已结束。
   * stop_reason 可以是：end_turn（正常完成）、cancelled（被取消）、error（出错）
   */
  sendPromptResponse = (payload: PromptResponsePayload, guid?: string, userId?: string): void => {
    const contentPreview = payload.content ? JSON.stringify(payload.content).substring(0, 200) : '(empty)';
    console.log(`${this.logPrefix} [sendPromptResponse] sessionId=${payload.session_id}, promptId=${payload.prompt_id}, stopReason=${payload.stop_reason}, guid=${guid}, userId=${userId}, content=${contentPreview}`);
    this.sendEnvelope("session.promptResponse", payload, guid, userId);
  };


  /**
   * 建立 WebSocket 连接
   * @description
   * 使用 ws 库的 `new WebSocket(url)` 创建连接。
   * ws 库会在内部自动完成 TCP 握手和 WebSocket 升级协议（HTTP Upgrade）。
   * 连接是异步建立的，实际连接成功会触发 "open" 事件。
   */
  private connect = (): void => {
    // url 为空时不进行连接，避免 new URL("") 抛出 TypeError
    if (!this.config.url) {
      console.error(`${this.logPrefix} wsUrl 未配置，跳过连接`);
      this.state = "disconnected";
      return;
    }
    // token 为空时不进行连接，避免无效请求
    if (!this.config.token) {
      console.error(`${this.logPrefix} token 为空，跳过 WebSocket 连接`);
      this.state = "disconnected";
      return;
    }

    this.state = "connecting";
    console.error(`${this.logPrefix} 连接配置: url=${this.config.url}, token=${this.config.token.substring(0, 6) + '...'}, guid=${this.config.guid}, userId=${this.config.userId}`);
    const wsUrl = this.buildConnectionUrl();
    console.error(`${this.logPrefix} 正在连接: ${wsUrl}`);

    try {
      // new WebSocket(url) 立即返回，不会阻塞
      // 连接过程在后台异步进行，通过事件通知结果
      this.ws = new WebSocket(wsUrl);
      this.setupEventHandlers();
    } catch (error) {
      // 同步错误（如 URL 格式非法）会在这里捕获
      // 异步连接失败（如服务端拒绝）会触发 "error" 事件
      console.error(`${this.logPrefix} 创建连接失败:`, error);
      this.handleConnectionError(error instanceof Error ? error : new Error(String(error)));
    }
  };

  /**
   * 构建 WebSocket 连接 URL
   * @description
   * 使用 Node.js 内置的 `URL` 类（全局可用，无需 import）构建带查询参数的 URL。
   * `url.searchParams.set()` 会自动对参数值进行 URL 编码（encodeURIComponent），
   * 避免特殊字符导致的 URL 解析问题。
   *
   * 最终格式：ws://host:port/?token={token}
   */
  private buildConnectionUrl = (): string => {
    const url = new URL(this.config.url);
    if (this.config.token) {
      url.searchParams.set("token", this.config.token);
    }
    if (this.config.guid) {
      url.searchParams.set("guid", this.config.guid);
    }
    if (this.config.userId) {
      url.searchParams.set("user_id", this.config.userId);
    }
    return url.toString();
  };

  /**
   * 注册 ws 库的事件监听器
   * @description
   * ws 库使用 Node.js EventEmitter 风格的 `.on(event, handler)` 注册事件，
   * 而非浏览器的 `.addEventListener(event, handler)`。
   * 两者功能相同，但回调参数类型不同：
   *
   * | 事件    | 浏览器原生参数         | ws 库参数                        |
   * |---------|----------------------|----------------------------------|
   * | open    | Event                | 无参数                           |
   * | message | MessageEvent         | (data: RawData, isBinary: bool)  |
   * | close   | CloseEvent           | (code: number, reason: Buffer)   |
   * | error   | Event                | (error: Error)                   |
   * | pong    | 不支持               | 无参数（ws 库特有）               |
   */
  private setupEventHandlers = (): void => {
    if (!this.ws) return;

    this.ws.on("open", this.handleOpen);
    this.ws.on("message", this.handleRawMessage);
    this.ws.on("close", this.handleClose);
    this.ws.on("error", this.handleError);
    // "pong" 是 ws 库特有的事件，当收到服务端的 pong 控制帧时触发
    // 浏览器原生 WebSocket API 不暴露此事件
    this.ws.on("pong", this.handlePong);
  };

  // ============================================
  // 事件处理
  // ============================================

  /**
   * 处理连接建立事件
   * @description
   * ws 库的 "open" 事件在 WebSocket 握手完成后触发，此时可以开始收发消息。
   * 连接成功后：
   * 1. 更新状态为 "connected"
   * 2. 重置重连计数器
   * 3. 重置 pong 时间戳
   * 4. 启动心跳定时器
   * 5. 启动系统唤醒检测
   * 6. 触发 onConnected 回调
   */
  private handleOpen = (): void => {
    console.log(`${this.logPrefix} 连接成功`);
    this.state = "connected";
    this.reconnectAttempts = 0;
    this.lastPongTime = Date.now();
    this.startHeartbeat();
    this.startWakeupDetection();
    this.callbacks.onConnected?.();
  };

  /**
   * 处理收到的原始消息
   * @param data - ws 库的原始消息数据，类型为 `WebSocket.RawData`
   * @description
   * `WebSocket.RawData` 是 ws 库定义的联合类型：`Buffer | ArrayBuffer | Buffer[]`
   * - 文本消息（text frame）：通常是 Buffer 类型
   * - 二进制消息（binary frame）：可能是 Buffer 或 ArrayBuffer
   *
   * 处理步骤：
   * 1. 将 RawData 转为字符串（Buffer.toString() 默认使用 UTF-8 编码）
   * 2. JSON.parse 解析为 AGPEnvelope 对象
   * 3. 检查 msg_id 去重
   * 4. 根据 method 字段分发到对应的回调
   */
  private handleRawMessage = (data: WebSocket.RawData): void => {
    try {
      // Buffer.toString() 默认 UTF-8 编码，等同于 data.toString("utf8")
      // 如果 data 已经是 string 类型（理论上 ws 库不会这样，但做兼容处理）
      const raw = typeof data === "string" ? data : data.toString();
      const envelope = JSON.parse(raw) as AGPEnvelope;

      // 消息去重：同一个 msg_id 只处理一次
      // 网络不稳定时服务端可能重发消息，通过 msg_id 避免重复处理
      if (this.processedMsgIds.has(envelope.msg_id)) {
        console.log(`${this.logPrefix} 重复消息，跳过: ${envelope.msg_id}`);
        return;
      }
      this.processedMsgIds.add(envelope.msg_id);

      console.log(`${this.logPrefix} 收到消息: method=${envelope.method}, msg_id=${envelope.msg_id}`);

      // 根据 method 字段分发消息到对应的业务处理回调
      switch (envelope.method) {
        case "session.prompt":
          // 下行：服务端下发用户指令，需要调用 Agent 处理
          this.callbacks.onPrompt?.(envelope as PromptMessage);
          break;
        case "session.cancel":
          // 下行：服务端要求取消正在处理的 Turn
          this.callbacks.onCancel?.(envelope as CancelMessage);
          break;
        default:
          console.warn(`${this.logPrefix} 未知消息类型: ${envelope.method}`);
      }
    } catch (error) {
      console.error(`${this.logPrefix} 消息解析失败:`, error, '原始数据:', data);
      this.callbacks.onError?.(
        error instanceof Error ? error : new Error(`消息解析失败: ${String(error)}`)
      );
    }
  };

  /**
   * 处理连接关闭事件
   * @param code - WebSocket 关闭状态码（RFC 6455 定义）
   *   常见值：
   *   - 1000: 正常关闭
   *   - 1001: 端点离开（如服务端重启）
   *   - 1006: 异常关闭（连接被强制断开，无关闭握手）
   *   - 1008: 策略违规（如 token 不匹配）
   * @param reason - 关闭原因，ws 库中类型为 `Buffer`，需要调用 `.toString()` 转为字符串
   * @description
   * 注意：ws 库的 close 事件参数与浏览器不同：
   *   - 浏览器：`(event: CloseEvent)` → 通过 event.code 和 event.reason 获取
   *   - ws 库：`(code: number, reason: Buffer)` → 直接获取，reason 是 Buffer 需要转换
   *
   * 只有在非主动关闭（state !== "disconnected"）时才触发重连，
   * 避免调用 stop() 后又自动重连。
   */
  private handleClose = (code: number, reason: Buffer): void => {
    // Buffer.toString() 将 Buffer 转为 UTF-8 字符串
    // 如果 reason 为空 Buffer，toString() 返回空字符串，此时用 code 作为描述
    const reasonStr = reason.toString() || `code=${code}`;
    console.log(`${this.logPrefix} 连接关闭: ${reasonStr}`);
    this.clearHeartbeat();
    this.clearWakeupDetection();
    this.ws = null;

    // 仅在非主动关闭的情况下尝试重连
    // 主动调用 stop() 时会先将 state 设为 "disconnected"，此处就不会触发重连
    if (this.state !== "disconnected") {
      this.callbacks.onDisconnected?.(reasonStr);
      this.scheduleReconnect();
    }
  };

  /**
   * 处理 pong 控制帧
   * @description
   * 当服务端收到我们发送的 ping 帧后，会自动回复一个 pong 帧。
   * ws 库会触发 "pong" 事件通知我们。
   * 记录收到 pong 的时间戳，供心跳定时器检测连接是否假死。
   * 如果长时间未收到 pong，说明连接已不可用（如电脑休眠导致 TCP 断开）。
   */
  private handlePong = (): void => {
    this.lastPongTime = Date.now();
  };

  /**
   * 处理连接错误事件
   * @param error - ws 库直接传递 Error 对象（浏览器原生 API 传递的是 Event 对象）
   * @description
   * ws 库的 "error" 事件在以下情况触发：
   *   - 连接被拒绝（如服务端不可达）
   *   - TLS 握手失败
   *   - 消息发送失败
   * 注意：error 事件之后通常会紧跟 close 事件，重连逻辑在 handleClose 中处理。
   */
  private handleError = (error: Error): void => {
    console.error(`${this.logPrefix} 连接错误:`, error);
    this.callbacks.onError?.(error);
  };

  /**
   * 处理连接创建时的同步错误
   * @description
   * 当 `new WebSocket(url)` 抛出同步异常时调用（如 URL 格式非法）。
   * 此时不会触发 "error" 和 "close" 事件，需要手动触发重连。
   */
  private handleConnectionError = (error: Error): void => {
    this.callbacks.onError?.(error);
    this.scheduleReconnect();
  };

  /**
   * 安排下一次重连
   * @description
   * 使用指数退避（Exponential Backoff）策略计算重连延迟：
   *   delay = min(reconnectInterval × 1.5^(attempts-1), 30000)
   *
   * 例如 reconnectInterval=3000 时：
   *   第 1 次：3000ms
   *   第 2 次：4500ms
   *   第 3 次：6750ms
   *   第 4 次：10125ms
   *   第 5 次：15187ms（之后趋近 30000ms 上限）
   *
   * 指数退避的目的：避免服务端故障时大量客户端同时重连造成雪崩效应。
   *
   * `setTimeout` 是 Node.js 全局函数，在指定延迟后执行一次回调。
   * 返回值是 Timeout 对象（Node.js）或 number（浏览器），
   * 需要保存以便后续调用 clearTimeout 取消。
   */
  private scheduleReconnect = (): void => {
    // 检查是否超过最大重连次数（0 表示无限重连）
    if (
      this.config.maxReconnectAttempts > 0 &&
      this.reconnectAttempts >= this.config.maxReconnectAttempts
    ) {
      console.error(`${this.logPrefix} 已达最大重连次数 (${this.config.maxReconnectAttempts})，停止重连`);
      this.state = "disconnected";
      return;
    }

    this.state = "reconnecting";
    this.reconnectAttempts++;

    // 指数退避：每次重连等待时间递增，最大 25 秒
    const delay = Math.min(
      this.config.reconnectInterval * Math.pow(1.5, this.reconnectAttempts - 1),
      25000
    );

    console.log(`${this.logPrefix} ${delay}ms 后尝试第 ${this.reconnectAttempts} 次重连...`);

    // setTimeout 返回的句柄保存到 reconnectTimer，
    // 以便在 stop() 或成功连接时通过 clearTimeout 取消待执行的重连
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  };

  /**
   * 清除重连定时器
   * @description
   * `clearTimeout` 是 Node.js 全局函数，取消由 setTimeout 创建的定时器。
   * 如果定时器已执行或已被取消，调用 clearTimeout 不会报错（安全操作）。
   */
  private clearReconnectTimer = (): void => {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  };

  // ============================================
  // 心跳保活
  // ============================================

  /**
   * 启动心跳定时器
   * @description
   * 使用 `setInterval` 定期发送 WebSocket ping 控制帧，并检测 pong 超时。
   *
   * `ws.ping()` 发送 WebSocket 协议层的 ping 控制帧（opcode=0x9），
   * 服务端必须自动回复 pong 帧。
   *
   * Pong 超时检测：
   * 如果超过 2 倍心跳间隔仍未收到 pong，判定连接已死（如休眠后 TCP 已断），
   * 主动 terminate 触发 close 事件 → 自动重连。
   *
   * Ping 失败处理：
   * 如果 ping 发送抛异常（底层 socket 已关闭），也主动 terminate 触发重连。
   */
  private startHeartbeat = (): void => {
    this.clearHeartbeat();
    this.heartbeatTimer = setInterval(() => {
      if (this.ws && this.state === "connected") {
        // 检测 pong 超时：超过 2 倍心跳间隔未收到 pong，判定连接已死
        const pongTimeout = this.config.heartbeatInterval * 2;
        if (Date.now() - this.lastPongTime > pongTimeout) {
          console.warn(`${this.logPrefix} pong 超时 (${pongTimeout}ms 未收到)，判定连接已死，主动断开`);
          this.ws.terminate();
          return;
        }

        try {
          // ws.ping() 发送 WebSocket 原生 ping 控制帧
          this.ws.ping();
        } catch {
          console.warn(`${this.logPrefix} 心跳发送失败，主动断开触发重连`);
          this.ws?.terminate();
        }
      }
    }, this.config.heartbeatInterval);
  };

  /**
   * 清除心跳定时器
   * @description
   * `clearInterval` 是 Node.js 全局函数，停止由 setInterval 创建的定时器。
   * 在连接关闭或主动停止时调用，避免向已断开的连接发送 ping。
   */
  private clearHeartbeat = (): void => {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  };

  // ============================================
  // 系统唤醒检测
  // ============================================

  /**
   * 启动系统唤醒检测
   * @description
   * 电脑休眠时 setInterval 会被冻结，唤醒后恢复。
   * 利用「两次 tick 之间实际经过的时间」远大于「setInterval 设定的间隔」来检测唤醒事件。
   *
   * 例如：CHECK_INTERVAL = 5s，但实际两次 tick 间隔了 60s → 说明系统休眠了约 55s。
   * 此时 TCP 连接大概率已被服务端超时关闭，需要主动 terminate 触发重连。
   *
   * 同时重置重连计数器，确保唤醒后有足够的重连机会。
   */
  private startWakeupDetection = (): void => {
    this.clearWakeupDetection();
    this.lastTickTime = Date.now();

    const CHECK_INTERVAL = 5000; // 每 5 秒检查一次
    const WAKEUP_THRESHOLD = 15000; // 实际间隔超过 15 秒视为休眠唤醒

    this.wakeupCheckTimer = setInterval(() => {
      const now = Date.now();
      const elapsed = now - this.lastTickTime;
      this.lastTickTime = now;

      if (elapsed > WAKEUP_THRESHOLD) {
        console.warn(`${this.logPrefix} 检测到系统唤醒 (tick 间隔 ${elapsed}ms，阈值 ${WAKEUP_THRESHOLD}ms)`);
        // 重置重连计数器，给予唤醒后充足的重连机会
        this.reconnectAttempts = 0;
        // 如果当前连接还标记为已连接，主动断开触发重连
        if (this.ws && this.state === "connected") {
          console.warn(`${this.logPrefix} 唤醒后主动断开连接，触发重连`);
          this.ws.terminate();
        }
      }
    }, CHECK_INTERVAL);
  };

  /**
   * 清除系统唤醒检测定时器
   */
  private clearWakeupDetection = (): void => {
    if (this.wakeupCheckTimer) {
      clearInterval(this.wakeupCheckTimer);
      this.wakeupCheckTimer = null;
    }
  };

  // ============================================
  // 消息发送
  // ============================================

  /**
   * 发送 AGP 信封消息（内部通用方法）
   * @param method - AGP 消息类型（如 "session.update"、"session.promptResponse"）
   * @param payload - 消息载荷，泛型 T 由调用方决定具体类型
   * @description
   * 所有上行消息都通过此方法发送，统一处理：
   * 1. 检查连接状态
   * 2. 构建 AGP 信封（添加 msg_id等公共字段）
   * 3. JSON 序列化
   * 4. 调用 ws.send() 发送文本帧
   *
   * `ws.send(data)` 是 ws 库的发送方法：
   *   - 传入 string：发送文本帧（opcode=0x1）
   *   - 传入 Buffer/ArrayBuffer：发送二进制帧（opcode=0x2）
   *   - 这里传入 JSON 字符串，发送文本帧
   *
   * `randomUUID()` 为每条消息生成唯一 ID，服务端可用于去重和追踪。
   */
  private sendEnvelope = <T>(method: AGPMethod, payload: T, guid?: string, userId?: string): void => {
    if (!this.ws || this.state !== "connected") {
      console.warn(`${this.logPrefix} 无法发送消息，当前状态: ${this.state}`);
      return;
    }

    const envelope: AGPEnvelope<T> = {
      msg_id: randomUUID(),
      guid: guid ?? this.config.guid,
      user_id: userId ?? this.config.userId,
      method,
      payload,
    };

    try {
      const data = JSON.stringify(envelope);
      // ws.send() 将字符串作为 WebSocket 文本帧发送
      this.ws.send(data);
      // 截断过长的 JSON 日志，避免日志文件膨胀
      const jsonPreview = data.length > 500 ? data.substring(0, 500) + `...(truncated, total ${data.length} chars)` : data;
      console.log(`${this.logPrefix} 发送消息: method=${method}, msg_id=${envelope.msg_id}, json=${jsonPreview}`);
    } catch (error) {
      console.error(`${this.logPrefix} 消息发送失败:`, error);
      this.callbacks.onError?.(
        error instanceof Error ? error : new Error(`消息发送失败: ${String(error)}`)
      );
    }
  };

  // ============================================
  // 消息 ID 缓存清理
  // ============================================

  /**
   * 启动消息 ID 缓存定期清理任务
   * @description
   * `processedMsgIds` 是一个 Set，会随着消息的接收不断增长。
   * 如果不清理，长时间运行后会占用大量内存（内存泄漏）。
   *
   * 清理策略：
   * - 每 5 分钟检查一次
   * - 当 Set 大小超过 MAX_MSG_ID_CACHE（1000）时触发清理
   * - 清理时保留最新的一半（500 条），丢弃最旧的一半
   *
   * 为什么保留最新的一半而不是全部清空？
   * 因为刚处理过的消息 ID 最有可能被重发，保留它们可以继续防重。
   *
   * `[...this.processedMsgIds]` 将 Set 转为数组，
   * Set 的迭代顺序是插入顺序，所以 slice(-500) 取的是最后插入的 500 条（最新的）。
   */
  private startMsgIdCleanup = (): void => {
    this.clearMsgIdCleanup();
    this.msgIdCleanupTimer = setInterval(() => {
      if (this.processedMsgIds.size > WechatAccessWebSocketClient.MAX_MSG_ID_CACHE) {
        console.log(`${this.logPrefix} 清理消息 ID 缓存: ${this.processedMsgIds.size} → ${WechatAccessWebSocketClient.MAX_MSG_ID_CACHE / 2}`);
        // 将 Set 转为数组（保持插入顺序），取后半部分（最新的），重建 Set
        const entries = [...this.processedMsgIds];
        this.processedMsgIds.clear();
        entries.slice(-WechatAccessWebSocketClient.MAX_MSG_ID_CACHE / 2).forEach((id) => {
          this.processedMsgIds.add(id);
        });
      }
    }, 5 * 60 * 1000); // 每 5 分钟执行一次
  };

  /**
   * 清除消息 ID 缓存清理定时器
   */
  private clearMsgIdCleanup = (): void => {
    if (this.msgIdCleanupTimer) {
      clearInterval(this.msgIdCleanupTimer);
      this.msgIdCleanupTimer = null;
    }
  };
}
