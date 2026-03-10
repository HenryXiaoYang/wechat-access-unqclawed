import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { emptyPluginConfigSchema } from "openclaw/plugin-sdk";
import { WechatAccessWebSocketClient, handlePrompt, handleCancel } from "./websocket/index.js";
// import { handleSimpleWecomWebhook } from "./http/webhook.js";
import { setWecomRuntime, getWecomRuntime } from "./common/runtime.js";
import { performLogin, loadState, clearState, saveState, getDeviceGuid, getEnvironment, QClawAPI, buildAuthUrl, fetchQrUuid, fetchQrImageDataUrl, pollQrStatus } from "./auth/index.js";
import type { QClawEnvironment, PersistedAuthState } from "./auth/index.js";
import { nested } from "./auth/utils.js";

// 类型定义
type NormalizedChatType = "direct" | "group" | "channel";

// WebSocket 客户端实例（按 accountId 存储）
const wsClients = new Map<string, WechatAccessWebSocketClient>();

// QR 扫码登录中间状态（loginWithQrStart 写入，loginWithQrWait 消费）
let pendingQrLogin: {
  state: string;
  uuid: string;
  env: QClawEnvironment;
  guid: string;
  bypassInvite: boolean;
  authStatePath?: string;
} | null = null;

// 渠道元数据
const meta = {
  id: "wechat-access-unqclawed",
  label: "腾讯通路",
  /** 选择时的显示文本 */
  selectionLabel: "腾讯通路",
  detailLabel: "腾讯通路",
  /** 文档路径 */
  docsPath: "/channels/wechat-access",
  docsLabel: "wechat-access-unqclawed",
  /** 简介 */
  blurb: "通用通路",
  /** 图标 */
  systemImage: "message.fill",
  /** 排序权重 */
  order: 85,
};

// 渠道插件
const tencentAccessPlugin = {
  id: "wechat-access-unqclawed",
  meta,

  // 能力声明
  capabilities: {
    chatTypes: ["direct"] as NormalizedChatType[],
    reactions: false,
    threads: false,
    media: true,
    nativeCommands: false,
    blockStreaming: false,
  },

  // 热重载：token 或 wsUrl 变更时触发 gateway 重启
  reload: {
    configPrefixes: ["channels.wechat-access-unqclawed.token", "channels.wechat-access-unqclawed.wsUrl"],
  },

  // 声明支持的 gateway 方法（框架通过此字段找到 login provider）
  gatewayMethods: ["web.login.start", "web.login.wait"],

  // 配置适配器（必需）
  config: {
    listAccountIds: (cfg: any) => {
      const accounts = cfg.channels?.["wechat-access-unqclawed"]?.accounts;
      if (accounts && typeof accounts === "object") {
        return Object.keys(accounts);
      }
      // 没有配置账号时，返回默认账号
      return ["default"];
    },
    resolveAccount: (cfg: any, accountId: string) => {
      const accounts = cfg.channels?.["wechat-access-unqclawed"]?.accounts;
      const account = accounts?.[accountId ?? "default"];
      return account ?? { accountId: accountId ?? "default" };
    },
  },

  // 认证适配器：openclaw channels login --channel wechat-access-unqclawed
  auth: {
    login: async ({ cfg, accountId, runtime }: { cfg: any; accountId?: string; runtime: any; verbose?: boolean; channelInput?: string }) => {
      const channelCfg = cfg?.channels?.["wechat-access-unqclawed"];
      const envName = channelCfg?.environment ? String(channelCfg.environment) : "production";
      const authStatePath = channelCfg?.authStatePath ? String(channelCfg.authStatePath) : undefined;

      const env = getEnvironment(envName);
      const guid = getDeviceGuid();

      // 1. 获取 OAuth state
      runtime.log("[wechat-access] 获取登录 state...");
      const api = new QClawAPI(env, guid);
      const stateResult = await api.getWxLoginState();
      let state = String(Math.floor(Math.random() * 10000));
      if (stateResult.success) {
        const s = nested(stateResult.data, "state") as string | undefined;
        if (s) state = s;
      }

      // 2. 构造 auth URL
      runtime.log("[wechat-access] 生成微信登录二维码...");
      const authUrl = buildAuthUrl(state, env);

      // 3. 终端显示 QR 码
      try {
        const qrterm = await import("qrcode-terminal");
        const generate = qrterm.default?.generate ?? qrterm.generate;
        generate(authUrl, { small: true }, (qrcode: string) => {
          runtime.log("\n" + qrcode);
        });
      } catch {
        runtime.log("(qrcode-terminal 不可用)");
      }
      runtime.log(`\n或在浏览器打开: ${authUrl}\n`);

      // 4. 用临时文件接收 code：用户扫码授权后浏览器跳转，把地址栏 URL 或 code 写到临时文件
      const { join } = await import("node:path");
      const { homedir } = await import("node:os");
      const { readFileSync, unlinkSync, existsSync } = await import("node:fs");
      const codeTmpFile = join(homedir(), ".openclaw", "wechat-auth-code.tmp");

      // 清理上次残留
      try { unlinkSync(codeTmpFile); } catch { /* ignore */ }

      runtime.log("=".repeat(60));
      runtime.log("  扫码并在手机上确认后，浏览器会跳转到新页面。");
      runtime.log("  请复制地址栏的完整 URL 或其中的 code 参数值，");
      runtime.log("  然后在另一个终端窗口执行：");
      runtime.log("");
      runtime.log(`  echo "粘贴的URL或code" > ${codeTmpFile}`);
      runtime.log("");
      runtime.log("  本窗口会自动检测并完成登录。");
      runtime.log("=".repeat(60));

      // 5. 轮询临时文件
      const deadline = Date.now() + 300_000; // 5 分钟超时
      while (Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 1500));

        if (!existsSync(codeTmpFile)) continue;

        let raw = "";
        try {
          raw = readFileSync(codeTmpFile, "utf-8").trim();
          unlinkSync(codeTmpFile); // 读完即删
        } catch { continue; }

        if (!raw) continue;

        // 从 URL 或裸 code 中提取 code
        let code = raw;
        if (raw.includes("code=")) {
          try {
            const url = new URL(raw);
            const c = url.searchParams.get("code");
            if (c) code = c;
          } catch {
            const match = raw.match(/[?&#]code=([^&#]+)/);
            if (match?.[1]) code = match[1];
          }
        }

        if (!code) {
          runtime.log("[wechat-access] 未能从输入中提取 code，请重试");
          continue;
        }

        // 6. 用 code 换 token
        runtime.log(`[wechat-access] 收到 code: ${code.substring(0, 10)}...，正在获取 token...`);
        const loginResult = await api.wxLogin(code, state);
        if (!loginResult.success) {
          throw new Error(`登录失败: ${loginResult.message ?? "未知错误"}`);
        }

        const loginData = loginResult.data as Record<string, unknown>;
        const jwtToken = (loginData.token as string) || "";
        const channelToken = (loginData.openclaw_channel_token as string) || "";
        const userInfo = (loginData.user_info as Record<string, unknown>) || {};

        // 更新 loginKey（服务端可能返回新值，后续 API 调用需要）
        const loginKey = userInfo.loginKey as string | undefined;
        if (loginKey) api.loginKey = loginKey;

        // 创建 API Key（非致命）
        api.jwtToken = jwtToken;
        api.userId = String(userInfo.user_id ?? "");
        let apiKey = "";
        try {
          const keyResult = await api.createApiKey();
          if (keyResult.success) {
            apiKey =
              (nested(keyResult.data, "key") as string) ??
              (nested(keyResult.data, "resp", "data", "key") as string) ??
              "";
          }
        } catch { /* non-fatal */ }

        // 写入 openclaw.json（统一存储）
        try {
          const wRuntime = getWecomRuntime();
          const fullCfg = wRuntime.config.loadConfig();
          const channels = { ...(fullCfg.channels ?? {}) } as Record<string, any>;
          channels["wechat-access-unqclawed"] = {
            ...(channels["wechat-access-unqclawed"] ?? {}),
            token: channelToken,
          };
          const nextCfg: Record<string, unknown> = { ...fullCfg, channels };
          if (apiKey) {
            const models = { ...(fullCfg.models ?? {}) } as Record<string, any>;
            const providers = { ...(models.providers ?? {}) } as Record<string, any>;
            providers.qclaw = { ...(providers.qclaw ?? {}), apiKey };
            models.providers = providers;
            nextCfg.models = models;
          }
          await runtime.config.writeConfigFile(nextCfg);
        } catch { /* non-fatal: fallback to state file */ }

        // 备份到独立文件（兜底）
        saveState({ jwtToken, channelToken, apiKey, guid, userInfo, savedAt: Date.now() }, authStatePath);

        const nickname = (userInfo.nickname as string) ?? "用户";
        runtime.log(`[wechat-access] 登录成功! 欢迎 ${nickname}，token 已保存。请重启 Gateway 生效。`);
        return;
      }
      // 超时清理
      try { unlinkSync(codeTmpFile); } catch { /* ignore */ }
      throw new Error("登录超时（5 分钟），请重试");
    },
  },

  // 出站适配器（必需）
  outbound: {
    deliveryMode: "direct" as const,
    sendText: async () => ({ ok: true }),
  },

  // 状态适配器：上报 WebSocket 连接状态
  status: {
    buildAccountSnapshot: ({ accountId }: { accountId?: string; cfg: any; runtime?: any }) => {
      const client = wsClients.get(accountId ?? "default");
      const running = client?.getState() === "connected";
      return { running };
    },
  },

  // Gateway 适配器：按账号启动/停止 WebSocket 连接
  gateway: {
    startAccount: async (ctx: any) => {
      const { cfg, accountId, abortSignal, log } = ctx;

      const tencentAccessConfig = cfg?.channels?.["wechat-access-unqclawed"];
      let token = tencentAccessConfig?.token ? String(tencentAccessConfig.token) : "";
      const configWsUrl = tencentAccessConfig?.wsUrl ? String(tencentAccessConfig.wsUrl) : "";
      const bypassInvite = tencentAccessConfig?.bypassInvite === true;
      const authStatePath = tencentAccessConfig?.authStatePath
        ? String(tencentAccessConfig.authStatePath)
        : undefined;
      const envName: string = tencentAccessConfig?.environment
        ? String(tencentAccessConfig.environment)
        : "production";
      const gatewayPort = cfg?.gateway?.port ? String(cfg.gateway.port) : "unknown";

      const env = getEnvironment(envName);
      const guid = getDeviceGuid();
      const wsUrl = configWsUrl || env.wechatWsUrl;

      // 启动诊断日志
      log?.info(`[wechat-access] 启动账号 ${accountId}`, {
        platform: process.platform,
        nodeVersion: process.version,
        hasToken: !!token,
        hasUrl: !!wsUrl,
        url: wsUrl || "(未配置)",
        tokenPrefix: token ? token.substring(0, 6) + "..." : "(未配置)",
      });

      // Token 获取策略：配置 > 已保存的登录态 > 提示用户手动登录
      if (!token) {
        const savedState = loadState(authStatePath);
        if (savedState?.channelToken) {
          token = savedState.channelToken;
          log?.info(`[wechat-access] 使用已保存的 token: ${token.substring(0, 6)}...`);
        } else {
          log?.warn(`[wechat-access] 未找到 token，请运行 "openclaw channels login --channel wechat-access-unqclawed" 完成扫码登录，然后重启 Gateway`);
          return;
        }
      }

      if (!token) {
        log?.warn(`[wechat-access] token 为空，跳过 WebSocket 连接`);
        return;
      }

      const wsConfig = {
        url: wsUrl,
        token,
        guid,
        userId: "",
        gatewayPort,
        reconnectInterval: 3000,
        maxReconnectAttempts: 10,
        heartbeatInterval: 20000,
      };

      const client = new WechatAccessWebSocketClient(wsConfig, {
        onConnected: () => {
          log?.info(`[wechat-access] WebSocket 连接成功`);
          ctx.setStatus({ running: true });
        },
        onDisconnected: (reason?: string) => {
          log?.warn(`[wechat-access] WebSocket 连接断开: ${reason}`);
          ctx.setStatus({ running: false });
        },
        onPrompt: (message: any) => {
          void handlePrompt(message, client).catch((err: Error) => {
            log?.error(`[wechat-access] 处理 prompt 失败: ${err.message}`);
          });
        },
        onCancel: (message: any) => {
          handleCancel(message, client);
        },
        onError: (error: Error) => {
          log?.error(`[wechat-access] WebSocket 错误: ${error.message}`);
        },
      });

      wsClients.set(accountId, client);
      client.start();

      // 等待框架发出停止信号
      await new Promise<void>((resolve) => {
        abortSignal.addEventListener("abort", () => {
          log?.info(`[wechat-access] 停止账号 ${accountId}`);
          // 始终停止当前闭包捕获的 client，避免多次 startAccount 时
          // wsClients 被新 client 覆盖后，旧 client 的 stop() 永远不被调用，导致无限重连
          client.stop();
          // 仅当 wsClients 中存的还是当前 client 时才删除，避免误删新 client
          if (wsClients.get(accountId) === client) {
            wsClients.delete(accountId);
            ctx.setStatus({ running: false });
          }
          resolve();
        });
      });
    },

    stopAccount: async (ctx: any) => {
      const { accountId, log } = ctx;
      log?.info(`[wechat-access] stopAccount 钩子触发，停止账号 ${accountId}`);
      const client = wsClients.get(accountId);
      if (client) {
        client.stop();
        wsClients.delete(accountId);
        ctx.setStatus({ running: false });
        log?.info(`[wechat-access] 账号 ${accountId} 已停止`);
      } else {
        log?.warn(`[wechat-access] stopAccount: 未找到账号 ${accountId} 的客户端`);
      }
    },

    // QR 扫码登录：生成二维码（openclaw channels login 调用）
    loginWithQrStart: async (_params: { accountId?: string; force?: boolean; timeoutMs?: number; verbose?: boolean }) => {
      try {
        const runtime = getWecomRuntime();
        const cfg = runtime.config.loadConfig();
        const channelCfg = cfg?.channels?.["wechat-access-unqclawed"];

        const envName = channelCfg?.environment ? String(channelCfg.environment) : "production";
        const bypassInvite = channelCfg?.bypassInvite === true;
        const authStatePath = channelCfg?.authStatePath ? String(channelCfg.authStatePath) : undefined;

        const env = getEnvironment(envName);
        const guid = getDeviceGuid();

        // 1. 获取 OAuth state
        const api = new QClawAPI(env, guid);
        const stateResult = await api.getWxLoginState();
        let state = String(Math.floor(Math.random() * 10000));
        if (stateResult.success) {
          const s = nested(stateResult.data, "state") as string | undefined;
          if (s) state = s;
        }

        // 2. 构造 auth URL → 抓取 QR 页面拿 uuid
        const authUrl = buildAuthUrl(state, env);
        const uuid = await fetchQrUuid(authUrl);

        // 3. 拿 QR 图片转 base64 data URL
        const qrDataUrl = await fetchQrImageDataUrl(uuid);

        // 4. 存中间状态给 loginWithQrWait 用
        pendingQrLogin = { state, uuid, env, guid, bypassInvite, authStatePath };

        return { qrDataUrl, message: "请用微信扫描二维码登录" };
      } catch (err) {
        return { message: `登录初始化失败: ${err instanceof Error ? err.message : String(err)}` };
      }
    },

    // QR 扫码登录：轮询扫码状态（openclaw channels login 循环调用）
    loginWithQrWait: async (_params: { accountId?: string; timeoutMs?: number }) => {
      if (!pendingQrLogin) {
        return { connected: false, message: "请先执行 loginWithQrStart" };
      }

      try {
        const result = await pollQrStatus(pendingQrLogin.uuid);

        if (result.status === "waiting") {
          return { connected: false, message: "等待扫码..." };
        }

        if (result.status === "scanned") {
          return { connected: false, message: "已扫码，请在手机上确认..." };
        }

        if (result.status === "expired") {
          pendingQrLogin = null;
          return { connected: false, message: "二维码已过期，请重新执行 openclaw channels login" };
        }

        if (result.status === "confirmed" && result.code) {
          const { state, env, guid, authStatePath } = pendingQrLogin;
          const api = new QClawAPI(env, guid);

          // 用 code 换 token
          const loginResult = await api.wxLogin(result.code, state);
          if (!loginResult.success) {
            pendingQrLogin = null;
            return { connected: false, message: `登录失败: ${loginResult.message ?? "未知错误"}` };
          }

          const loginData = loginResult.data as Record<string, unknown>;
          const jwtToken = (loginData.token as string) || "";
          const channelToken = (loginData.openclaw_channel_token as string) || "";
          const userInfo = (loginData.user_info as Record<string, unknown>) || {};

          // 更新 loginKey（服务端可能返回新值，后续 API 调用需要）
          const loginKey = userInfo.loginKey as string | undefined;
          if (loginKey) api.loginKey = loginKey;

          // 创建 API Key（非致命）
          api.jwtToken = jwtToken;
          api.userId = String(userInfo.user_id ?? "");
          let apiKey = "";
          try {
            const keyResult = await api.createApiKey();
            if (keyResult.success) {
              apiKey =
                (nested(keyResult.data, "key") as string) ??
                (nested(keyResult.data, "resp", "data", "key") as string) ??
                "";
            }
          } catch { /* non-fatal */ }

          // 写入 openclaw.json（统一存储）
          try {
            const wRuntime = getWecomRuntime();
            const fullCfg = wRuntime.config.loadConfig();
            const channels = { ...(fullCfg.channels ?? {}) } as Record<string, any>;
            channels["wechat-access-unqclawed"] = {
              ...(channels["wechat-access-unqclawed"] ?? {}),
              token: channelToken,
            };
            const nextCfg: Record<string, unknown> = { ...fullCfg, channels };
            if (apiKey) {
              const models = { ...(fullCfg.models ?? {}) } as Record<string, any>;
              const providers = { ...(models.providers ?? {}) } as Record<string, any>;
              providers.qclaw = { ...(providers.qclaw ?? {}), apiKey };
              models.providers = providers;
              nextCfg.models = models;
            }
            await wRuntime.config.writeConfigFile(nextCfg);
          } catch { /* non-fatal */ }

          // 备份到独立文件（兜底）
          saveState({ jwtToken, channelToken, apiKey, guid, userInfo, savedAt: Date.now() }, authStatePath);

          pendingQrLogin = null;
          const nickname = (userInfo.nickname as string) ?? "用户";
          return { connected: true, message: `登录成功! 欢迎 ${nickname}，请重启 Gateway 生效。` };
        }

        // error 或其他未知状态
        return { connected: false, message: "等待扫码..." };
      } catch (err) {
        return { connected: false, message: `轮询失败: ${err instanceof Error ? err.message : String(err)}` };
      }
    },
  },
};

const index = {
  id: "wechat-access-unqclawed",
  name: "通用通路插件",
  description: "腾讯通用通路插件",
  configSchema: emptyPluginConfigSchema(),

  /**
   * 插件注册入口点
   */
  register(api: OpenClawPluginApi) {
    // 1. 设置运行时环境
    setWecomRuntime(api.runtime);

    // 2. 注册渠道插件
    api.registerChannel({ plugin: tencentAccessPlugin as any });

    // 3. 注册 CLI 命令（终端交互式登录/登出）
    api.registerCli(
      ({ program, config }) => {
        const wechat = program.command("wechat").description("微信通路登录管理");

        wechat
          .command("login")
          .description("微信扫码登录，获取 channel token")
          .action(async () => {
            const channelCfg = config?.channels?.["wechat-access-unqclawed"];
            const bypassInvite = channelCfg?.bypassInvite === true;
            const authStatePath = channelCfg?.authStatePath
              ? String(channelCfg.authStatePath)
              : undefined;
            const envName = channelCfg?.environment
              ? String(channelCfg.environment)
              : "production";

            const env = getEnvironment(envName);
            const guid = getDeviceGuid();

            try {
              const credentials = await performLogin({
                guid,
                env,
                bypassInvite,
                authStatePath,
              });
              console.log(`\n登录成功! token: ${credentials.channelToken.substring(0, 6)}...`);
              console.log("token 已保存，请运行 openclaw gateway restart 生效。");
            } catch (err) {
              console.error(`\n登录失败: ${err instanceof Error ? err.message : String(err)}`);
              process.exit(1);
            }
          });

        wechat
          .command("logout")
          .description("清除已保存的微信登录态")
          .action(() => {
            const channelCfg = config?.channels?.["wechat-access-unqclawed"];
            const authStatePath = channelCfg?.authStatePath
              ? String(channelCfg.authStatePath)
              : undefined;
            clearState(authStatePath);
            console.log("已清除登录态，下次启动将需要重新扫码登录。");
          });
      },
      { commands: ["wechat"] },
    );

    // 4. 注册 /wechat-login 命令（聊天渠道内触发）
    api.registerCommand?.({
      name: "wechat-login",
      description: "手动执行微信扫码登录，获取 channel token",
      handler: async ({ config }) => {
        const channelCfg = config?.channels?.["wechat-access-unqclawed"];
        const bypassInvite = channelCfg?.bypassInvite === true;
        const authStatePath = channelCfg?.authStatePath
          ? String(channelCfg.authStatePath)
          : undefined;
        const envName = channelCfg?.environment
          ? String(channelCfg.environment)
          : "production";

        const env = getEnvironment(envName);
        const guid = getDeviceGuid();

        try {
          const credentials = await performLogin({
            guid,
            env,
            bypassInvite,
            authStatePath,
          });
          return { text: `登录成功! token: ${credentials.channelToken.substring(0, 6)}... (已保存，重启 Gateway 生效)` };
        } catch (err) {
          return { text: `登录失败: ${err instanceof Error ? err.message : String(err)}`, isError: true };
        }
      },
    });

    // 5. 注册 /wechat-logout 命令（聊天渠道内触发）
    api.registerCommand?.({
      name: "wechat-logout",
      description: "清除已保存的微信登录态",
      handler: async ({ config }) => {
        const channelCfg = config?.channels?.["wechat-access-unqclawed"];
        const authStatePath = channelCfg?.authStatePath
          ? String(channelCfg.authStatePath)
          : undefined;
        clearState(authStatePath);
        return { text: "已清除登录态，下次启动将重新扫码登录。" };
      },
    });

    console.log("[wechat-access] 腾讯通路插件已注册");
  },
};

export default index;