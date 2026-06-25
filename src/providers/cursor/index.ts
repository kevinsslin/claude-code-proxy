import type { AnthropicRequest } from "../../anthropic/schema.ts";
import { wantsDownstreamStream } from "../../anthropic/stream.ts";
import { jsonError, jsonResponse, sseResponse } from "../../anthropic/response.ts";
import { logVerbose } from "../../config.ts";
import type { Logger } from "../../log.ts";
import { computeBackoffDelay, MAX_RATE_LIMIT_RETRIES, sleep, type BackoffOutcome } from "../retry.ts";
import type { Provider, RequestContext } from "../types.ts";
import {
  CursorError,
  isRetryableCursorNetworkResourceError,
  runCursorAgent,
  type CursorRunOptions,
} from "./client.ts";
import { countCursorTokens } from "./count-tokens.ts";
import {
  cursorAuthLocation,
  expiredAuthMessage,
  loadCursorAuth,
  missingAuthMessage,
} from "./auth/token-store.ts";
import { cursorSelectedImages, renderCursorPrompt } from "./translate/request.ts";
import { CURSOR_SUPPORTED_MODELS, resolveCursorModel } from "./translate/model.ts";
import {
  accumulateCursorResponse,
  translateCursorStream,
} from "./translate/response.ts";
import {
  cursorConversationForRequest,
  recordCursorConversation,
} from "./session.ts";
import {
  canBridgeCursorBashTool,
  canBridgeCursorNativeTools,
  canBridgeCursorReadTool,
  canBridgeCursorWriteTool,
  createCursorShellToolBridge,
  denyCursorBashTool,
  denyCursorReadTool,
  denyCursorWriteTool,
  resumeCursorShellToolBridge,
} from "./tool-bridge.ts";
import type { CursorAuth } from "./auth/token-store.ts";
import type { CursorProto } from "./proto-loader.ts";
import { cursorCli } from "./cli.ts";

const AUTH_EXPIRY_SKEW_MS = 60_000;

export interface CursorProviderDeps {
  loadAuth: () => Promise<CursorAuth | undefined>;
  runAgent: (opts: CursorRunOptions) => Promise<ReadableStream<Uint8Array>>;
  proto?: CursorProto;
  bridgeOutputIdleTimeoutMs?: number;
  computeRetryDelay?: (attempt: number) => BackoffOutcome;
}

const defaultDeps: CursorProviderDeps = {
  loadAuth: () => loadCursorAuth(),
  runAgent: runCursorAgent,
};

async function handleCountTokens(body: AnthropicRequest, ctx: RequestContext): Promise<Response> {
  const tokens = countCursorTokens(body);
  ctx.childLogger("provider.cursor").debug("count_tokens", { tokens });
  return jsonResponse({ input_tokens: tokens });
}

async function handleMessages(
  body: AnthropicRequest,
  ctx: RequestContext,
  deps: CursorProviderDeps,
): Promise<Response> {
  const log = ctx.childLogger("provider.cursor");
  const messageId = `msg_${crypto.randomUUID().replace(/-/g, "")}`;
  const resumed = resumeCursorShellToolBridge(body, ctx, messageId);
  if (resumed) return resumed;

  const selection = resolveCursorModel(body);
  const prompt = renderCursorPrompt(body);
  const inputTokens = countCursorTokens(body);
  const selectedImages = cursorSelectedImages(body);
  const wantStream = wantsDownstreamStream(body);
  const conversationId = cursorConversationForRequest(body, ctx.sessionId);
  const allowedToolNames = new Set((body.tools ?? []).map((tool) => tool.name));

  log.debug("cursor request", {
    requestedModel: body.model,
    resolvedModel: selection.requestedModel,
    mode: selection.mode,
    conversationId,
    stream: wantStream,
    messageCount: body.messages.length,
    promptChars: prompt.length,
    selectedImageCount: selectedImages.length,
  });
  if (logVerbose()) log.debug("cursor prompt", { prompt });

  const auth = await deps.loadAuth();
  if (!auth) return jsonError(401, "authentication_error", missingAuthMessage());
  if (auth.expires && auth.expires <= Date.now() + AUTH_EXPIRY_SKEW_MS) {
    return jsonError(401, "authentication_error", expiredAuthMessage(auth));
  }

  const onSession = (cursorSessionId: string) => {
    recordCursorConversation(ctx.sessionId, cursorSessionId);
    log.debug("cursor session observed", { cursorSessionId });
  };
  const nativeToolBridge = wantStream && ctx.sessionId && canBridgeCursorNativeTools(body, ctx)
    ? createCursorShellToolBridge({
      sessionId: ctx.sessionId,
      messageId,
      model: body.model,
      inputTokens,
      log: ctx.childLogger("cursor.bridge"),
      traffic: ctx.traffic,
      proto: deps.proto,
      onSession,
      allowedToolNames,
      outputIdleTimeoutMs: deps.bridgeOutputIdleTimeoutMs,
    })
    : undefined;
  const bridgeRead = canBridgeCursorReadTool(body);
  const bridgeBash = canBridgeCursorBashTool(body);
  const bridgeWrite = canBridgeCursorWriteTool(body);
  const denyUnadvertisedNativeTools = Boolean(wantStream && ctx.sessionId);

  const runOptions: CursorRunOptions = {
    prompt,
    mode: selection.mode,
    conversationId,
    model: selection.requestedModel,
    selectedImages,
    auth,
    ctx,
    readHandler: bridgeRead
      ? nativeToolBridge?.readHandler
      : denyUnadvertisedNativeTools
      ? denyCursorReadTool
      : undefined,
    shellStreamHandler: bridgeBash
      ? nativeToolBridge?.shellStreamHandler
      : denyUnadvertisedNativeTools
      ? denyCursorBashTool
      : undefined,
    writeHandler: bridgeWrite
      ? nativeToolBridge?.writeHandler
      : denyUnadvertisedNativeTools
      ? denyCursorWriteTool
      : undefined,
  };
  const runAgent = () => deps.runAgent(runOptions);
  const retryRunAgent = () =>
    retryCursorNetworkResourceError(runAgent, {
      log,
      signal: ctx.signal,
      computeRetryDelay: deps.computeRetryDelay,
    });

  let upstream: ReadableStream<Uint8Array>;
  try {
    upstream = await retryRunAgent();
  } catch (err) {
    if (err instanceof CursorError) {
      log.warn("cursor upstream error", {
        status: err.status,
        message: err.message,
        detail: err.detail,
      });
      const type = err.status === 401 || err.status === 403 ? "authentication_error" : "api_error";
      return jsonError(err.status, type, err.detail || err.message);
    }
    throw err;
  }

  if (wantStream) {
    const stream = nativeToolBridge?.stream(upstream, ctx.signal, {
      retryUpstream: retryRunAgent,
      computeRetryDelay: deps.computeRetryDelay,
    }) ?? translateCursorStream(upstream, {
      messageId,
      model: body.model,
      log: ctx.childLogger("cursor.stream"),
      signal: ctx.signal,
      traffic: ctx.traffic,
      proto: deps.proto,
      onSession,
      allowedToolNames,
      inputTokens,
      retryUpstream: retryRunAgent,
      computeRetryDelay: deps.computeRetryDelay,
    });
    return sseResponse(stream);
  }

  try {
    const accumulateLog = ctx.childLogger("cursor.accumulate");
    let nextUpstream: ReadableStream<Uint8Array> | undefined = upstream;
    const result = await retryCursorNetworkResourceError(
      async () => {
        const currentUpstream = nextUpstream ?? await runAgent();
        nextUpstream = undefined;
        return accumulateCursorResponse(currentUpstream, {
          messageId,
          model: body.model,
          log: accumulateLog,
          traffic: ctx.traffic,
          proto: deps.proto,
          onSession,
          allowedToolNames,
          inputTokens,
        });
      },
      {
        log: accumulateLog,
        signal: ctx.signal,
        computeRetryDelay: deps.computeRetryDelay,
      },
    );
    return jsonResponse(result.response);
  } catch (err) {
    log.warn("cursor accumulate error", { err: String(err) });
    if (err instanceof CursorError) {
      const type = err.status === 401 || err.status === 403 ? "authentication_error" : "api_error";
      return jsonError(err.status, type, err.detail || err.message);
    }
    return jsonError(502, "api_error", String(err));
  }
}

export function createCursorProvider(deps: CursorProviderDeps = defaultDeps): Provider {
  return {
    name: "cursor",
    supportedModels: CURSOR_SUPPORTED_MODELS,
    handleMessages: (body, ctx) => handleMessages(body, ctx, deps),
    handleCountTokens,
    cli: cursorCli,
  };
}

export const cursorProvider: Provider = createCursorProvider();

async function retryCursorNetworkResourceError<T>(
  run: () => Promise<T>,
  opts: {
    log: Logger;
    signal?: AbortSignal;
    computeRetryDelay?: (attempt: number) => BackoffOutcome;
  },
): Promise<T> {
  const retryDelay = opts.computeRetryDelay ?? computeBackoffDelay;
  for (let attempt = 0; ; attempt++) {
    try {
      return await run();
    } catch (err) {
      if (!isRetryableCursorNetworkResourceError(err) || attempt >= MAX_RATE_LIMIT_RETRIES) {
        throw err;
      }
      const { waitMs, exceedsBudget } = retryDelay(attempt);
      if (exceedsBudget) {
        opts.log.warn("cursor resource_exhausted network retry delay exceeds budget; giving up", {
          maxDelayMs: waitMs,
          err: String(err),
        });
        throw err;
      }
      opts.log.warn("cursor resource_exhausted network error, retrying", {
        attempt: attempt + 1,
        maxRetries: MAX_RATE_LIMIT_RETRIES,
        waitMs,
        err: String(err),
      });
      await sleep(waitMs, opts.signal);
    }
  }
}
