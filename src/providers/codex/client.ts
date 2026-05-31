import { CODEX_API_ENDPOINT, ORIGINATOR as ORIGINATOR_DEFAULT } from "./auth/constants.ts";
import { codexBaseUrl, codexOriginator, codexUserAgent } from "../../config.ts";
declare const BUILD_VERSION: string | undefined;
const PROXY_VERSION = typeof BUILD_VERSION === "string" ? BUILD_VERSION : "dev";
import { forceRefresh, getAuth } from "./auth/manager.ts";
import type { Logger } from "../../log.ts";
import type { RequestContext } from "../types.ts";
import type { ResponsesRequest } from "./translate/request.ts";
import { retryOn429, sleep } from "../retry.ts";
import { summarizeCodexRequestSize } from "./request-summary.ts";

const FETCH_WATCHDOG_INTERVAL_MS = 30_000;
let fetchHeaderTimeoutMs = 60_000;
let fetchHeaderTimeoutRetries = 1;

export function setCodexHeaderTimeoutForTests(timeoutMs: number, retries: number): void {
  fetchHeaderTimeoutMs = timeoutMs;
  fetchHeaderTimeoutRetries = retries;
}

export interface CodexResponse {
  body: ReadableStream<Uint8Array>;
  status: number;
  headers: Headers;
}

export async function postCodex(
  body: ResponsesRequest,
  ctx: RequestContext,
): Promise<CodexResponse> {
  const log = ctx.childLogger("codex.client");
  return retryHeaderTimeouts(
    () =>
      retryOn429(() => attemptPostCodex(body, ctx, log), {
        log,
        signal: ctx.signal,
        classify: (err) =>
          err instanceof CodexError && err.status === 429
            ? { retryAfter: err.meta?.retryAfter }
            : undefined,
      }),
    log,
    ctx.signal,
    body,
  );
}

async function retryHeaderTimeouts(
  run: () => Promise<CodexResponse>,
  log: Logger,
  signal: AbortSignal | undefined,
  body: ResponsesRequest,
): Promise<CodexResponse> {
  for (let attempt = 0; ; attempt++) {
    try {
      return await run();
    } catch (err) {
      if (!(err instanceof CodexHeaderTimeoutError) || attempt >= fetchHeaderTimeoutRetries)
        throw err;
      const waitMs = fetchHeaderTimeoutMs <= 10 ? 0 : 500 + Math.round(Math.random() * 1000);
      log.warn("codex response headers timed out, retrying", {
        attempt: attempt + 1,
        maxRetries: fetchHeaderTimeoutRetries,
        waitMs,
        timeoutMs: fetchHeaderTimeoutMs,
        model: body.model,
        inputCount: body.input.length,
        toolCount: body.tools?.length ?? 0,
        requestSize: summarizeCodexRequestSize(body),
      });
      await sleep(waitMs, signal);
    }
  }
}

async function attemptPostCodex(
  body: ResponsesRequest,
  ctx: RequestContext,
  log: Logger,
): Promise<CodexResponse> {
  let auth = await getAuth();
  let resp = await doFetch(auth.access, auth.accountId, body, log, ctx.signal, ctx.sessionId);

  if (resp.status === 401) {
    log.warn("got 401, refreshing token", {});
    try {
      auth = await forceRefresh();
      resp = await doFetch(auth.access, auth.accountId, body, log, ctx.signal, ctx.sessionId);
    } catch (err) {
      log.error("refresh after 401 failed", { err: String(err) });
    }
  }

  if (resp.status === 403) {
    const text = await safeText(resp);
    log.error("403 from upstream (non-refreshable)", { body: text });
    throw new CodexError(403, "Forbidden", text);
  }

  if (resp.status === 429) {
    const retryAfter = resp.headers.get("retry-after") || undefined;
    const text = await safeText(resp);
    throw new CodexError(429, "Rate limited", text, { retryAfter });
  }

  if (!resp.ok) {
    const text = await safeText(resp);
    throw new CodexError(resp.status, "Upstream error", text);
  }

  if (!resp.body) throw new CodexError(500, "Upstream returned no body");

  return { body: resp.body, status: resp.status, headers: resp.headers };
}

async function doFetch(
  accessToken: string,
  accountId: string | undefined,
  body: ResponsesRequest,
  log: Logger,
  signal?: AbortSignal,
  sessionId?: string,
): Promise<Response> {
  const headers = new Headers({
    "Content-Type": "application/json",
    accept: "text/event-stream",
    authorization: `Bearer ${accessToken}`,
    originator: codexOriginator(ORIGINATOR_DEFAULT),
    "openai-beta": "responses=experimental",
  });
  const userAgent = codexUserAgent(`claude-code-proxy/${PROXY_VERSION}`);
  if (userAgent) headers.set("User-Agent", userAgent);
  if (accountId) headers.set("ChatGPT-Account-Id", accountId);
  if (sessionId) {
    headers.set("session_id", sessionId);
    headers.set("x-client-request-id", sessionId);
    headers.set("x-codex-window-id", `${sessionId}:0`);
  }

  const codexUrl = codexBaseUrl(CODEX_API_ENDPOINT);

  const bodyJson = JSON.stringify(body);
  const size = summarizeCodexRequestSize(body, bodyJson);

  log.debug("posting to codex", {
    url: codexUrl,
    model: body.model,
    inputCount: body.input.length,
    toolCount: body.tools?.length ?? 0,
    serviceTier: body.service_tier,
    reasoningEffort: body.reasoning?.effort,
    promptCacheKey: body.prompt_cache_key,
    size,
  });

  const startedAt = Date.now();
  const watchdog = setInterval(() => {
    log.info("waiting for codex response headers", {
      elapsedMs: Date.now() - startedAt,
      model: body.model,
      inputCount: body.input.length,
      toolCount: body.tools?.length ?? 0,
    });
  }, FETCH_WATCHDOG_INTERVAL_MS);
  const headerTimeout = new AbortController();
  const timeout = setTimeout(() => {
    headerTimeout.abort(new CodexHeaderTimeoutError(fetchHeaderTimeoutMs));
  }, fetchHeaderTimeoutMs);
  const onAbort = () => headerTimeout.abort(signal?.reason);
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const resp = await fetch(codexUrl, {
      method: "POST",
      headers,
      body: bodyJson,
      signal: headerTimeout.signal,
    });
    log.debug("received codex response headers", {
      status: resp.status,
      elapsedMs: Date.now() - startedAt,
    });
    return resp;
  } catch (err) {
    if (headerTimeout.signal.reason instanceof CodexHeaderTimeoutError) {
      throw headerTimeout.signal.reason;
    }
    throw err;
  } finally {
    signal?.removeEventListener("abort", onAbort);
    clearTimeout(timeout);
    clearInterval(watchdog);
  }
}

async function safeText(resp: Response): Promise<string> {
  try {
    return await resp.text();
  } catch {
    return "";
  }
}

export class CodexHeaderTimeoutError extends Error {
  constructor(public timeoutMs: number) {
    super(`Timed out waiting ${timeoutMs}ms for Codex response headers`);
    this.name = "CodexHeaderTimeoutError";
  }
}

export class CodexError extends Error {
  constructor(
    public status: number,
    message: string,
    public detail?: string,
    public meta?: { retryAfter?: string },
  ) {
    super(message);
    this.name = "CodexError";
  }
}
