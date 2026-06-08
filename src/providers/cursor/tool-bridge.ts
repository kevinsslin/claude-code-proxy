import { encodeSseEvent } from "../../sse.ts";
import type { AnthropicRequest, AnthropicToolResultBlock } from "../../anthropic/schema.ts";
import type { Logger } from "../../log.ts";
import type { RequestContext } from "../types.ts";
import {
  appendCursorReadResult,
  appendCursorShellStreamResult,
  appendCursorWriteResult,
  cursorReadArgs,
  cursorShellStreamArgs,
  cursorWriteArgs,
  decodeCursorStream,
  type CursorAppendMessage,
  type CursorReadExec,
  type CursorShellStreamExec,
  type CursorStreamEvent,
  type CursorUsage,
  type CursorWriteExec,
} from "./client.ts";
import type { CursorProto } from "./proto-loader.ts";
import { cursorUsageToAnthropic } from "./translate/response.ts";

interface PendingToolBase {
  toolUseId: string;
  startedAt: number;
  append: CursorAppendMessage;
  resolve(result: NativeToolResult): void;
  result: Promise<NativeToolResult>;
}

interface PendingReadTool extends PendingToolBase {
  kind: "Read";
  exec: CursorReadExec;
  path: string;
}

interface PendingShellTool extends PendingToolBase {
  kind: "Bash";
  exec: CursorShellStreamExec;
  command: string;
  workingDirectory: string;
  timeoutMs: number;
}

interface PendingWriteTool extends PendingToolBase {
  kind: "Write";
  exec: CursorWriteExec;
  path: string;
  content: string;
}

type PendingNativeTool = PendingReadTool | PendingShellTool | PendingWriteTool;

interface NativeToolResult {
  content: string;
  isError: boolean;
}

interface CursorBridgeState {
  sessionId: string;
  messageId: string;
  model: string;
  iterator: AsyncGenerator<CursorStreamEvent>;
  pendingNext?: Promise<IteratorResult<CursorStreamEvent>>;
  pendingTool?: PendingNativeTool;
  waiters: Array<(tool: PendingNativeTool) => void>;
  log: Logger;
  traffic?: RequestContext["traffic"];
  onSession?: (sessionId: string) => void;
}

const bridgeStates = new Map<string, CursorBridgeState>();

export function canBridgeCursorNativeTools(body: AnthropicRequest, ctx: RequestContext): boolean {
  return Boolean(
    ctx.sessionId && body.stream && body.tools?.some((tool) =>
      tool.name === "Read" || tool.name === "Bash" || tool.name === "Write"
    ),
  );
}

export function canBridgeCursorReadTool(body: AnthropicRequest): boolean {
  return Boolean(body.tools?.some((tool) => tool.name === "Read"));
}

export function canBridgeCursorBashTool(body: AnthropicRequest): boolean {
  return Boolean(body.tools?.some((tool) => tool.name === "Bash"));
}

export function canBridgeCursorWriteTool(body: AnthropicRequest): boolean {
  return Boolean(body.tools?.some((tool) => tool.name === "Write"));
}

export async function denyCursorReadTool(exec: CursorReadExec, append: CursorAppendMessage): Promise<void> {
  await appendCursorReadResult(
    exec,
    {
      success: false,
      error: "Cursor requested Read, but Claude did not advertise the Read tool",
    },
    append,
  );
}

export async function denyCursorBashTool(exec: CursorShellStreamExec, append: CursorAppendMessage): Promise<void> {
  await appendCursorShellStreamResult(
    exec,
    {
      stderr: "Cursor requested Bash, but Claude did not advertise the Bash tool",
      exitCode: 1,
      cwd: cursorShellStreamArgs(exec).workingDirectory,
      localExecutionTimeMs: 0,
    },
    append,
  );
}

export async function denyCursorWriteTool(exec: CursorWriteExec, append: CursorAppendMessage): Promise<void> {
  await appendCursorWriteResult(
    exec,
    {
      success: false,
      error: "Cursor requested Write, but Claude did not advertise the Write tool",
    },
    append,
  );
}

export function createCursorShellToolBridge(opts: {
  sessionId: string;
  messageId: string;
  model: string;
  log: Logger;
  traffic?: RequestContext["traffic"];
  proto?: CursorProto;
  onSession?: (sessionId: string) => void;
}): {
  readHandler: (exec: CursorReadExec, append: CursorAppendMessage) => Promise<void>;
  shellStreamHandler: (exec: CursorShellStreamExec, append: CursorAppendMessage) => Promise<void>;
  writeHandler: (exec: CursorWriteExec, append: CursorAppendMessage) => Promise<void>;
  stream: (upstream: ReadableStream<Uint8Array>, signal?: AbortSignal) => ReadableStream<Uint8Array>;
} {
  const state: CursorBridgeState = {
    sessionId: opts.sessionId,
    messageId: opts.messageId,
    model: opts.model,
    iterator: undefined as unknown as AsyncGenerator<CursorStreamEvent>,
    waiters: [],
    log: opts.log,
    traffic: opts.traffic,
    onSession: opts.onSession,
  };

  const notifyTool = (tool: PendingNativeTool) => {
    state.pendingTool = tool;
    for (const waiter of state.waiters.splice(0)) waiter(tool);
  };

  return {
    async readHandler(exec, append) {
      const { path } = cursorReadArgs(exec);
      const toolUseId = `call_cursor_${crypto.randomUUID().replace(/-/g, "")}`;
      let resolve!: (result: NativeToolResult) => void;
      const result = new Promise<NativeToolResult>((r) => {
        resolve = r;
      });
      const tool: PendingReadTool = {
        kind: "Read",
        toolUseId,
        exec,
        path,
        startedAt: Date.now(),
        append,
        resolve,
        result,
      };
      opts.traffic?.writeJsonEvent("038-cursor-tool-bridge-pause", {
        kind: tool.kind,
        toolUseId,
        path,
      });
      notifyTool(tool);
      const readResult = await result;
      await appendCursorReadResult(
        exec,
        {
          success: !readResult.isError,
          error: readResult.isError ? readResult.content : undefined,
        },
        append,
      );
      opts.traffic?.writeJsonEvent("038-cursor-tool-bridge-resume", {
        kind: tool.kind,
        toolUseId,
        isError: readResult.isError,
        contentChars: readResult.content.length,
      });
    },
    async shellStreamHandler(exec, append) {
      const { command, workingDirectory, timeoutMs } = cursorShellStreamArgs(exec);
      const toolUseId = `call_cursor_${crypto.randomUUID().replace(/-/g, "")}`;
      let resolve!: (result: NativeToolResult) => void;
      const result = new Promise<NativeToolResult>((r) => {
        resolve = r;
      });
      const tool: PendingShellTool = {
        kind: "Bash",
        toolUseId,
        exec,
        command,
        workingDirectory,
        timeoutMs,
        startedAt: Date.now(),
        append,
        resolve,
        result,
      };
      opts.traffic?.writeJsonEvent("038-cursor-tool-bridge-pause", {
        kind: tool.kind,
        toolUseId,
        command,
        workingDirectory,
        timeoutMs,
      });
      notifyTool(tool);
      const shellResult = await result;
      await appendCursorShellStreamResult(
        exec,
        {
          stdout: shellResult.isError ? undefined : shellResult.content,
          stderr: shellResult.isError ? shellResult.content : undefined,
          exitCode: shellResult.isError ? 1 : 0,
          cwd: workingDirectory,
          localExecutionTimeMs: Date.now() - tool.startedAt,
        },
        append,
      );
      opts.traffic?.writeJsonEvent("038-cursor-tool-bridge-resume", {
        kind: tool.kind,
        toolUseId,
        isError: shellResult.isError,
        contentChars: shellResult.content.length,
      });
    },
    async writeHandler(exec, append) {
      const { path, content } = cursorWriteArgs(exec);
      const toolUseId = `call_cursor_${crypto.randomUUID().replace(/-/g, "")}`;
      let resolve!: (result: NativeToolResult) => void;
      const result = new Promise<NativeToolResult>((r) => {
        resolve = r;
      });
      const tool: PendingWriteTool = {
        kind: "Write",
        toolUseId,
        exec,
        path,
        content,
        startedAt: Date.now(),
        append,
        resolve,
        result,
      };
      opts.traffic?.writeJsonEvent("038-cursor-tool-bridge-pause", {
        kind: tool.kind,
        toolUseId,
        path,
        contentChars: content.length,
      });
      notifyTool(tool);
      const writeResult = await result;
      await appendCursorWriteResult(
        exec,
        {
          success: !writeResult.isError,
          error: writeResult.isError ? writeResult.content : undefined,
        },
        append,
      );
      opts.traffic?.writeJsonEvent("038-cursor-tool-bridge-resume", {
        kind: tool.kind,
        toolUseId,
        isError: writeResult.isError,
        contentChars: writeResult.content.length,
      });
    },
    stream(upstream, signal) {
      state.iterator = decodeCursorStream(upstream, opts.proto, {
        traffic: opts.traffic,
        log: opts.log,
      });
      bridgeStates.set(opts.sessionId, state);
      return streamBridgeUntilToolOrEnd(state, signal);
    },
  };
}

export function resumeCursorShellToolBridge(
  body: AnthropicRequest,
  ctx: RequestContext,
  messageId: string,
): Response | undefined {
  const sessionId = ctx.sessionId;
  if (!sessionId) return undefined;
  const state = bridgeStates.get(sessionId);
  const tool = state?.pendingTool;
  if (!state || !tool) return undefined;
  const result = findToolResult(body, tool.toolUseId);
  if (!result) return undefined;

  state.pendingTool = undefined;
  state.messageId = messageId;
  state.model = body.model;
  tool.resolve({
    content: renderToolResultContent(result.content),
    isError: Boolean(result.is_error),
  });

  const stream = streamBridgeUntilToolOrEnd(state, ctx.signal);
  return new Response(stream, {
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "connection": "keep-alive",
    },
  });
}

function streamBridgeUntilToolOrEnd(
  state: CursorBridgeState,
  signal?: AbortSignal,
): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder();
  return new ReadableStream<Uint8Array>({
    async start(controller) {
      let closed = false;
      let started = false;
      let thinkingOpen = false;
      let textOpen = false;
      let nextIndex = 0;
      let thinkingIndex = -1;
      let textIndex = -1;
      let finalUsage: CursorUsage | undefined;

      const emit = (event: string, data: unknown) => {
        if (closed || signal?.aborted || controller.desiredSize === null) return false;
        state.traffic?.writeJsonEvent("050-downstream-event", { event, data });
        controller.enqueue(encoder.encode(encodeSseEvent(event, data)));
        return true;
      };

      const ensureStart = () => {
        if (started) return;
        started = true;
        emit("message_start", {
          type: "message_start",
          message: {
            id: state.messageId,
            type: "message",
            role: "assistant",
            model: state.model,
            content: [],
            stop_reason: null,
            stop_sequence: null,
            usage: {
              input_tokens: 0,
              output_tokens: 0,
              cache_creation_input_tokens: 0,
              cache_read_input_tokens: 0,
            },
          },
        });
        emit("ping", { type: "ping" });
      };

      const openThinking = () => {
        if (thinkingOpen) return;
        ensureStart();
        thinkingOpen = true;
        thinkingIndex = nextIndex++;
        emit("content_block_start", {
          type: "content_block_start",
          index: thinkingIndex,
          content_block: { type: "thinking", thinking: "", signature: "" },
        });
      };

      const openText = () => {
        if (textOpen) return;
        ensureStart();
        textOpen = true;
        textIndex = nextIndex++;
        emit("content_block_start", {
          type: "content_block_start",
          index: textIndex,
          content_block: { type: "text", text: "" },
        });
      };

      const closeOpenBlocks = () => {
        if (thinkingOpen) {
          emit("content_block_stop", { type: "content_block_stop", index: thinkingIndex });
          thinkingOpen = false;
        }
        if (textOpen) {
          emit("content_block_stop", { type: "content_block_stop", index: textIndex });
          textOpen = false;
        }
      };

      const emitToolUseAndPause = (tool: PendingNativeTool) => {
        closeOpenBlocks();
        ensureStart();
        const index = nextIndex++;
        const input = toolUseInput(tool);
        emit("content_block_start", {
          type: "content_block_start",
          index,
          content_block: {
            type: "tool_use",
            id: tool.toolUseId,
            name: tool.kind,
            input: {},
          },
        });
        emit("content_block_delta", {
          type: "content_block_delta",
          index,
          delta: { type: "input_json_delta", partial_json: input },
        });
        emit("content_block_stop", { type: "content_block_stop", index });
        emit("message_delta", {
          type: "message_delta",
          delta: { stop_reason: "tool_use", stop_sequence: null },
          usage: cursorUsageToAnthropic(finalUsage),
        });
        emit("message_stop", { type: "message_stop" });
      };

      try {
        while (!signal?.aborted) {
          const next = state.pendingNext ?? state.iterator.next();
          state.pendingNext = next;
          const result = await Promise.race([
            next.then((value) => ({ type: "event" as const, value })),
            waitForPendingTool(state).then((tool) => ({ type: "tool" as const, tool })),
          ]);

          if (result.type === "tool") {
            emitToolUseAndPause(result.tool);
            return;
          }

          state.pendingNext = undefined;
          if (result.value.done) break;
          const event = result.value.value;
          state.traffic?.writeJsonEvent("040-cursor-event", event);
          switch (event.type) {
            case "session":
              state.onSession?.(event.sessionId);
              break;
            case "thinking_delta":
              openThinking();
              emit("content_block_delta", {
                type: "content_block_delta",
                index: thinkingIndex,
                delta: { type: "thinking_delta", thinking: event.text },
              });
              break;
            case "text_delta":
              if (thinkingOpen) {
                emit("content_block_stop", { type: "content_block_stop", index: thinkingIndex });
                thinkingOpen = false;
              }
              openText();
              emit("content_block_delta", {
                type: "content_block_delta",
                index: textIndex,
                delta: { type: "text_delta", text: event.text },
              });
              break;
            case "usage":
              finalUsage = event.usage;
              break;
            case "end":
              break;
          }
        }

        ensureStart();
        closeOpenBlocks();
        emit("message_delta", {
          type: "message_delta",
          delta: { stop_reason: "end_turn", stop_sequence: null },
          usage: cursorUsageToAnthropic(finalUsage),
        });
        emit("message_stop", { type: "message_stop" });
        bridgeStates.delete(state.sessionId);
      } catch (err) {
        state.log.warn("cursor bridge stream error", { err: String(err) });
        ensureStart();
        closeOpenBlocks();
        emit("error", {
          type: "error",
          error: { type: "api_error", message: String(err) },
        });
        bridgeStates.delete(state.sessionId);
      } finally {
        closed = true;
        try {
          controller.close();
        } catch {}
      }
    },
  });
}

function waitForPendingTool(state: CursorBridgeState): Promise<PendingNativeTool> {
  if (state.pendingTool) return Promise.resolve(state.pendingTool);
  return new Promise((resolve) => state.waiters.push(resolve));
}

function findToolResult(body: AnthropicRequest, toolUseId: string): AnthropicToolResultBlock | undefined {
  for (let i = body.messages.length - 1; i >= 0; i--) {
    const message = body.messages[i];
    if (!message || message.role !== "user" || typeof message.content === "string") continue;
    for (const block of message.content) {
      if (block.type === "tool_result" && block.tool_use_id === toolUseId) return block;
    }
  }
  return undefined;
}

function renderToolResultContent(content: AnthropicToolResultBlock["content"]): string {
  if (typeof content === "string") return content;
  return content
    .map((block) => {
      if (block.type === "text") return block.text;
      if (block.type === "image") return "[image result omitted]";
      if (block.type === "thinking") return block.thinking;
      return JSON.stringify(block);
    })
    .join("\n");
}

function toolUseInput(tool: PendingNativeTool): string {
  if (tool.kind === "Read") {
    return JSON.stringify({
      file_path: tool.path,
    });
  }
  if (tool.kind === "Write") {
    return JSON.stringify({
      file_path: tool.path,
      content: tool.content,
    });
  }
  return JSON.stringify({
    command: claudeBashCommand(tool),
    timeout: tool.timeoutMs,
    description: "Run Cursor-requested shell command",
    run_in_background: false,
    dangerouslyDisableSandbox: false,
  });
}

function claudeBashCommand(tool: PendingShellTool): string {
  if (!tool.workingDirectory || tool.workingDirectory === process.cwd()) return tool.command;
  return `cd ${shellQuote(tool.workingDirectory)} && ${tool.command}`;
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}
