import { afterEach, describe, expect, it } from "bun:test";
import { cursorProvider, createCursorProvider } from "./index.ts";
import type { RequestContext } from "../types.ts";
import { encodeConnectFrame, runCursorAgent } from "./client.ts";
import {
  decodeFrameJson,
  fakeProtoMerged as fakeProto,
  frame,
  jsonBytes,
  streamFromChunks,
} from "./cursor-test-helpers.ts";
import { parseSseStream } from "../../sse.ts";
import { mkdir, mkdtemp, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

const originalToken = process.env.CCP_CURSOR_AUTH_TOKEN;

afterEach(() => {
  if (originalToken === undefined) delete process.env.CCP_CURSOR_AUTH_TOKEN;
  else process.env.CCP_CURSOR_AUTH_TOKEN = originalToken;
});

describe("Cursor provider auth errors", () => {
  it("surfaces expired auth before calling Cursor", async () => {
    process.env.CCP_CURSOR_AUTH_TOKEN = jwt({ exp: 1 });

    const response = await cursorProvider.handleMessages(
      {
        model: "cursor",
        messages: [{ role: "user", content: "hello" }],
      },
      fakeCtx(),
    );
    const body = (await response.json()) as {
      error: { type: string; message: string };
    };

    expect(response.status).toBe(401);
    expect(response.headers.get("content-type")).toBe("application/json");
    expect(body.error.type).toBe("authentication_error");
    expect(body.error.message).toContain("expired or near expiry");
  });
});

describe("Cursor provider messages", () => {
  it("returns assistant text for non-streaming requests", async () => {
    const provider = createCursorProvider({
      loadAuth: async () => ({ accessToken: "token", source: "test" }),
      runAgent: async () =>
        streamFromChunks([
          frame({ interactionUpdate: { textDelta: { text: "hello" } } }),
          frame({ interactionUpdate: { turnEnded: { inputTokens: "4", outputTokens: "1" } } }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
      proto: fakeProto,
    });

    const response = await provider.handleMessages(
      {
        model: "cursor",
        messages: [{ role: "user", content: "hello" }],
      },
      fakeCtx(),
    );
    const body = (await response.json()) as { content: Array<{ type: string; text?: string }> };

    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("application/json");
    expect(body.content).toEqual([{ type: "text", text: "hello" }]);
  });

  it("returns valid Anthropic SSE for streaming requests", async () => {
    const provider = createCursorProvider({
      loadAuth: async () => ({ accessToken: "token", source: "test" }),
      runAgent: async () =>
        streamFromChunks([
          frame({ interactionUpdate: { textDelta: { text: "streamed" } } }),
          frame({ interactionUpdate: { turnEnded: { inputTokens: "4", outputTokens: "2" } } }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
      proto: fakeProto,
    });

    const response = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        messages: [{ role: "user", content: "hello" }],
      },
      fakeCtx(),
    );
    const events = [];
    for await (const event of parseSseStream(response.body!)) {
      events.push({ event: event.event, data: JSON.parse(event.data) });
    }

    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(response.headers.get("cache-control")).toBe("no-cache");
    expect(response.headers.get("connection")).toBe("keep-alive");
    expect(events.map((event) => event.event)).toContain("message_start");
    expect(events.find((event) => event.event === "content_block_delta")?.data.delta.text).toBe(
      "streamed",
    );
    expect(events.at(-1)?.event).toBe("message_stop");
  });

  it("preserves Cursor Connect end error status for non-streaming requests", async () => {
    const provider = createCursorProvider({
      loadAuth: async () => ({ accessToken: "token", source: "test" }),
      runAgent: async () =>
        streamFromChunks([
          encodeConnectFrame(
            jsonBytes({
              error: {
                code: "resource_exhausted",
                message: "Error",
                details: [
                  {
                    debug: {
                      details: {
                        additionalInfo: {
                          chatMessage: "You've hit your free requests limit.",
                        },
                      },
                    },
                  },
                ],
              },
            }),
            2,
          ),
        ]),
      proto: fakeProto,
    });

    const response = await provider.handleMessages(
      {
        model: "cursor",
        messages: [{ role: "user", content: "hello" }],
      },
      fakeCtx(),
    );
    const body = (await response.json()) as { error: { message: string } };

    expect(response.status).toBe(429);
    expect(body.error.message).toContain("resource_exhausted");
    expect(body.error.message).toContain("free requests limit");
  });

  it("bridges Cursor shellStreamArgs through Claude Bash tool_use and resumes from tool_result", async () => {
    let serverController!: ReadableStreamDefaultController<Uint8Array>;
    const sentFrames: Array<Record<string, any>> = [];
    let finalResponseSent = false;
    const workingDirectory = "/tmp/cursor bridge cwd";
    const serverReadable = new ReadableStream<Uint8Array>({
      start(controller) {
        serverController = controller;
        queueMicrotask(() => {
          controller.enqueue(frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 11,
                execId: "exec-shell",
                message: {
                  case: "shellStreamArgs",
                  value: {
                    command: "printf should-not-run",
                    workingDirectory,
                    timeout: 5000,
                  },
                },
              },
            },
          }));
        });
      },
    });
    const provider = createCursorProvider({
      loadAuth: async () => ({ accessToken: "token", source: "test" }),
      runAgent: async (opts) =>
        runCursorAgent({
          ...opts,
          proto: fakeProto,
          openRunStream: async () => ({
            readable: serverReadable,
            status: Promise.resolve({ status: 200 }),
            async write(frameBytes) {
              const message = decodeFrameJson(frameBytes) as Record<string, any>;
              sentFrames.push(message);
              if (message.execClientMessage?.shellStream?.exit && !finalResponseSent) {
                finalResponseSent = true;
                serverController.enqueue(frame({
                  interactionUpdate: { textDelta: { text: "resumed after native shell" } },
                }));
                serverController.enqueue(frame({
                  interactionUpdate: { turnEnded: { inputTokens: "4", outputTokens: "3" } },
                }));
                serverController.close();
              }
            },
            close() {},
          }),
        }),
      proto: fakeProto,
    });

    const initial = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        tools: [{ name: "Bash", input_schema: { type: "object" } }],
        messages: [{ role: "user", content: "run shell" }],
      },
      fakeCtx(),
    );
    const initialEvents = await collectSse(initial);
    const toolStart = initialEvents.find((event) => event.event === "content_block_start"
      && event.data.content_block?.type === "tool_use");

    expect(toolStart?.data.content_block.name).toBe("Bash");
    expect(toolStart?.data.content_block.id).toStartWith("call_cursor_");
    const toolInputDelta = initialEvents.find((event) => event.event === "content_block_delta"
      && event.data.delta?.type === "input_json_delta");
    expect(JSON.parse(toolInputDelta?.data.delta.partial_json).command).toBe(
      "cd '/tmp/cursor bridge cwd' && printf should-not-run",
    );
    expect(initialEvents.find((event) => event.event === "message_delta")?.data.delta.stop_reason).toBe("tool_use");
    expect(sentFrames.some((message) => message.execClientMessage?.shellStream?.stdout)).toBe(false);

    const resume = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        messages: [
          {
            role: "assistant",
            content: [{
              type: "tool_use",
              id: toolStart!.data.content_block.id,
              name: "Bash",
              input: { command: "printf should-not-run" },
            }],
          },
          {
            role: "user",
            content: [{
              type: "tool_result",
              tool_use_id: toolStart!.data.content_block.id,
              content: "native shell output",
              is_error: false,
            }],
          },
        ],
      },
      fakeCtx(),
    );
    const resumeEvents = await collectSse(resume);

    expect(sentFrames.some((message) =>
      message.execClientMessage?.shellStream?.stdout?.data === "native shell output"
    )).toBe(true);
    expect(sentFrames.some((message) =>
      message.execClientMessage?.shellStream?.stdout?.data === "should-not-run"
    )).toBe(false);
    expect(resumeEvents.find((event) => event.event === "content_block_delta")?.data.delta.text).toBe(
      "resumed after native shell",
    );
    expect(resumeEvents.find((event) => event.event === "message_delta")?.data.delta.stop_reason).toBe("end_turn");
    expect(resumeEvents.at(-1)?.event).toBe("message_stop");
  });

  it("denies Cursor shellStreamArgs instead of executing internally when Claude did not advertise Bash", async () => {
    let serverController!: ReadableStreamDefaultController<Uint8Array>;
    const sentFrames: Array<Record<string, any>> = [];
    let finalResponseSent = false;
    const dir = await mkdtemp(join(tmpdir(), "cursor-denied-shell-"));
    const file = join(dir, "hidden-edit.txt");
    const serverReadable = new ReadableStream<Uint8Array>({
      start(controller) {
        serverController = controller;
        queueMicrotask(() => {
          controller.enqueue(frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 13,
                execId: "exec-shell-denied",
                message: {
                  case: "shellStreamArgs",
                  value: {
                    command: `printf hidden > ${JSON.stringify(file)}`,
                    workingDirectory: dir,
                    timeout: 5000,
                  },
                },
              },
            },
          }));
        });
      },
    });
    const provider = createCursorProvider({
      loadAuth: async () => ({ accessToken: "token", source: "test" }),
      runAgent: async (opts) =>
        runCursorAgent({
          ...opts,
          proto: fakeProto,
          openRunStream: async () => ({
            readable: serverReadable,
            status: Promise.resolve({ status: 200 }),
            async write(frameBytes) {
              const message = decodeFrameJson(frameBytes) as Record<string, any>;
              sentFrames.push(message);
              if (message.execClientMessage?.shellStream?.exit && !finalResponseSent) {
                finalResponseSent = true;
                serverController.enqueue(frame({
                  interactionUpdate: { textDelta: { text: "shell was denied" } },
                }));
                serverController.enqueue(frame({
                  interactionUpdate: { turnEnded: { inputTokens: "4", outputTokens: "3" } },
                }));
                serverController.close();
              }
            },
            close() {},
          }),
        }),
      proto: fakeProto,
    });

    const response = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        tools: [{ name: "Read", input_schema: { type: "object" } }],
        messages: [{ role: "user", content: "try hidden shell edit" }],
      },
      fakeCtx(),
    );
    const events = await collectSse(response);

    expect(events.some((event) =>
      event.event === "content_block_start" && event.data.content_block?.name === "Bash"
    )).toBe(false);
    expect(sentFrames.some((message) =>
      String(message.execClientMessage?.shellStream?.stderr?.data ?? "").includes("did not advertise the Bash tool")
    )).toBe(true);
    expect(sentFrames.find((message) => message.execClientMessage?.shellStream?.exit)
      ?.execClientMessage.shellStream.exit.code).toBe(1);
    expect(await exists(file)).toBe(false);
    expect(events.find((event) => event.event === "content_block_delta")?.data.delta.text).toBe("shell was denied");
    expect(events.find((event) => event.event === "message_delta")?.data.delta.stop_reason).toBe("end_turn");
  });

  it("bridges Cursor writeArgs through Claude Write tool_use and resumes from tool_result", async () => {
    let serverController!: ReadableStreamDefaultController<Uint8Array>;
    const sentFrames: Array<Record<string, any>> = [];
    let finalResponseSent = false;
    const dir = await mkdtemp(join(tmpdir(), "cursor-write-bridge-"));
    const file = join(dir, "history", "findings.md");
    const fileContent = "finding one\nfinding two\n";
    const serverReadable = new ReadableStream<Uint8Array>({
      start(controller) {
        serverController = controller;
        queueMicrotask(() => {
          controller.enqueue(frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 12,
                execId: "exec-write",
                message: {
                  case: "writeArgs",
                  value: {
                    path: file,
                    fileText: fileContent,
                    returnFileContentAfterWrite: true,
                  },
                },
              },
            },
          }));
        });
      },
    });
    const provider = createCursorProvider({
      loadAuth: async () => ({ accessToken: "token", source: "test" }),
      runAgent: async (opts) =>
        runCursorAgent({
          ...opts,
          proto: fakeProto,
          openRunStream: async () => ({
            readable: serverReadable,
            status: Promise.resolve({ status: 200 }),
            async write(frameBytes) {
              const message = decodeFrameJson(frameBytes) as Record<string, any>;
              sentFrames.push(message);
              if (message.execClientMessage?.writeResult?.success && !finalResponseSent) {
                finalResponseSent = true;
                serverController.enqueue(frame({
                  interactionUpdate: { textDelta: { text: "resumed after native write" } },
                }));
                serverController.enqueue(frame({
                  interactionUpdate: { turnEnded: { inputTokens: "5", outputTokens: "4" } },
                }));
                serverController.close();
              }
            },
            close() {},
          }),
        }),
      proto: fakeProto,
    });

    const initial = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        tools: [{ name: "Write", input_schema: { type: "object" } }],
        messages: [{ role: "user", content: "write file" }],
      },
      fakeCtx(),
    );
    const initialEvents = await collectSse(initial);
    const toolStart = initialEvents.find((event) => event.event === "content_block_start"
      && event.data.content_block?.type === "tool_use");
    const toolInputDelta = initialEvents.find((event) => event.event === "content_block_delta"
      && event.data.delta?.type === "input_json_delta");

    expect(toolStart?.data.content_block.name).toBe("Write");
    expect(toolStart?.data.content_block.id).toStartWith("call_cursor_");
    expect(JSON.parse(toolInputDelta?.data.delta.partial_json)).toEqual({
      file_path: file,
      content: fileContent,
    });
    expect(initialEvents.find((event) => event.event === "message_delta")?.data.delta.stop_reason).toBe("tool_use");
    expect(sentFrames.some((message) => message.execClientMessage?.writeResult)).toBe(false);

    await mkdir(join(dir, "history"), { recursive: true });
    await writeFile(file, fileContent, "utf8");
    const resume = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        messages: [
          {
            role: "assistant",
            content: [{
              type: "tool_use",
              id: toolStart!.data.content_block.id,
              name: "Write",
              input: { file_path: file, content: fileContent },
            }],
          },
          {
            role: "user",
            content: [{
              type: "tool_result",
              tool_use_id: toolStart!.data.content_block.id,
              content: `Wrote 3 lines to ${file}`,
              is_error: false,
            }],
          },
        ],
      },
      fakeCtx(),
    );
    const resumeEvents = await collectSse(resume);
    const writeResult = sentFrames.find((message) => message.execClientMessage?.writeResult)
      ?.execClientMessage.writeResult.success;

    expect(writeResult).toEqual({
      path: file,
      linesCreated: 3,
      fileSize: 24,
      fileContentAfterWrite: fileContent,
    });
    expect(sentFrames.at(-1)).toEqual({ execClientControlMessage: { streamClose: { id: 12 } } });
    expect(resumeEvents.find((event) => event.event === "content_block_delta")?.data.delta.text).toBe(
      "resumed after native write",
    );
    expect(resumeEvents.find((event) => event.event === "message_delta")?.data.delta.stop_reason).toBe("end_turn");
    expect(resumeEvents.at(-1)?.event).toBe("message_stop");
  });

  it("bridges Cursor readArgs before writeArgs so Claude Write can edit existing files", async () => {
    let serverController!: ReadableStreamDefaultController<Uint8Array>;
    const sentFrames: Array<Record<string, any>> = [];
    let writeRequested = false;
    let finalResponseSent = false;
    const dir = await mkdtemp(join(tmpdir(), "cursor-read-write-bridge-"));
    const file = join(dir, "README.md");
    const originalContent = "# Demo\n\nOld detail.\n";
    const updatedContent = "# Demo\n\nUpdated detail.\n";
    await writeFile(file, originalContent, "utf8");
    const serverReadable = new ReadableStream<Uint8Array>({
      start(controller) {
        serverController = controller;
        queueMicrotask(() => {
          controller.enqueue(frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 21,
                execId: "exec-read",
                message: { case: "readArgs", value: { path: file } },
              },
            },
          }));
        });
      },
    });
    const provider = createCursorProvider({
      loadAuth: async () => ({ accessToken: "token", source: "test" }),
      runAgent: async (opts) =>
        runCursorAgent({
          ...opts,
          proto: fakeProto,
          openRunStream: async () => ({
            readable: serverReadable,
            status: Promise.resolve({ status: 200 }),
            async write(frameBytes) {
              const message = decodeFrameJson(frameBytes) as Record<string, any>;
              sentFrames.push(message);
              if (message.execClientMessage?.readResult?.success && !writeRequested) {
                writeRequested = true;
                serverController.enqueue(frame({
                  message: {
                    case: "execServerMessage",
                    value: {
                      id: 22,
                      execId: "exec-write",
                      message: {
                        case: "writeArgs",
                        value: {
                          path: file,
                          fileText: updatedContent,
                          returnFileContentAfterWrite: true,
                        },
                      },
                    },
                  },
                }));
              }
              if (message.execClientMessage?.writeResult?.success && !finalResponseSent) {
                finalResponseSent = true;
                serverController.enqueue(frame({
                  interactionUpdate: { textDelta: { text: "edited existing file" } },
                }));
                serverController.enqueue(frame({
                  interactionUpdate: { turnEnded: { inputTokens: "8", outputTokens: "5" } },
                }));
                serverController.close();
              }
            },
            close() {},
          }),
        }),
      proto: fakeProto,
    });

    const initial = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        tools: [
          { name: "Read", input_schema: { type: "object" } },
          { name: "Write", input_schema: { type: "object" } },
        ],
        messages: [{ role: "user", content: "edit readme" }],
      },
      fakeCtx(),
    );
    const initialEvents = await collectSse(initial);
    const readToolStart = initialEvents.find((event) => event.event === "content_block_start"
      && event.data.content_block?.type === "tool_use");
    const readToolInput = initialEvents.find((event) => event.event === "content_block_delta"
      && event.data.delta?.type === "input_json_delta");

    expect(readToolStart?.data.content_block.name).toBe("Read");
    expect(JSON.parse(readToolInput?.data.delta.partial_json)).toEqual({ file_path: file });
    expect(sentFrames.some((message) => message.execClientMessage?.readResult)).toBe(false);

    const afterRead = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        messages: [
          {
            role: "assistant",
            content: [{
              type: "tool_use",
              id: readToolStart!.data.content_block.id,
              name: "Read",
              input: { file_path: file },
            }],
          },
          {
            role: "user",
            content: [{
              type: "tool_result",
              tool_use_id: readToolStart!.data.content_block.id,
              content: originalContent,
              is_error: false,
            }],
          },
        ],
      },
      fakeCtx(),
    );
    const afterReadEvents = await collectSse(afterRead);
    const readResult = sentFrames.find((message) => message.execClientMessage?.readResult)
      ?.execClientMessage.readResult.success;
    const writeToolStart = afterReadEvents.find((event) => event.event === "content_block_start"
      && event.data.content_block?.type === "tool_use");
    const writeToolInput = afterReadEvents.find((event) => event.event === "content_block_delta"
      && event.data.delta?.type === "input_json_delta");

    expect(readResult).toEqual({
      path: file,
      content: originalContent,
      totalLines: 4,
      fileSize: "20",
    });
    expect(writeToolStart?.data.content_block.name).toBe("Write");
    expect(JSON.parse(writeToolInput?.data.delta.partial_json)).toEqual({
      file_path: file,
      content: updatedContent,
    });

    await writeFile(file, updatedContent, "utf8");
    const afterWrite = await provider.handleMessages(
      {
        model: "cursor",
        stream: true,
        messages: [
          {
            role: "assistant",
            content: [{
              type: "tool_use",
              id: writeToolStart!.data.content_block.id,
              name: "Write",
              input: { file_path: file, content: updatedContent },
            }],
          },
          {
            role: "user",
            content: [{
              type: "tool_result",
              tool_use_id: writeToolStart!.data.content_block.id,
              content: `Wrote 3 lines to ${file}`,
              is_error: false,
            }],
          },
        ],
      },
      fakeCtx(),
    );
    const afterWriteEvents = await collectSse(afterWrite);
    const writeResult = sentFrames.find((message) => message.execClientMessage?.writeResult)
      ?.execClientMessage.writeResult.success;

    expect(writeResult).toEqual({
      path: file,
      linesCreated: 4,
      fileSize: 24,
      fileContentAfterWrite: updatedContent,
    });
    expect(afterWriteEvents.find((event) => event.event === "content_block_delta")?.data.delta.text).toBe(
      "edited existing file",
    );
    expect(afterWriteEvents.find((event) => event.event === "message_delta")?.data.delta.stop_reason).toBe(
      "end_turn",
    );
  });
});

function fakeCtx(): RequestContext {
  return {
    reqId: "req",
    sessionId: "session",
    signal: new AbortController().signal,
    childLogger: () => ({
      debug() {},
      info() {},
      warn() {},
      error() {},
      child() {
        return this;
      },
    }),
  };
}

async function collectSse(response: Response): Promise<Array<{ event: string; data: any }>> {
  const events = [];
  for await (const event of parseSseStream(response.body!)) {
    events.push({ event: event.event ?? "message", data: JSON.parse(event.data) });
  }
  return events;
}

async function exists(path: string): Promise<boolean> {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

function jwt(payload: Record<string, unknown>): string {
  return [
    Buffer.from(JSON.stringify({ alg: "none", typ: "JWT" })).toString("base64url"),
    Buffer.from(JSON.stringify(payload)).toString("base64url"),
    "signature",
  ].join(".");
}
