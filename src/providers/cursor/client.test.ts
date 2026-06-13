import { describe, expect, it } from "bun:test";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { decodeCursorStream, encodeConnectFrame, runCursorAgent } from "./client.ts";
import {
  decodeFrameJson,
  fakeProtoMerged as fakeProto,
  frame,
  jsonBytes,
  streamFromChunks,
} from "./cursor-test-helpers.ts";
import type { RequestContext } from "../types.ts";

describe("Cursor protocol client", () => {
  it("acks exec setup and KV messages on the HTTP/2 Run stream", async () => {
    const sentFrames: Uint8Array[] = [];

    const upstream = await runCursorAgent({
      prompt: "hello",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: streamFromChunks([
          frame({
            message: {
              case: "execServerMessage",
              value: { id: 0, message: { case: "requestContextArgs", value: {} } },
            },
          }),
          frame({
            message: {
              case: "kvServerMessage",
              value: { id: 0, message: { case: "setBlobArgs", value: {} } },
            },
          }),
          frame({
            message: {
              case: "kvServerMessage",
              value: { id: 2, message: { case: "getBlobArgs", value: {} } },
            },
          }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
        status: Promise.resolve({ status: 200 }),
        async write(frame) {
          sentFrames.push(frame);
        },
        close() {},
      }),
    });
    await drain(upstream);

    const clientMessages = sentFrames.map(decodeFrameJson) as Array<Record<string, any>>;
    expect(clientMessages[0]?.runRequest.conversationId).toBe("conversation");
    expect(clientMessages[0]?.runRequest.clientSupportsInlineImages).toBe(true);
    expect(clientMessages[1]).toEqual({ execClientControlMessage: { heartbeat: {} } });
    expect(clientMessages[2]?.execClientMessage.requestContextResult.success.requestContext).toBeDefined();
    expect(clientMessages[2]?.execClientMessage.requestContextResult.success.requestContext.webSearchEnabled).toBe(true);
    expect(clientMessages[2]?.execClientMessage.requestContextResult.success.requestContext.webFetchEnabled).toBe(true);
    expect(clientMessages[3]).toEqual({ execClientControlMessage: { streamClose: {} } });
    expect(clientMessages[4]).toEqual({ kvClientMessage: { setBlobResult: {} } });
    expect(clientMessages[5]).toEqual({ kvClientMessage: { getBlobResult: {}, id: 2 } });
  });

  it("sends selected images in the Cursor run request", async () => {
    const sentFrames: Uint8Array[] = [];

    const upstream = await runCursorAgent({
      prompt: "describe image",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      selectedImages: [
        {
          data: "aGVsbG8=",
          uuid: "image-id",
          path: "claude-image-1.png",
          mimeType: "image/png",
        },
      ],
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: streamFromChunks([encodeConnectFrame(jsonBytes({}), 2)]),
        status: Promise.resolve({ status: 200 }),
        async write(frame) {
          sentFrames.push(frame);
        },
        close() {},
      }),
    });
    await drain(upstream);

    const clientMessages = sentFrames.map(decodeFrameJson) as Array<Record<string, any>>;
    expect(clientMessages[0]?.runRequest.action.userMessageAction.userMessage.selectedContext).toEqual({
      selectedImages: [
        {
          data: "aGVsbG8=",
          uuid: "image-id",
          path: "claude-image-1.png",
          mimeType: "image/png",
        },
      ],
    });
    expect(clientMessages[0]?.runRequest.clientSupportsInlineImages).toBe(true);
  });

  it("approves Cursor web search and web fetch interaction queries", async () => {
    const sentFrames: Uint8Array[] = [];

    const upstream = await runCursorAgent({
      prompt: "search the web",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: streamFromChunks([
          frame({
            message: {
              case: "interactionQuery",
              value: { id: 11, query: { case: "webSearchRequestQuery", value: { args: { searchTerm: "cursor" } } } },
            },
          }),
          frame({
            message: {
              case: "interactionQuery",
              value: { id: 12, query: { case: "webFetchRequestQuery", value: { args: { url: "https://example.com" } } } },
            },
          }),
          frame({
            message: {
              case: "interactionQuery",
              value: { id: 13, query: { case: "generateImageRequestQuery", value: {} } },
            },
          }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
        status: Promise.resolve({ status: 200 }),
        async write(frame) {
          sentFrames.push(frame);
        },
        close() {},
      }),
    });
    await drain(upstream);

    const clientMessages = sentFrames.map(decodeFrameJson) as Array<Record<string, any>>;
    expect(clientMessages).toContainEqual({
      interactionResponse: { id: 11, webSearchRequestResponse: { approved: {} } },
    });
    expect(clientMessages).toContainEqual({
      interactionResponse: { id: 12, webFetchRequestResponse: { approved: {} } },
    });
    expect(clientMessages.some((message) => message.interactionResponse?.id === 13)).toBe(false);
  });

  it("answers Cursor readArgs with file content and closes the exec stream", async () => {
    const sentFrames: Uint8Array[] = [];
    const dir = await mkdtemp(join(tmpdir(), "cursor-read-"));
    const file = join(dir, "SKILL.md");
    await writeFile(file, "hello\nworld\n", "utf8");

    const upstream = await runCursorAgent({
      prompt: "hello",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: streamFromChunks([
          frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 7,
                execId: "exec-read",
                message: { case: "readArgs", value: { path: file } },
              },
            },
          }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
        status: Promise.resolve({ status: 200 }),
        async write(frame) {
          sentFrames.push(frame);
        },
        close() {},
      }),
    });
    await drain(upstream);

    const clientMessages = sentFrames.map(decodeFrameJson) as Array<Record<string, any>>;
    expect(clientMessages[1]).toEqual({ execClientControlMessage: { heartbeat: {} } });
    expect(clientMessages[2]).toEqual({
      execClientMessage: {
        id: 7,
        execId: "exec-read",
        readResult: {
          success: {
            path: file,
            content: "hello\nworld\n",
            totalLines: 3,
            fileSize: "12",
          },
        },
      },
    });
    expect(clientMessages[3]).toEqual({ execClientControlMessage: { streamClose: { id: 7 } } });
  });

  it("answers Cursor writeArgs by writing the file and closing the exec stream", async () => {
    const sentFrames: Uint8Array[] = [];
    const dir = await mkdtemp(join(tmpdir(), "cursor-write-"));
    const file = join(dir, "history", "findings.md");

    const upstream = await runCursorAgent({
      prompt: "hello",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: streamFromChunks([
          frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 10,
                execId: "exec-write",
                message: {
                  case: "writeArgs",
                  value: {
                    path: file,
                    fileText: "finding one\nfinding two\n",
                    returnFileContentAfterWrite: true,
                  },
                },
              },
            },
          }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
        status: Promise.resolve({ status: 200 }),
        async write(frame) {
          sentFrames.push(frame);
        },
        close() {},
      }),
    });
    await drain(upstream);

    const clientMessages = sentFrames.map(decodeFrameJson) as Array<Record<string, any>>;
    expect(await readFile(file, "utf8")).toBe("finding one\nfinding two\n");
    expect(clientMessages[1]).toEqual({ execClientControlMessage: { heartbeat: {} } });
    expect(clientMessages[2]).toEqual({
      execClientMessage: {
        id: 10,
        execId: "exec-write",
        writeResult: {
          success: {
            path: file,
            linesCreated: 3,
            fileSize: 24,
            fileContentAfterWrite: "finding one\nfinding two\n",
          },
        },
      },
    });
    expect(clientMessages[3]).toEqual({ execClientControlMessage: { streamClose: { id: 10 } } });
  });

  it("answers Cursor grepArgs with glob file matches and closes the exec stream", async () => {
    const sentFrames: Uint8Array[] = [];
    const dir = await mkdtemp(join(tmpdir(), "cursor-grep-"));
    await writeFile(join(dir, "README.md"), "hello\n", "utf8");
    await writeFile(join(dir, "notes.txt"), "hello\n", "utf8");

    const upstream = await runCursorAgent({
      prompt: "hello",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: streamFromChunks([
          frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 8,
                execId: "exec-grep",
                message: {
                  case: "grepArgs",
                  value: {
                    pattern: "",
                    path: dir,
                    glob: "**/README*",
                    outputMode: "files_with_matches",
                  },
                },
              },
            },
          }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
        status: Promise.resolve({ status: 200 }),
        async write(frame) {
          sentFrames.push(frame);
        },
        close() {},
      }),
    });
    await drain(upstream);

    const clientMessages = sentFrames.map(decodeFrameJson) as Array<Record<string, any>>;
    expect(clientMessages[1]).toEqual({ execClientControlMessage: { heartbeat: {} } });
    expect(clientMessages[2]).toEqual({
      execClientMessage: {
        id: 8,
        execId: "exec-grep",
        grepResult: {
          success: {
            pattern: "**/README*",
            path: dir,
            outputMode: "files_with_matches",
            workspaceResults: {
              [dir]: {
                files: {
                  files: ["README.md"],
                  totalFiles: 1,
                },
              },
            },
          },
        },
      },
    });
    expect(clientMessages[3]).toEqual({ execClientControlMessage: { streamClose: { id: 8 } } });
  });

  it("answers Cursor shellStreamArgs with stream events and closes the exec stream", async () => {
    const sentFrames: Uint8Array[] = [];

    const upstream = await runCursorAgent({
      prompt: "hello",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: streamFromChunks([
          frame({
            message: {
              case: "execServerMessage",
              value: {
                id: 9,
                execId: "exec-shell",
                message: {
                  case: "shellStreamArgs",
                  value: {
                    command: "printf stdout; printf stderr >&2",
                    workingDirectory: process.cwd(),
                    timeout: 5000,
                  },
                },
              },
            },
          }),
          encodeConnectFrame(jsonBytes({}), 2),
        ]),
        status: Promise.resolve({ status: 200 }),
        async write(frame) {
          sentFrames.push(frame);
        },
        close() {},
      }),
    });
    let trace = "";
    for await (const event of decodeCursorStream(upstream, fakeProto)) {
      if (event.type === "text_delta") trace += event.text;
    }

    const clientMessages = sentFrames.map(decodeFrameJson) as Array<Record<string, any>>;
    expect(clientMessages[1]).toEqual({ execClientControlMessage: { heartbeat: {} } });
    expect(clientMessages.some((message) => message.execClientMessage?.shellStream?.start)).toBe(true);
    expect(clientMessages.some((message) => message.execClientMessage?.shellStream?.stdout?.data === "stdout")).toBe(true);
    expect(clientMessages.some((message) => message.execClientMessage?.shellStream?.stderr?.data === "stderr")).toBe(true);
    expect(clientMessages.some((message) => message.execClientMessage?.shellStream?.exit?.code === 0)).toBe(true);
    expect(clientMessages.at(-1)).toEqual({ execClientControlMessage: { streamClose: { id: 9 } } });
    expect(trace).toContain("Bash(printf stdout; printf stderr >&2)");
    expect(trace).toContain("stdout");
    expect(trace).toContain("stderr");
  });

  it("closes the Cursor run stream when the downstream consumer cancels", async () => {
    let closeCalls = 0;
    const upstream = await runCursorAgent({
      prompt: "hello",
      mode: "AGENT_MODE_AGENT",
      conversationId: "conversation",
      model: { modelId: "composer-2.5" },
      auth: { accessToken: "token", source: "test" },
      ctx: fakeCtx(),
      proto: fakeProto,
      openRunStream: async () => ({
        readable: new ReadableStream<Uint8Array>(),
        status: Promise.resolve({ status: 200 }),
        async write() {},
        close() {
          closeCalls += 1;
        },
      }),
    });

    await upstream.cancel("done");

    expect(closeCalls).toBe(1);
  });

  it("stops Cursor heartbeats when the Run stream closes", async () => {
    const originalSetInterval = globalThis.setInterval;
    const originalClearInterval = globalThis.clearInterval;
    const fakeTimer = Symbol("heartbeat-timer");
    let heartbeatCleared = false;
    let resolveClosed!: () => void;
    const closed = new Promise<void>((resolve) => {
      resolveClosed = resolve;
    });
    let closeCalls = 0;

    globalThis.setInterval = (() => fakeTimer) as unknown as typeof setInterval;
    globalThis.clearInterval = ((timer: unknown) => {
      if (timer === fakeTimer) heartbeatCleared = true;
    }) as typeof clearInterval;

    try {
      await runCursorAgent({
        prompt: "hello",
        mode: "AGENT_MODE_AGENT",
        conversationId: "conversation",
        model: { modelId: "composer-2.5" },
        auth: { accessToken: "token", source: "test" },
        ctx: fakeCtx(),
        proto: fakeProto,
        openRunStream: async () => ({
          readable: new ReadableStream<Uint8Array>(),
          status: Promise.resolve({ status: 200 }),
          closed,
          async write() {},
          close() {
            closeCalls += 1;
          },
        }),
      });

      resolveClosed();
      await Promise.resolve();

      expect(heartbeatCleared).toBe(true);
      expect(closeCalls).toBe(1);
    } finally {
      globalThis.setInterval = originalSetInterval;
      globalThis.clearInterval = originalClearInterval;
    }
  });
});

async function drain(stream: ReadableStream<Uint8Array>): Promise<void> {
  const reader = stream.getReader();
  try {
    while (!(await reader.read()).done) {
      // Drain.
    }
  } finally {
    reader.releaseLock();
  }
}

function fakeCtx(): RequestContext {
  return {
    reqId: "req",
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
