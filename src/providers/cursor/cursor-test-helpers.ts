import { encodeConnectFrame } from "./client.ts";
import type { CursorProto, ProtoClass, ProtoMessage } from "./proto-loader.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export type JsonProtoClassOptions = {
  mergeFields?: boolean;
};

export function jsonProtoClass(options: JsonProtoClassOptions = {}): ProtoClass {
  const { mergeFields = false } = options;
  return {
    fromBinary(bytes: Uint8Array): ProtoMessage {
      return messageFromJson(JSON.parse(decoder.decode(bytes)), options);
    },
    fromJson(json: unknown): ProtoMessage {
      return messageFromJson(json, options);
    },
  };
}

export function messageFromJson(
  json: unknown,
  options: JsonProtoClassOptions = {},
): ProtoMessage {
  const { mergeFields = false } = options;
  const message: ProtoMessage = {
    toBinary(): Uint8Array {
      return jsonBytes(json);
    },
    toJson(): unknown {
      return json;
    },
  };
  if (!mergeFields) return message;
  return Object.assign(
    message,
    json && typeof json === "object" && !Array.isArray(json) ? json : {},
  );
}

export function createFakeProto(options: JsonProtoClassOptions = {}): CursorProto {
  const protoClass = jsonProtoClass(options);
  return {
    AgentServerMessage: protoClass,
    AgentClientMessage: protoClass,
  };
}

export const fakeProtoMerged = createFakeProto({ mergeFields: true });
export const fakeProto = createFakeProto();

export function frame(json: unknown): Uint8Array {
  return encodeConnectFrame(jsonBytes(json));
}

export function jsonBytes(json: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(json));
}

export function decodeFrameJson(frameBytes: Uint8Array): unknown {
  const buf = Buffer.from(frameBytes);
  const len = buf.readUInt32BE(1);
  return JSON.parse(decoder.decode(buf.subarray(5, 5 + len)));
}

export function streamFromChunks(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(chunk);
      controller.close();
    },
  });
}
