import { encode } from "gpt-tokenizer/model/gpt-4o";
import type { AnthropicRequest } from "../../anthropic/schema.ts";
import type { KimiChatRequest } from "./translate/request.ts";
import {
  flattenSystemText,
  normalizeContent,
  toolResultToString,
} from "../translate/anthropic-content.ts";
import { countToolSchemaTokens } from "../shared/tool-schema.ts";

const IMAGE_TOKEN_ESTIMATE = 2000;

// Approximate: Kimi's tokenizer isn't gpt-tokenizer, but Claude Code's
// compaction logic only needs a monotonic estimate, not an exact count.
export function countTokens(req: AnthropicRequest): number {
  let total = 0;
  const system = flattenSystemText(req.system);
  if (system) total += encode(system).length;

  for (const msg of req.messages) {
    const blocks = normalizeContent(msg.content);
    for (const block of blocks) {
      if (block.type === "text") {
        total += encode(block.text).length;
      } else if (block.type === "image") {
        total += IMAGE_TOKEN_ESTIMATE;
      } else if (block.type === "tool_use") {
        total += encode(block.name).length;
        total += encode(JSON.stringify(block.input ?? {})).length;
      } else if (block.type === "tool_result") {
        total += encode(toolResultToString(block.content)).length;
      } else if (block.type === "thinking") {
        total += encode(block.thinking).length;
      }
    }
  }

  total += countToolSchemaTokens(
    req.tools,
    (tool) => tool.name,
    (tool) => tool.description,
    (tool) => tool.input_schema,
  );

  total += req.messages.length * 4;
  return total;
}

export function countTranslatedTokens(req: KimiChatRequest): number {
  let total = 0;
  for (const m of req.messages) {
    if (m.role === "system") {
      total += encode(m.content).length;
    } else if (m.role === "user") {
      if (typeof m.content === "string") total += encode(m.content).length;
      else {
        for (const p of m.content) {
          if (p.type === "text") total += encode(p.text).length;
          else total += IMAGE_TOKEN_ESTIMATE;
        }
      }
    } else if (m.role === "assistant") {
      if (typeof m.content === "string") total += encode(m.content).length;
      if (m.reasoning_content) total += encode(m.reasoning_content).length;
      for (const tc of m.tool_calls ?? []) {
        total += encode(tc.function.name).length;
        total += encode(tc.function.arguments).length;
      }
    } else if (m.role === "tool") {
      if (typeof m.content === "string") total += encode(m.content).length;
      else {
        for (const p of m.content) {
          if (p.type === "text") total += encode(p.text).length;
          else total += IMAGE_TOKEN_ESTIMATE;
        }
      }
    }
  }

  total += countToolSchemaTokens(
    req.tools,
    (tool) => tool.function.name,
    (tool) => tool.function.description,
    (tool) => tool.function.parameters,
  );

  total += req.messages.length * 4;
  return total;
}
