import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { clearCursorAuth, cursorAuthLocation, loadCursorAuth, saveCursorAuth } from "./token-store.ts";
import { parseJwtClaims, tokenExpiryMs } from "./jwt.ts";
import { jwt } from "../cursor-test-helpers.ts";

describe("Cursor auth token discovery", () => {
  const originalConfigDir = process.env.CCP_CONFIG_DIR;
  const tempDirs: string[] = [];

  afterEach(() => {
    restoreEnv("CCP_CONFIG_DIR", originalConfigDir);
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });

  it("loads CCP_CURSOR_AUTH_TOKEN without reading Cursor storage", async () => {
    const token = jwt({ sub: "user_1", email: "user@example.com", exp: 2_000_000_000 });

    const auth = await loadCursorAuth({ CCP_CURSOR_AUTH_TOKEN: token });

    expect(auth?.accessToken).toBe(token);
    expect(auth?.source).toBe("environment");
    expect(auth?.userId).toBe("user_1");
    expect(auth?.email).toBe("user@example.com");
    expect(auth?.expires).toBe(2_000_000_000_000);
  });

  it("parses JWT claims and expiration", () => {
    const token = jwt({ sub: "user_2", exp: 123 });

    expect(parseJwtClaims(token)?.sub).toBe("user_2");
    expect(tokenExpiryMs(token)).toBe(123_000);
  });

  it("uses CCP_CONFIG_DIR for Cursor auth storage", async () => {
    const dir = tempConfigDir(tempDirs);
    process.env.CCP_CONFIG_DIR = dir;
    const token = jwt({ sub: "user_3", email: "cursor@example.com", exp: 2_000_000_000 });

    const saved = await saveCursorAuth({ accessToken: token, refreshToken: "refresh-token" });
    const raw = await readFile(join(dir, "cursor", "auth.json"), "utf8");
    const loaded = await loadCursorAuth({});
    await clearCursorAuth();

    expect(saved.source).toBe(join(dir, "cursor", "auth.json"));
    expect(JSON.parse(raw)).toEqual({ accessToken: token, refreshToken: "refresh-token" });
    expect(loaded?.accessToken).toBe(token);
    expect(loaded?.source).toBe(join(dir, "cursor", "auth.json"));
    expect(cursorAuthLocation()).toBe(join(dir, "cursor", "auth.json"));
    await expect(readFile(join(dir, "cursor", "auth.json"), "utf8")).rejects.toThrow();
  });
});

function restoreEnv(key: string, value: string | undefined): void {
  if (value === undefined) delete process.env[key];
  else process.env[key] = value;
}

function tempConfigDir(tempDirs: string[]): string {
  const dir = mkdtempSync(join(tmpdir(), "ccp-cursor-auth-"));
  tempDirs.push(dir);
  return dir;
}
