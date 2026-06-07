import type { CliHandlers } from "../types.ts";
import { runDeviceLogin } from "./auth/device.ts";
import { persistInitialTokens } from "./auth/manager.ts";
import { runBrowserLogin } from "./auth/pkce.ts";
import { authPath, clearAuth, loadAuth } from "./auth/token-store.ts";

export const codexCli: CliHandlers = {
  async login() {
    const tokens = await runBrowserLogin();
    const saved = await persistInitialTokens(tokens);
    console.log(`Auth saved in ${authPath()}`);
    if (saved.accountId) console.log(`Account: ${saved.accountId}`);
  },
  async device() {
    const tokens = await runDeviceLogin();
    const saved = await persistInitialTokens(tokens);
    console.log(`Auth saved in ${authPath()}`);
    if (saved.accountId) console.log(`Account: ${saved.accountId}`);
  },
  async status() {
    const auth = await loadAuth();
    if (!auth) {
      console.log("Not authenticated");
      process.exit(1);
    }
    const ms = auth.expires - Date.now();
    console.log(`Account: ${auth.accountId ?? "(none)"}`);
    console.log(`Expires: ${new Date(auth.expires).toISOString()} (in ${Math.floor(ms / 1000)}s)`);
    console.log(`Storage: ${authPath()}`);
  },
  async logout() {
    await clearAuth();
    console.log("Logged out");
  },
};
