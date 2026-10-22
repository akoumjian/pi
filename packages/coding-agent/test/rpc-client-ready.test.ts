import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";

const tempDirs: string[] = [];

function writeChildScript(contents: string): { dir: string; path: string } {
	const dir = mkdtempSync(join(tmpdir(), "pi-rpc-client-ready-"));
	tempDirs.push(dir);
	const path = join(dir, "child.mjs");
	writeFileSync(path, contents);
	return { dir, path };
}

afterEach(() => {
	for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("RpcClient ready handshake", () => {
	it("waits for the exact ready message instead of a fixed startup delay", async () => {
		const { dir, path } = writeChildScript(`
import { existsSync, writeFileSync } from "node:fs";
writeFileSync(process.env.STARTED, "started");
const timer = setInterval(() => {
  if (!existsSync(process.env.RELEASE)) return;
  clearInterval(timer);
  process.stdout.write(JSON.stringify({ type: "rpc_ready" }) + "\\n");
}, 10);
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
`);
		const startedMarker = join(dir, "started");
		const releaseMarker = join(dir, "release");
		const client = new RpcClient({
			cliPath: path,
			env: { STARTED: startedMarker, RELEASE: releaseMarker },
		});
		let settled = false;
		const starting = client.start().then(() => {
			settled = true;
		});
		await vi.waitFor(() => expect(existsSync(startedMarker)).toBe(true));
		await new Promise<void>((resolve) => setTimeout(resolve, 150));
		expect(settled).toBe(false);

		writeFileSync(releaseMarker, "ready");
		await starting;
		expect(settled).toBe(true);
		await client.stop();
	});

	it("dispatches extension host requests while start is waiting for ready", async () => {
		const id = "123e4567-e89b-42d3-a456-426614174000";
		const { path } = writeChildScript(`
let input = "";
process.stdout.write(JSON.stringify({
  type: "extension_host_request",
  id: ${JSON.stringify(id)},
  service: "startup.check",
  payload: { version: 1 }
}) + "\\n");
process.stdin.on("data", (chunk) => {
  input += chunk;
  const newline = input.indexOf("\\n");
  if (newline === -1) return;
  const response = JSON.parse(input.slice(0, newline));
  if (response.type !== "extension_host_response" || response.id !== ${JSON.stringify(id)} || !response.success) {
    process.exit(41);
  }
  process.stdout.write(JSON.stringify({ type: "rpc_ready" }) + "\\n");
});
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
`);
		const handler = vi.fn(async () => ({ compatible: true }));
		const client = new RpcClient({
			cliPath: path,
			hostRequestHandlers: { "startup.check": handler },
		});

		await client.start();
		expect(handler).toHaveBeenCalledOnce();
		await client.stop();
	});

	it("rejects start deterministically when stdout ends before ready", async () => {
		const { path } = writeChildScript(`
process.stdout.end();
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
`);
		const client = new RpcClient({ cliPath: path });
		await expect(client.start()).rejects.toThrow("Agent process stdout ended");
		await client.stop();
	});

	it("rejects start on process exit before ready", async () => {
		const { path } = writeChildScript(`process.exit(42);`);
		const client = new RpcClient({ cliPath: path });
		await expect(client.start()).rejects.toThrow(/Agent process (?:stdout ended|exited)/);
		await client.stop();
	});

	it("rejects start on a startup transport error", async () => {
		const { path } = writeChildScript(`
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
`);
		const client = new RpcClient({ cliPath: path });
		const starting = client.start();
		const startRejected = expect(starting).rejects.toThrow("Agent process stdin error: synthetic startup failure");
		const child = (client as unknown as { process: import("node:child_process").ChildProcess }).process;
		child.stdin!.emit("error", new Error("synthetic startup failure"));

		await startRejected;
		await client.stop();
	});

	it("rejects an overlapping start when stop closes startup transport", async () => {
		const { dir, path } = writeChildScript(`
import { writeFileSync } from "node:fs";
writeFileSync(process.env.STARTED, "started");
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
`);
		const startedMarker = join(dir, "started");
		const client = new RpcClient({ cliPath: path, env: { STARTED: startedMarker } });
		const starting = client.start();
		const startRejected = expect(starting).rejects.toThrow("RPC client stopped");
		await vi.waitFor(() => expect(existsSync(startedMarker)).toBe(true));

		await client.stop();
		await startRejected;
	});

	it("does not accept a ready-shaped message with additional properties", async () => {
		const { path } = writeChildScript(`
process.stdout.write(JSON.stringify({ type: "rpc_ready", extra: true }) + "\\n");
process.stdout.end();
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
`);
		const client = new RpcClient({ cliPath: path });
		await expect(client.start()).rejects.toThrow("Agent process stdout ended");
		await client.stop();
	});
});
