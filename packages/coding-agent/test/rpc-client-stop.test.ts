import type { ChildProcess } from "node:child_process";
import { EventEmitter } from "node:events";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PassThrough } from "node:stream";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";

const tempDirs: string[] = [];

function writeChildScript(contents: string): { dir: string; path: string } {
	const dir = mkdtempSync(join(tmpdir(), "pi-rpc-client-stop-"));
	tempDirs.push(dir);
	const path = join(dir, "child.mjs");
	writeFileSync(path, contents);
	return { dir, path };
}

afterEach(() => {
	vi.useRealTimers();
	for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe("RpcClient graceful stop", () => {
	it("closes stdin and lets the child exit cleanly before signaling", async () => {
		const { dir, path } = writeChildScript(`
import { writeFileSync } from "node:fs";
process.on("SIGTERM", () => {
  writeFileSync(process.env.MARKER, "sigterm");
  process.exit(143);
});
process.stdin.on("end", () => {
  writeFileSync(process.env.MARKER, "eof");
  setTimeout(() => process.exit(0), 10);
});
process.stdin.resume();
`);
		const marker = join(dir, "shutdown.txt");
		const client = new RpcClient({ cliPath: path, env: { MARKER: marker } });

		await client.start();
		await client.stop();

		expect(readFileSync(marker, "utf8")).toBe("eof");
	});

	it("escalates on schedule but remains pending after SIGKILL until actual exit", async () => {
		vi.useFakeTimers();
		const client = new RpcClient();
		const stdin = new PassThrough();
		const child = Object.assign(new EventEmitter(), {
			stdin,
			exitCode: null,
			signalCode: null,
			kill: vi.fn(() => true),
		}) as unknown as ChildProcess;
		child.on("error", () => {});
		(client as unknown as { process: ChildProcess | null }).process = child;
		const end = vi.spyOn(stdin, "end");
		let settled = false;

		const stopping = client.stop().then(() => {
			settled = true;
		});
		expect(end).toHaveBeenCalledOnce();
		await vi.advanceTimersByTimeAsync(4999);
		expect(child.kill).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(1);
		expect(child.kill).toHaveBeenNthCalledWith(1, "SIGTERM");
		await vi.advanceTimersByTimeAsync(1000);
		expect(child.kill).toHaveBeenNthCalledWith(2, "SIGKILL");
		expect(settled).toBe(false);

		child.emit("error", new Error("synthetic child error"));
		await Promise.resolve();
		expect(settled).toBe(false);

		child.emit("exit", null, "SIGKILL");
		await stopping;
		expect(settled).toBe(true);
	});

	it("writes bounded cancellation failures before closing stdin on explicit stop", async () => {
		let signal: AbortSignal | undefined;
		const client = new RpcClient({
			hostRequestHandlers: {
				hold: (_payload, context) =>
					new Promise<null>(() => {
						signal = context.signal;
					}),
			},
		});
		const stdin = new PassThrough();
		const child = Object.assign(new EventEmitter(), {
			stdin,
			exitCode: null,
			signalCode: null,
			kill: vi.fn(() => true),
		}) as unknown as ChildProcess;
		(client as unknown as { process: ChildProcess | null }).process = child;
		const response = new Promise<Record<string, unknown>>((resolve) => {
			stdin.once("data", (chunk) => resolve(JSON.parse(chunk.toString()) as Record<string, unknown>));
		});
		const id = "123e4567-e89b-42d3-a456-426614174000";
		(client as unknown as { handleLine(line: string): void }).handleLine(
			JSON.stringify({ type: "extension_host_request", id, service: "hold", payload: null }),
		);
		await vi.waitFor(() => expect(signal).toBeDefined());

		const stopping = client.stop();
		await expect(response).resolves.toEqual({
			type: "extension_host_response",
			id,
			success: false,
			error: "Host request cancelled: RPC client stopping",
		});
		expect(signal?.aborted).toBe(true);
		expect(stdin.writableEnded).toBe(true);

		child.emit("exit", 0, null);
		await stopping;
	});
});
