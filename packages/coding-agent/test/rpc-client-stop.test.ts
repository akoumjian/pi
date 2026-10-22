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

	it("escalates to SIGTERM after five seconds and SIGKILL one second later", async () => {
		vi.useFakeTimers();
		const client = new RpcClient();
		const stdin = new PassThrough();
		const child = Object.assign(new EventEmitter(), {
			stdin,
			exitCode: null,
			signalCode: null,
			kill: vi.fn((signal?: NodeJS.Signals | number) => {
				if (signal === "SIGKILL") queueMicrotask(() => child.emit("exit", null, "SIGKILL"));
				return true;
			}),
		}) as unknown as ChildProcess;
		(client as unknown as { process: ChildProcess | null }).process = child;
		const end = vi.spyOn(stdin, "end");

		const stopping = client.stop();
		expect(end).toHaveBeenCalledOnce();
		await vi.advanceTimersByTimeAsync(4999);
		expect(child.kill).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(1);
		expect(child.kill).toHaveBeenNthCalledWith(1, "SIGTERM");
		await vi.advanceTimersByTimeAsync(1000);
		expect(child.kill).toHaveBeenNthCalledWith(2, "SIGKILL");
		await stopping;
	});
});
