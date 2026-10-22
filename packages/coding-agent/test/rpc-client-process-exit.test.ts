import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, test } from "vitest";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";

const tempDirs: string[] = [];

function writeChildScript(contents: string): string {
	const dir = mkdtempSync(join(tmpdir(), "pi-rpc-client-exit-"));
	tempDirs.push(dir);
	const path = join(dir, "child.mjs");
	writeFileSync(path, contents);
	return path;
}

afterEach(() => {
	for (const dir of tempDirs.splice(0)) {
		rmSync(dir, { recursive: true, force: true });
	}
});

describe("RpcClient child process failures", () => {
	test("rejects an in-flight request when the child process exits", async () => {
		const client = new RpcClient({
			cliPath: writeChildScript(`
process.stdin.once("data", () => {
	process.exit(43);
});
process.stdin.resume();
`),
		});

		await client.start();

		await expect(client.getCommands()).rejects.toThrow(/Agent process (?:stdout ended|exited)/);
	});

	test("resolves a trailing response without LF before stdout EOF cleanup", async () => {
		const client = new RpcClient({
			cliPath: writeChildScript(`
let input = "";
process.stdin.on("data", (chunk) => {
  input += chunk;
  const newline = input.indexOf("\\n");
  if (newline === -1) return;
  const command = JSON.parse(input.slice(0, newline));
  process.stdout.end(JSON.stringify({
    id: command.id,
    type: "response",
    command: "get_commands",
    success: true,
    data: { commands: [] }
  }));
});
process.stdin.resume();
`),
		});

		await client.start();
		await expect(client.getCommands()).resolves.toEqual([]);
		await client.stop();
	});

	test("deterministically aborts active host handlers when the child exits", async () => {
		for (let iteration = 0; iteration < 10; iteration++) {
			let observeAbort: (() => void) | undefined;
			const aborted = new Promise<void>((resolve) => {
				observeAbort = resolve;
			});
			let observeHandlerStart: (() => void) | undefined;
			const handlerStarted = new Promise<void>((resolve) => {
				observeHandlerStart = resolve;
			});
			const client = new RpcClient({
				cliPath: writeChildScript(`
process.on("SIGTERM", () => process.exit(44));
process.stdout.write(JSON.stringify({
  type: "extension_host_request",
  id: "123e4567-e89b-42d3-a456-426614174000",
  service: "wait",
  payload: null
}) + "\\n");
process.stdin.resume();
`),
				hostRequestHandlers: {
					wait: (_payload, { signal }) =>
						new Promise((resolve) => {
							observeHandlerStart?.();
							signal.addEventListener("abort", () => {
								observeAbort?.();
								resolve(null);
							});
						}),
				},
			});

			await client.start();
			await handlerStarted;
			const child = (
				client as unknown as {
					process: import("node:child_process").ChildProcess;
				}
			).process;
			const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
			const pending = client.getCommands();
			child.kill("SIGTERM");
			await expect(aborted).resolves.toBeUndefined();
			await expect(pending).rejects.toThrow(/Agent process (?:stdout ended|exited)/);
			await exited;
		}
	});
	test("rejects pending work on an RPC stdin error", async () => {
		const client = new RpcClient({
			cliPath: writeChildScript(`
process.stdin.on("end", () => process.exit(0));
process.stdin.resume();
`),
		});
		await client.start();
		const pending = client.getCommands();
		const child = (client as unknown as { process: import("node:child_process").ChildProcess }).process;
		child.stdin!.emit("error", new Error("synthetic stdin failure"));

		await expect(pending).rejects.toThrow("Agent process stdin error: synthetic stdin failure");
		await client.stop();
	});
});
