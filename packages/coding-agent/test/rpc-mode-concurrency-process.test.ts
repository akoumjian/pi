import { type ChildProcess, spawn } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";

const testDir = dirname(fileURLToPath(import.meta.url));
const packageDir = join(testDir, "..");
const builtCli = join(packageDir, "dist", "cli.js");

function shellQuote(value: string): string {
	return `'${value.replaceAll("'", `'"'"'`)}'`;
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
	let timeout: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			promise,
			new Promise<never>((_, reject) => {
				timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
			}),
		]);
	} finally {
		if (timeout) clearTimeout(timeout);
	}
}

async function waitForFile(path: string, timeoutMs = 5000): Promise<void> {
	await vi.waitFor(() => expect(existsSync(path)).toBe(true), { timeout: timeoutMs, interval: 20 });
}

function writeProbeExtension(agentDirectory: string): void {
	const extensionDir = join(agentDirectory, "extensions");
	mkdirSync(extensionDir, { recursive: true });
	writeFileSync(
		join(extensionDir, "rpc-concurrency-probe.ts"),
		`import { appendFileSync } from "node:fs";
export default function (pi: any) {
  pi.on("user_bash", async (event: any) => {
    if (!event.command.includes("reentrant-probe")) return;
    await pi.requestHost("test.reentrant", { command: event.command });
  });
  pi.on("session_shutdown", async () => {
    const marker = process.env.SHUTDOWN_MARKER;
    if (!marker) return;
    appendFileSync(marker, "start\\n");
    await new Promise((resolve) => setTimeout(resolve, 500));
    appendFileSync(marker, "done\\n");
  });
}
`,
	);
}

describe.skipIf(process.platform === "win32" || !existsSync(builtCli))(
	"RPC mode concurrent real-process lifecycle",
	() => {
		let root: string;
		let cwd: string;
		let agentDir: string;
		let wrapper: string;
		let shutdownMarker: string;
		let client: RpcClient | undefined;
		let rawChild: ChildProcess | undefined;

		beforeEach(() => {
			root = mkdtempSync(join(tmpdir(), "pi-rpc-concurrency-"));
			cwd = join(root, "project");
			agentDir = join(root, "agent");
			shutdownMarker = join(root, "shutdown.log");
			mkdirSync(cwd, { recursive: true });
			mkdirSync(agentDir, { recursive: true });
			wrapper = builtCli;
			writeProbeExtension(agentDir);
		});

		afterEach(async () => {
			if (client) await client.stop();
			if (rawChild && rawChild.exitCode === null && rawChild.signalCode === null) rawChild.kill("SIGKILL");
			rmSync(root, { recursive: true, force: true });
		});

		it("serves a reentrant host callback, get_state, and abort_bash during bash, then stops during bash", async () => {
			let callbackStateSeen = false;
			client = new RpcClient({
				cliPath: wrapper,
				cwd,
				env: { PI_CODING_AGENT_DIR: agentDir, SHUTDOWN_MARKER: shutdownMarker },
				hostRequestHandlers: {
					"test.reentrant": async () => {
						const state = await client!.getState();
						callbackStateSeen = state.sessionId.length > 0;
						return { accepted: true };
					},
				},
			});
			await client.start();
			await withTimeout(client.getState(), 30_000, "RPC process did not finish startup");

			const reentrant = await withTimeout(
				client.bash("printf reentrant-probe"),
				10_000,
				"reentrant host callback blocked behind bash",
			);
			expect(reentrant.output).toContain("reentrant-probe");
			expect(callbackStateSeen).toBe(true);

			const abortMarker = join(root, "abort-bash-started");
			const longBash = client.bash(`touch ${shellQuote(abortMarker)} && sleep 30`);
			await waitForFile(abortMarker);
			const stateStartedAt = Date.now();
			await withTimeout(client.getState(), 1500, "get_state blocked behind bash");
			expect(Date.now() - stateStartedAt).toBeLessThan(1500);
			const abortStartedAt = Date.now();
			await withTimeout(client.abortBash(), 1500, "abort_bash blocked behind bash");
			expect(Date.now() - abortStartedAt).toBeLessThan(1500);
			await expect(withTimeout(longBash, 3000, "bash did not settle after abort_bash")).resolves.toMatchObject({
				cancelled: true,
			});

			const stopMarker = join(root, "stop-bash-started");
			const bashDuringStop = client.bash(`touch ${shellQuote(stopMarker)} && sleep 30`);
			const bashDuringStopSettled = Promise.allSettled([bashDuringStop]);
			await waitForFile(stopMarker);
			const stopStartedAt = Date.now();
			await withTimeout(
				client.stop(),
				4000,
				"RpcClient.stop() escalated instead of completing graceful EOF shutdown",
			);
			expect(Date.now() - stopStartedAt).toBeLessThan(4000);
			await bashDuringStopSettled;
			client = undefined;
			expect(readFileSync(shutdownMarker, "utf8")).toContain("start\ndone\n");
		}, 60_000);

		it("joins post-startup EOF and SIGTERM until session_shutdown completes, then exits 143", async () => {
			writeFileSync(shutdownMarker, "");
			let stdout = "";
			let stderr = "";
			rawChild = spawn(process.execPath, [wrapper, "--mode", "rpc"], {
				cwd,
				env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, SHUTDOWN_MARKER: shutdownMarker },
				stdio: ["pipe", "pipe", "pipe"],
			});
			rawChild.stdout!.on("data", (chunk) => {
				stdout += chunk.toString();
			});
			rawChild.stderr!.on("data", (chunk) => {
				stderr += chunk.toString();
			});
			rawChild.stdin!.write('{"type":"get_state","id":"warm"}\n');
			try {
				await vi.waitFor(() => expect(stdout).toContain('"id":"warm"'), { timeout: 30_000, interval: 20 });
			} catch (error) {
				throw new Error(
					`RPC process did not finish startup: ${error instanceof Error ? error.message : String(error)}; stderr: ${stderr}`,
				);
			}

			const bashMarker = join(root, "signal-bash-started");
			rawChild.stdin!.write(
				`${JSON.stringify({ type: "bash", id: "bash", command: `touch ${shellQuote(bashMarker)} && sleep 30` })}\n`,
			);
			await waitForFile(bashMarker);
			rawChild.stdin!.end();
			await vi.waitFor(() => expect(readFileSync(shutdownMarker, "utf8")).toContain("start\n"), {
				timeout: 5000,
				interval: 20,
			});
			const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
				rawChild!.once("exit", (code, signal) => resolve({ code, signal }));
			});
			expect(rawChild.kill("SIGTERM")).toBe(true);

			const exit = await withTimeout(
				exited,
				5000,
				`RPC child did not exit after graceful shutdown; stderr: ${stderr}`,
			);
			expect(exit).toEqual({ code: 143, signal: null });
			expect(readFileSync(shutdownMarker, "utf8")).toBe("start\ndone\n");
			rawChild = undefined;
		}, 60_000);
	},
);
