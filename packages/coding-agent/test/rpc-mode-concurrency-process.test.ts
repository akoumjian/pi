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

async function waitForFile(path: string, timeoutMs = 60_000): Promise<void> {
	await vi.waitFor(() => expect(existsSync(path)).toBe(true), { timeout: timeoutMs, interval: 20 });
}

async function waitForFileText(path: string, text: string, timeoutMs = 60_000): Promise<void> {
	await vi.waitFor(() => expect(existsSync(path) ? readFileSync(path, "utf8") : "").toContain(text), {
		timeout: timeoutMs,
		interval: 20,
	});
}

function writeProbeExtension(agentDirectory: string): void {
	const extensionDir = join(agentDirectory, "extensions");
	mkdirSync(extensionDir, { recursive: true });
	writeFileSync(
		join(extensionDir, "rpc-lifecycle-probe.ts"),
		`import { appendFileSync } from "node:fs";
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
export default function (pi: any) {
  pi.on("session_start", async () => {
    const marker = process.env.STARTUP_MARKER;
    if (marker) appendFileSync(marker, "start\\n");
    if (process.env.STARTUP_REQUEST_HOST === "1") {
      await pi.requestHost("startup.check", { version: 1 });
    }
    await delay(Number(process.env.START_DELAY || 0));
    if (marker) appendFileSync(marker, "done\\n");
  });
  pi.on("user_bash", async (event: any) => {
    if (!event.command.includes("reentrant-probe")) return;
    await pi.requestHost("test.reentrant", { command: event.command });
  });
  pi.on("session_shutdown", async () => {
    const marker = process.env.SHUTDOWN_MARKER;
    if (!marker) return;
    appendFileSync(marker, "start\\n");
    await delay(Number(process.env.SHUTDOWN_DELAY || 200));
    appendFileSync(marker, "done\\n");
  });
}
`,
	);
}

interface RawProcess {
	child: ChildProcess;
	stdout: () => string;
	stderr: () => string;
	exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

function spawnRaw(cwd: string, env: NodeJS.ProcessEnv): RawProcess {
	let stdout = "";
	let stderr = "";
	const child = spawn(process.execPath, [builtCli, "--mode", "rpc"], {
		cwd,
		env: { ...process.env, ...env },
		stdio: ["pipe", "pipe", "pipe"],
	});
	child.stdout!.on("data", (chunk) => {
		stdout += chunk.toString();
	});
	child.stderr!.on("data", (chunk) => {
		stderr += chunk.toString();
	});
	const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
		child.once("exit", (code, signal) => resolve({ code, signal }));
	});
	return { child, stdout: () => stdout, stderr: () => stderr, exited };
}

describe.skipIf(process.platform === "win32" || !existsSync(builtCli))(
	"RPC cancellation-first real-process lifecycle",
	() => {
		let root: string;
		let cwd: string;
		let agentDir: string;
		let startupMarker: string;
		let shutdownMarker: string;
		let client: RpcClient | undefined;
		let rawChild: ChildProcess | undefined;

		beforeEach(() => {
			root = mkdtempSync(join(tmpdir(), "pi-rpc-lifecycle-"));
			cwd = join(root, "project");
			agentDir = join(root, "agent");
			startupMarker = join(root, "startup.log");
			shutdownMarker = join(root, "shutdown.log");
			mkdirSync(cwd, { recursive: true });
			mkdirSync(agentDir, { recursive: true });
			writeProbeExtension(agentDir);
		});

		afterEach(async () => {
			if (client) await client.stop();
			if (rawChild && rawChild.exitCode === null && rawChild.signalCode === null) rawChild.kill("SIGKILL");
			rmSync(root, { recursive: true, force: true });
		});

		it("waits for session_start host work before ready, then preserves concurrent bash control", async () => {
			let callbackStateSeen = false;
			const startupHandler = vi.fn(async () => ({ compatible: true }));
			client = new RpcClient({
				cliPath: builtCli,
				cwd,
				env: {
					PI_CODING_AGENT_DIR: agentDir,
					STARTUP_MARKER: startupMarker,
					SHUTDOWN_MARKER: shutdownMarker,
					STARTUP_REQUEST_HOST: "1",
				},
				hostRequestHandlers: {
					"startup.check": startupHandler,
					"test.reentrant": async () => {
						const state = await client!.getState();
						callbackStateSeen = state.sessionId.length > 0;
						return { accepted: true };
					},
				},
			});
			await client.start();
			expect(startupHandler).toHaveBeenCalledOnce();
			expect(readFileSync(startupMarker, "utf8")).toBe("start\ndone\n");

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
			await withTimeout(client.getState(), 2000, "get_state blocked behind bash");
			await withTimeout(client.abortBash(), 2000, "abort_bash blocked behind bash");
			await expect(withTimeout(longBash, 3000, "bash did not settle after abort_bash")).resolves.toMatchObject({
				cancelled: true,
			});

			const stopMarker = join(root, "stop-bash-started");
			const bashDuringStop = client.bash(`touch ${shellQuote(stopMarker)} && sleep 30`);
			const bashDuringStopSettled = Promise.allSettled([bashDuringStop]);
			await waitForFile(stopMarker);
			await withTimeout(client.stop(), 4000, "stop escalated instead of interrupting ready-session bash");
			await bashDuringStopSettled;
			client = undefined;
			expect(readFileSync(shutdownMarker, "utf8")).toBe("start\ndone\n");
		}, 120_000);

		it("rejects a final unterminated pre-ready command and immediately closes on EOF", async () => {
			const sideEffect = join(root, "must-not-exist");
			const raw = spawnRaw(cwd, {
				PI_CODING_AGENT_DIR: agentDir,
				STARTUP_MARKER: startupMarker,
				SHUTDOWN_MARKER: shutdownMarker,
				START_DELAY: "3000",
			});
			rawChild = raw.child;
			await waitForFileText(startupMarker, "start\n");
			raw.child.stdin!.end(
				JSON.stringify({ type: "bash", id: "pre-eof", command: `touch ${shellQuote(sideEffect)} && sleep 30` }),
			);

			const exit = await withTimeout(raw.exited, 10_000, `EOF shutdown stalled; stderr: ${raw.stderr()}`);
			expect(exit).toEqual({ code: 0, signal: null });
			expect(raw.stdout()).toContain('"id":"pre-eof"');
			expect(raw.stdout()).toContain('"error":"RPC session is not ready"');
			expect(raw.stdout()).not.toContain('"type":"rpc_ready"');
			expect(existsSync(sideEffect)).toBe(false);
			expect(readFileSync(shutdownMarker, "utf8")).toBe("start\ndone\n");
			rawChild = undefined;
		}, 120_000);

		it("admits no work and emits no ready when SIGTERM closes slow startup", async () => {
			const sideEffect = join(root, "must-not-exist");
			const raw = spawnRaw(cwd, {
				PI_CODING_AGENT_DIR: agentDir,
				STARTUP_MARKER: startupMarker,
				SHUTDOWN_MARKER: shutdownMarker,
				START_DELAY: "3000",
			});
			rawChild = raw.child;
			await waitForFileText(startupMarker, "start\n");
			raw.child.stdin!.write(
				`${JSON.stringify({ type: "bash", id: "pre-term", command: `touch ${shellQuote(sideEffect)} && sleep 30` })}\n`,
			);
			await vi.waitFor(() => expect(raw.stdout()).toContain('"id":"pre-term"'), { timeout: 5000, interval: 20 });
			expect(raw.child.kill("SIGTERM")).toBe(true);

			const exit = await withTimeout(raw.exited, 10_000, `SIGTERM shutdown stalled; stderr: ${raw.stderr()}`);
			expect(exit).toEqual({ code: 143, signal: null });
			expect(raw.stdout()).toContain('"error":"RPC session is not ready"');
			expect(raw.stdout()).not.toContain('"type":"rpc_ready"');
			expect(existsSync(sideEffect)).toBe(false);
			expect(readFileSync(shutdownMarker, "utf8")).toBe("start\ndone\n");
			rawChild = undefined;
		}, 120_000);

		it("rejects overlapping start and pre-ready work when stop closes startup", async () => {
			const sideEffect = join(root, "must-not-exist");
			client = new RpcClient({
				cliPath: builtCli,
				cwd,
				env: {
					PI_CODING_AGENT_DIR: agentDir,
					STARTUP_MARKER: startupMarker,
					SHUTDOWN_MARKER: shutdownMarker,
					START_DELAY: "3000",
				},
			});
			const starting = client.start();
			const startResult = starting.then(
				() => "ready",
				(error: Error) => error.message,
			);
			await waitForFileText(startupMarker, "start\n");
			const preReadyBash = client.bash(`touch ${shellQuote(sideEffect)} && sleep 30`);
			await expect(preReadyBash).rejects.toThrow("RPC session is not ready");

			await withTimeout(client.stop(), 5000, "stop stalled during startup");
			expect(await startResult).toBe("RPC client stopped");
			expect(existsSync(sideEffect)).toBe(false);
			expect(readFileSync(shutdownMarker, "utf8")).toBe("start\ndone\n");
			client = undefined;
		}, 120_000);
	},
);
