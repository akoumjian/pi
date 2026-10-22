import { type ChildProcess, execFileSync, spawn } from "node:child_process";
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

function findTaggedProcesses(tag: string): number[] {
	const output = execFileSync("ps", ["-eo", "pid=,args="], { encoding: "utf8" });
	return output
		.split("\n")
		.filter((line) => line.includes(tag))
		.map((line) => Number.parseInt(line.trim().split(/\s+/, 1)[0] ?? "", 10))
		.filter((pid) => Number.isSafeInteger(pid) && pid > 1 && pid !== process.pid);
}

function killTaggedProcesses(tag: string): void {
	for (const pid of findTaggedProcesses(tag)) {
		try {
			process.kill(-pid, "SIGKILL");
		} catch {
			try {
				process.kill(pid, "SIGKILL");
			} catch {
				// Already exited.
			}
		}
	}
}

function isProcessAlive(pid: number): boolean {
	try {
		const state = execFileSync("ps", ["-o", "stat=", "-p", String(pid)], { encoding: "utf8" }).trim();
		return state.length > 0 && !state.startsWith("Z");
	} catch {
		return false;
	}
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
		`import { appendFileSync, existsSync, readFileSync } from "node:fs";
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
export default function (pi: any) {
  pi.on("session_start", async (_event: any, ctx: any) => {
    const marker = process.env.STARTUP_MARKER;
    if (marker) appendFileSync(marker, "start\\n");
    if (process.env.STARTUP_REQUEST_HOST === "1") {
      await pi.requestHost("startup.check", { version: 1 });
    }
    if (process.env.STARTUP_SHUTDOWN === "1") ctx.shutdown();
    await delay(Number(process.env.START_DELAY || 0));
    if (marker) appendFileSync(marker, "done\\n");
  });
  pi.on("user_bash", async (event: any, ctx: any) => {
    if (event.command.includes("reentrant-probe")) {
      await pi.requestHost("test.reentrant", { command: event.command });
    }
    if (event.command.includes("shutdown-hook")) {
      const marker = process.env.HOOK_MARKER;
      if (marker) appendFileSync(marker, "start\\n");
      ctx.shutdown();
      await delay(100);
      if (marker) appendFileSync(marker, "done\\n");
    }
  });
  pi.registerCommand("shutdown-probe", {
    description: "Request graceful RPC shutdown",
    handler: async (_args: string, ctx: any) => {
      const marker = process.env.COMMAND_MARKER;
      if (marker) appendFileSync(marker, "start\\n");
      ctx.shutdown();
      await delay(100);
      if (marker) appendFileSync(marker, "done\\n");
    },
  });
  pi.on("session_shutdown", async () => {
    const marker = process.env.SHUTDOWN_MARKER;
    if (!marker) return;
    const prerequisite = process.env.SHUTDOWN_PREREQUISITE;
    if (prerequisite) {
      const completed = existsSync(prerequisite) && readFileSync(prerequisite, "utf8").includes("done\\n");
      appendFileSync(marker, completed ? "after-response\\n" : "before-response\\n");
    }
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
		let trackedTags: string[];

		beforeEach(() => {
			root = mkdtempSync(join(tmpdir(), "pi-rpc-lifecycle-"));
			cwd = join(root, "project");
			agentDir = join(root, "agent");
			startupMarker = join(root, "startup.log");
			shutdownMarker = join(root, "shutdown.log");
			mkdirSync(cwd, { recursive: true });
			mkdirSync(agentDir, { recursive: true });
			writeProbeExtension(agentDir);
			trackedTags = [];
		});

		afterEach(async () => {
			if (client) await client.stop();
			if (rawChild && rawChild.exitCode === null && rawChild.signalCode === null) rawChild.kill("SIGKILL");
			for (const tag of trackedTags) killTaggedProcesses(tag);
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

		it("completes real hook work and its bash response before graceful extension shutdown", async () => {
			const hookMarker = join(root, "hook.log");
			client = new RpcClient({
				cliPath: builtCli,
				cwd,
				env: {
					PI_CODING_AGENT_DIR: agentDir,
					HOOK_MARKER: hookMarker,
					SHUTDOWN_MARKER: shutdownMarker,
					SHUTDOWN_PREREQUISITE: hookMarker,
				},
			});
			await client.start();

			const result = await withTimeout(
				client.bash("printf hook-started; sleep 0.2; printf hook-completed # shutdown-hook"),
				10_000,
				"hook-triggered bash response was lost",
			);
			expect(result).toMatchObject({ cancelled: false });
			expect(result.output).toContain("hook-startedhook-completed");
			await withTimeout(client.stop(), 10_000, "graceful hook shutdown did not exit");
			client = undefined;
			expect(readFileSync(hookMarker, "utf8")).toBe("start\ndone\n");
			expect(readFileSync(shutdownMarker, "utf8")).toBe("after-response\nstart\ndone\n");
		}, 180_000);

		it("completes a real extension command response before graceful shutdown", async () => {
			const commandMarker = join(root, "command.log");
			client = new RpcClient({
				cliPath: builtCli,
				cwd,
				env: {
					PI_CODING_AGENT_DIR: agentDir,
					COMMAND_MARKER: commandMarker,
					SHUTDOWN_MARKER: shutdownMarker,
					SHUTDOWN_PREREQUISITE: commandMarker,
				},
			});
			await client.start();

			await withTimeout(client.prompt("/shutdown-probe"), 10_000, "extension command response was lost");
			await withTimeout(client.stop(), 10_000, "graceful command shutdown did not exit");
			client = undefined;
			expect(readFileSync(commandMarker, "utf8")).toBe("start\ndone\n");
			expect(readFileSync(shutdownMarker, "utf8")).toBe("after-response\nstart\ndone\n");
		}, 180_000);

		it("uses nonzero shared cleanup when the stdout reader is destroyed", async () => {
			const tag = `pi-rpc-stdout-loss-${Date.now()}-${Math.random().toString(36).slice(2)}`;
			trackedTags.push(tag);
			const bashMarker = join(root, "stdout-loss-bash-started");
			const raw = spawnRaw(cwd, {
				PI_CODING_AGENT_DIR: agentDir,
				SHUTDOWN_MARKER: shutdownMarker,
			});
			rawChild = raw.child;
			await vi.waitFor(() => expect(raw.stdout()).toContain('"type":"rpc_ready"'), {
				timeout: 60_000,
				interval: 20,
			});
			raw.child.stdin!.write(
				`${JSON.stringify({ type: "bash", id: "silent", command: `touch ${shellQuote(bashMarker)}; exec -a ${tag} sleep 60` })}\n`,
			);
			await waitForFile(bashMarker);
			raw.child.stdout!.destroy();
			raw.child.stdin!.write(`${JSON.stringify({ type: "get_state", id: "break-output" })}\n`);

			const exit = await withTimeout(raw.exited, 10_000, `stdout-loss shutdown stalled; stderr: ${raw.stderr()}`);
			expect(exit).toEqual({ code: 1, signal: null });
			expect(readFileSync(shutdownMarker, "utf8")).toBe("start\ndone\n");
			await vi.waitFor(() => expect(findTaggedProcesses(tag)).toEqual([]), { timeout: 5000, interval: 25 });
			rawChild = undefined;
		}, 180_000);

		it("cleans silent and streaming sleeps when their real RPC host crashes", async () => {
			const silentTag = `pi-rpc-host-crash-silent-${Date.now()}-${Math.random().toString(36).slice(2)}`;
			const streamingTag = `pi-rpc-host-crash-stream-${Date.now()}-${Math.random().toString(36).slice(2)}`;
			trackedTags.push(silentTag, streamingTag);
			const piPidPath = join(root, "pi.pid");
			const silentMarker = join(root, "host-silent-started");
			const streamingMarker = join(root, "host-streaming-started");
			const hostScript = join(root, "host.mjs");
			writeFileSync(
				hostScript,
				`import { spawn } from "node:child_process";
import { writeFileSync } from "node:fs";
const [cli, cwd, agentDir, pidPath, shutdownMarker, silentMarker, streamingMarker, silentTag, streamingTag] = process.argv.slice(2);
const child = spawn(process.execPath, [cli, "--mode", "rpc"], {
  cwd,
  env: { ...process.env, PI_CODING_AGENT_DIR: agentDir, SHUTDOWN_MARKER: shutdownMarker },
  stdio: ["pipe", "pipe", "pipe"],
});
writeFileSync(pidPath, String(child.pid));
let buffer = "";
let started = false;
child.stdout.setEncoding("utf8");
child.stdout.on("data", (chunk) => {
  buffer += chunk;
  while (buffer.includes("\\n")) {
    const index = buffer.indexOf("\\n");
    const line = buffer.slice(0, index);
    buffer = buffer.slice(index + 1);
    let record;
    try { record = JSON.parse(line); } catch { continue; }
    if (!started && record?.type === "rpc_ready" && Object.keys(record).length === 1) {
      started = true;
      child.stdin.write(JSON.stringify({ type: "bash", id: "silent", command: "touch " + JSON.stringify(silentMarker) + "; exec -a " + silentTag + " sleep 60" }) + "\\n");
      child.stdin.write(JSON.stringify({ type: "bash", id: "streaming", command: "touch " + JSON.stringify(streamingMarker) + "; exec -a " + streamingTag + " bash -c 'while true; do printf stream; sleep 0.02; done'" }) + "\\n");
    }
  }
});
child.stderr.resume();
child.on("exit", () => process.exit(0));
`,
			);
			const host = spawn(
				process.execPath,
				[
					hostScript,
					builtCli,
					cwd,
					agentDir,
					piPidPath,
					shutdownMarker,
					silentMarker,
					streamingMarker,
					silentTag,
					streamingTag,
				],
				{ stdio: "ignore" },
			);
			rawChild = host;
			await waitForFile(silentMarker);
			await waitForFile(streamingMarker);
			const piPid = Number.parseInt(readFileSync(piPidPath, "utf8"), 10);
			expect(Number.isSafeInteger(piPid)).toBe(true);
			expect(host.kill("SIGKILL")).toBe(true);
			await new Promise<void>((resolve) => host.once("exit", () => resolve()));
			rawChild = undefined;

			await waitForFileText(shutdownMarker, "done\n", 10_000);
			expect(readFileSync(shutdownMarker, "utf8")).toBe("start\ndone\n");
			await vi.waitFor(() => expect(isProcessAlive(piPid)).toBe(false), { timeout: 10_000, interval: 25 });
			await vi.waitFor(
				() => expect([...findTaggedProcesses(silentTag), ...findTaggedProcesses(streamingTag)]).toEqual([]),
				{ timeout: 5000, interval: 25 },
			);
		}, 180_000);
	},
);
