import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import type { ExtensionBindings } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { runRpcMode } from "../src/modes/rpc/rpc-mode.ts";
import type { RpcExtensionHostRequest, RpcExtensionUIRequest } from "../src/modes/rpc/rpc-types.ts";

const rpcIo = vi.hoisted(() => ({
	outputLines: [] as string[],
}));

vi.mock("../src/core/output-guard.js", () => ({
	flushRawStdout: vi.fn(async () => {}),
	takeOverStdout: vi.fn(),
	waitForRawStdoutBackpressure: vi.fn(async () => {}),
	writeRawStdout: (line: string) => rpcIo.outputLines.push(line),
}));

vi.mock("../src/modes/interactive/theme/theme.js", () => ({ theme: {} }));

function parseOutput(): Array<Record<string, unknown>> {
	return rpcIo.outputLines
		.flatMap((line) => line.split("\n"))
		.filter(Boolean)
		.map((line) => JSON.parse(line) as Record<string, unknown>);
}

function createStateSession(bindExtensions: (bindings: ExtensionBindings) => Promise<void>) {
	return {
		agent: { subscribe: () => () => {} },
		subscribe: () => () => {},
		bindExtensions,
		model: undefined,
		thinkingLevel: "off",
		isStreaming: false,
		isCompacting: false,
		steeringMode: "all",
		followUpMode: "all",
		sessionFile: undefined,
		sessionId: "test-session",
		sessionName: undefined,
		autoCompactionEnabled: true,
		messages: [],
		pendingMessageCount: 0,
	};
}

function startRpcMode(
	runtimeHost: AgentSessionRuntime,
	input: PassThrough,
	signalSource?: EventEmitter,
): { exited: Promise<number>; exitCodes: number[] } {
	const exitCodes: number[] = [];
	let observeExit: ((code: number) => void) | undefined;
	const exited = new Promise<number>((resolve) => {
		observeExit = resolve;
	});
	void runRpcMode(runtimeHost, {
		input,
		signalSource,
		exit: (code) => {
			exitCodes.push(code);
			observeExit?.(code);
		},
	});
	return { exited, exitCodes };
}

async function waitForReady(): Promise<void> {
	await vi.waitFor(() => expect(parseOutput().filter((line) => line.type === "rpc_ready")).toHaveLength(1));
}

describe("RPC startup lifecycle", () => {
	it("emits ready only after session_start and accepts correlated startup host/UI responses", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		const lifecycle: string[] = [];
		let hostResult: unknown;
		let uiResult: unknown;
		const session = createStateSession(async (bindings) => {
			lifecycle.push("session_start");
			[hostResult, uiResult] = await Promise.all([
				bindings.hostRequestHandler!("startup.hello", { version: 1 }),
				bindings.uiContext!.input("Startup", "value"),
			]);
			lifecycle.push("session_start_done");
		});
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => lifecycle.push("session_shutdown")),
		} as unknown as AgentSessionRuntime;
		const { exited } = startRpcMode(runtimeHost, input);

		await vi.waitFor(() =>
			expect(
				parseOutput().filter((line) => typeof line.type === "string" && line.type.endsWith("request")),
			).toHaveLength(2),
		);
		expect(parseOutput().some((line) => line.type === "rpc_ready")).toBe(false);
		input.write('{"type":"get_state","id":"too-early"}\n');
		await vi.waitFor(() => expect(parseOutput().some((line) => line.id === "too-early")).toBe(true));
		const early = parseOutput().find((line) => line.id === "too-early");
		expect(early).toMatchObject({ success: false, error: "RPC session is not ready" });

		const hostRequest = parseOutput().find(
			(line) => line.type === "extension_host_request",
		) as unknown as RpcExtensionHostRequest;
		const uiRequest = parseOutput().find(
			(line) => line.type === "extension_ui_request",
		) as unknown as RpcExtensionUIRequest;
		input.write(
			`${JSON.stringify({ type: "extension_host_response", id: hostRequest.id, success: true, data: { compatible: true } })}\n`,
		);
		input.write(`${JSON.stringify({ type: "extension_ui_response", id: uiRequest.id, value: "accepted" })}\n`);

		await waitForReady();
		expect(lifecycle).toEqual(["session_start", "session_start_done"]);
		expect(hostResult).toEqual({ compatible: true });
		expect(uiResult).toBe("accepted");
		expect(parseOutput().at(-1)).toEqual({ type: "rpc_ready" });

		input.end();
		await exited;
		expect(runtimeHost.dispose).toHaveBeenCalledOnce();
	});

	it("rejects and never executes pre-ready commands, including a final unterminated record, before EOF cleanup", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		const lifecycle: string[] = [];
		let releaseStartup: (() => void) | undefined;
		const startupGate = new Promise<void>((resolve) => {
			releaseStartup = resolve;
		});
		let observeStartupDone: (() => void) | undefined;
		const startupDone = new Promise<void>((resolve) => {
			observeStartupDone = resolve;
		});
		const executeBash = vi.fn();
		const setSessionName = vi.fn();
		const session = Object.assign(
			createStateSession(async () => {
				lifecycle.push("session_start");
				await startupGate;
				lifecycle.push("session_start_done");
				observeStartupDone?.();
			}),
			{ executeBash, setSessionName },
		);
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => {
				lifecycle.push("session_shutdown");
				releaseStartup?.();
				await startupDone;
			}),
		} as unknown as AgentSessionRuntime;
		const { exited } = startRpcMode(runtimeHost, input);
		await vi.waitFor(() => expect(lifecycle).toEqual(["session_start"]));

		input.end(
			'{"type":"bash","id":"bash","command":"touch forbidden"}\n' +
				'{"type":"set_session_name","id":"name","name":"forbidden"}\n' +
				'{"type":"get_state","id":"three-no-lf"}',
		);
		await exited;

		const responses = parseOutput().filter((line) => line.type === "response");
		expect(responses.map((line) => line.id)).toEqual(["bash", "name", "three-no-lf"]);
		expect(responses.every((line) => line.success === false && line.error === "RPC session is not ready")).toBe(true);
		expect(executeBash).not.toHaveBeenCalled();
		expect(setSessionName).not.toHaveBeenCalled();
		expect(parseOutput().some((line) => line.type === "rpc_ready")).toBe(false);
		expect(lifecycle).toEqual(["session_start", "session_shutdown", "session_start_done"]);
		expect(runtimeHost.dispose).toHaveBeenCalledOnce();
	});

	it("a startup SIGTERM executes no pre-ready work, emits no ready, and exits once with 143", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		const signalSource = new EventEmitter();
		let releaseStartup: (() => void) | undefined;
		const startupGate = new Promise<void>((resolve) => {
			releaseStartup = resolve;
		});
		let observeStartupDone: (() => void) | undefined;
		const startupDone = new Promise<void>((resolve) => {
			observeStartupDone = resolve;
		});
		const executeBash = vi.fn();
		const setSessionName = vi.fn();
		const session = Object.assign(
			createStateSession(async () => {
				await startupGate;
				observeStartupDone?.();
			}),
			{ executeBash, setSessionName },
		);
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => {
				releaseStartup?.();
				await startupDone;
			}),
		} as unknown as AgentSessionRuntime;
		const { exited, exitCodes } = startRpcMode(runtimeHost, input, signalSource);
		input.write('{"type":"bash","id":"bash","command":"touch forbidden"}\n');
		input.write('{"type":"set_session_name","id":"name","name":"forbidden"}\n');
		await vi.waitFor(() => expect(parseOutput().filter((line) => line.type === "response")).toHaveLength(2));

		signalSource.emit("SIGTERM");
		await expect(exited).resolves.toBe(143);
		expect(exitCodes).toEqual([143]);
		expect(executeBash).not.toHaveBeenCalled();
		expect(setSessionName).not.toHaveBeenCalled();
		expect(parseOutput().some((line) => line.type === "rpc_ready")).toBe(false);
		expect(runtimeHost.dispose).toHaveBeenCalledOnce();
	});

	it("keeps get_state and abort_bash responsive while a ready-session bash is pending", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		let finishBash: ((result: object) => void) | undefined;
		let observeBashStart: (() => void) | undefined;
		const bashStarted = new Promise<void>((resolve) => {
			observeBashStart = resolve;
		});
		const abortBash = vi.fn(() => {
			finishBash?.({ output: "", exitCode: null, cancelled: true, truncated: false });
		});
		const session = Object.assign(
			createStateSession(async () => {}),
			{
				extensionRunner: { emitUserBash: async () => undefined, getRegisteredCommands: () => [] },
				sessionManager: { getCwd: () => process.cwd() },
				executeBash: () =>
					new Promise<object>((resolve) => {
						finishBash = resolve;
						observeBashStart?.();
					}),
				abortBash,
			},
		);
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => {}),
		} as unknown as AgentSessionRuntime;
		const { exited } = startRpcMode(runtimeHost, input);
		await waitForReady();

		input.write('{"type":"bash","id":"bash","command":"sleep 30"}\n');
		await bashStarted;
		input.write('{"type":"get_state","id":"during"}\n');
		await vi.waitFor(() => expect(parseOutput().some((line) => line.id === "during")).toBe(true));
		expect(parseOutput().some((line) => line.id === "bash")).toBe(false);
		input.write('{"type":"abort_bash","id":"abort"}\n');
		await vi.waitFor(() => expect(parseOutput().some((line) => line.id === "abort")).toBe(true));
		await vi.waitFor(() => expect(parseOutput().some((line) => line.id === "bash")).toBe(true));
		expect(abortBash).toHaveBeenCalledOnce();

		input.end();
		await exited;
	});

	it("starts ready-session EOF disposal without waiting for an in-flight bash", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		let finishBash: ((result: object) => void) | undefined;
		let observeBashStart: (() => void) | undefined;
		const bashStarted = new Promise<void>((resolve) => {
			observeBashStart = resolve;
		});
		const session = Object.assign(
			createStateSession(async () => {}),
			{
				extensionRunner: { emitUserBash: async () => undefined, getRegisteredCommands: () => [] },
				sessionManager: { getCwd: () => process.cwd() },
				executeBash: () =>
					new Promise<object>((resolve) => {
						finishBash = resolve;
						observeBashStart?.();
					}),
				abortBash: vi.fn(),
			},
		);
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => {
				finishBash?.({ output: "", exitCode: null, cancelled: true, truncated: false });
			}),
		} as unknown as AgentSessionRuntime;
		const { exited } = startRpcMode(runtimeHost, input);
		await waitForReady();
		input.write('{"type":"bash","id":"bash","command":"sleep 30"}\n');
		await bashStarted;

		input.end();
		await exited;
		expect(runtimeHost.dispose).toHaveBeenCalledOnce();
	});

	it("joins synchronous and repeated shutdown requests until cleanup completes", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		const signalSource = new EventEmitter();
		let releaseDispose: (() => void) | undefined;
		const disposeReleased = new Promise<void>((resolve) => {
			releaseDispose = resolve;
		});
		const lifecycle: string[] = [];
		const session = createStateSession(async () => {});
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(() => {
				lifecycle.push("shutdown-start");
				signalSource.emit("SIGTERM");
				return disposeReleased.then(() => lifecycle.push("shutdown-done"));
			}),
		} as unknown as AgentSessionRuntime;
		const { exited, exitCodes } = startRpcMode(runtimeHost, input, signalSource);
		await waitForReady();

		input.end();
		signalSource.emit("SIGTERM");
		await new Promise<void>((resolve) => setImmediate(resolve));
		expect(exitCodes).toEqual([]);
		expect(lifecycle).toEqual(["shutdown-start"]);
		releaseDispose?.();
		await exited;
		expect(lifecycle).toEqual(["shutdown-start", "shutdown-done"]);
		expect(exitCodes).toEqual([143]);
		expect(runtimeHost.dispose).toHaveBeenCalledOnce();
	});
});
