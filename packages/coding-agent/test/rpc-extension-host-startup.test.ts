import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import type { ExtensionBindings } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { runRpcMode } from "../src/modes/rpc/rpc-mode.ts";
import type { RpcExtensionHostRequest } from "../src/modes/rpc/rpc-types.ts";

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

describe("RPC extension host requests during startup", () => {
	it("reads a host response while session_start is waiting for it", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		let startupResult: unknown;
		let finishStartup: (() => void) | undefined;
		const startupFinished = new Promise<void>((resolve) => {
			finishStartup = resolve;
		});
		let finishExit: (() => void) | undefined;
		const exited = new Promise<void>((resolve) => {
			finishExit = resolve;
		});
		const session = createStateSession(async (bindings) => {
			startupResult = await bindings.hostRequestHandler!("startup.hello", { version: 1 });
			finishStartup?.();
		});
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => {}),
		} as unknown as AgentSessionRuntime;

		void runRpcMode(runtimeHost, { input, exit: () => finishExit?.() });
		await vi.waitFor(() => expect(rpcIo.outputLines).toHaveLength(1));
		const request = JSON.parse(rpcIo.outputLines[0]) as RpcExtensionHostRequest;
		expect(request).toMatchObject({
			type: "extension_host_request",
			service: "startup.hello",
			payload: { version: 1 },
		});

		input.write(
			`${JSON.stringify({ type: "extension_host_response", id: request.id, success: true, data: { compatible: true } })}\n`,
		);
		await startupFinished;
		expect(startupResult).toEqual({ compatible: true });

		input.end();
		await exited;
		expect(runtimeHost.dispose).toHaveBeenCalledOnce();
	});

	it("rejects startup requests on early EOF, then drains buffered commands before disposal", async () => {
		rpcIo.outputLines = [];
		const input = new PassThrough();
		const lifecycle: string[] = [];
		let hostError: string | undefined;
		let uiError: string | undefined;
		const session = createStateSession(async (bindings) => {
			lifecycle.push("session_start");
			const hostRequest = bindings.hostRequestHandler!("startup.wait", null).catch((error: Error) => {
				hostError = error.message;
			});
			const uiRequest = bindings.uiContext!.input("Startup", "value").catch((error: Error) => {
				uiError = error.message;
			});
			await Promise.all([hostRequest, uiRequest]);
			lifecycle.push("startup_requests_rejected");
			await new Promise<void>((resolve) => setImmediate(resolve));
			lifecycle.push("session_start_done");
		});
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => {
				lifecycle.push("session_shutdown");
			}),
		} as unknown as AgentSessionRuntime;
		let finishExit: (() => void) | undefined;
		const exited = new Promise<void>((resolve) => {
			finishExit = resolve;
		});

		void runRpcMode(runtimeHost, { input, exit: () => finishExit?.() });
		await vi.waitFor(() =>
			expect(
				parseOutput().filter((line) => typeof line.type === "string" && line.type.endsWith("request")),
			).toHaveLength(2),
		);

		input.end('{"type":"get_state","id":"one"}\n{"type":"get_state","id":"two"}');
		await exited;

		expect(hostError).toBe("RPC connection closed");
		expect(uiError).toBe("RPC connection closed");
		expect(lifecycle).toEqual([
			"session_start",
			"startup_requests_rejected",
			"session_start_done",
			"session_shutdown",
		]);
		const responses = parseOutput().filter((line) => line.type === "response");
		expect(responses.map((response) => response.id)).toEqual(["one", "two"]);
		expect(responses.every((response) => response.success === true)).toBe(true);
		expect(runtimeHost.dispose).toHaveBeenCalledOnce();
	});
});
