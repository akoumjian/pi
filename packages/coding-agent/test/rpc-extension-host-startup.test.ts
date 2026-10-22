import { describe, expect, it, vi } from "vitest";
import type { ExtensionBindings } from "../src/core/agent-session.ts";
import type { AgentSessionRuntime } from "../src/core/agent-session-runtime.ts";
import { runRpcMode } from "../src/modes/rpc/rpc-mode.ts";
import type { RpcExtensionHostRequest } from "../src/modes/rpc/rpc-types.ts";

const rpcIo = vi.hoisted(() => ({
	outputLines: [] as string[],
	lineHandler: undefined as ((line: string) => void) | undefined,
}));

vi.mock("../src/core/output-guard.js", () => ({
	flushRawStdout: vi.fn(async () => {}),
	takeOverStdout: vi.fn(),
	waitForRawStdoutBackpressure: vi.fn(async () => {}),
	writeRawStdout: (line: string) => rpcIo.outputLines.push(line),
}));

vi.mock("../src/modes/interactive/theme/theme.js", () => ({ theme: {} }));

vi.mock("../src/modes/rpc/jsonl.js", () => ({
	attachJsonlLineReader: vi.fn((_stream: NodeJS.ReadableStream, onLine: (line: string) => void) => {
		rpcIo.lineHandler = onLine;
		return () => {};
	}),
	serializeJsonLine: (value: unknown) => `${JSON.stringify(value)}\n`,
}));

describe("RPC extension host requests during startup", () => {
	it("reads a host response while session_start is waiting for it", async () => {
		rpcIo.outputLines = [];
		rpcIo.lineHandler = undefined;
		let startupResult: unknown;
		let finishStartup: (() => void) | undefined;
		const startupFinished = new Promise<void>((resolve) => {
			finishStartup = resolve;
		});
		const session = {
			agent: { subscribe: () => () => {} },
			subscribe: () => () => {},
			bindExtensions: async (bindings: ExtensionBindings) => {
				startupResult = await bindings.hostRequestHandler!("startup.hello", { version: 1 });
				finishStartup?.();
			},
		};
		const runtimeHost = {
			session,
			setRebindSession: vi.fn(),
			dispose: vi.fn(async () => {}),
		} as unknown as AgentSessionRuntime;

		void runRpcMode(runtimeHost);
		await vi.waitFor(() => {
			expect(rpcIo.lineHandler).toBeDefined();
			expect(rpcIo.outputLines).toHaveLength(1);
		});
		const request = JSON.parse(rpcIo.outputLines[0]) as RpcExtensionHostRequest;
		expect(request).toMatchObject({
			type: "extension_host_request",
			service: "startup.hello",
			payload: { version: 1 },
		});

		rpcIo.lineHandler!(
			JSON.stringify({ type: "extension_host_response", id: request.id, success: true, data: { compatible: true } }),
		);
		await startupFinished;
		expect(startupResult).toEqual({ compatible: true });
	});
});
