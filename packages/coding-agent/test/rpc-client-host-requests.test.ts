import type { ChildProcess } from "node:child_process";
import * as crypto from "node:crypto";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import { RPC_HOST_MAX_LINE_BYTES, RPC_HOST_MAX_PENDING_REQUESTS } from "../src/modes/rpc/extension-host-transport.ts";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";
import type { RpcExtensionHostResponse } from "../src/modes/rpc/rpc-types.ts";

interface RpcClientPrivate {
	process: ChildProcess | null;
	handleLine(line: string): void;
	handleHostRequest(value: unknown, lineBytes: number): Promise<void>;
	onStdoutEnd(childProcess: ChildProcess): void;
}

function createFakeProcess(client: RpcClient): {
	child: ChildProcess;
	responses: RpcExtensionHostResponse[];
} {
	const stdin = new PassThrough();
	const stdout = new PassThrough();
	const child = Object.assign(new EventEmitter(), {
		stdin,
		stdout,
		exitCode: null,
		signalCode: null,
		kill: vi.fn(() => true),
	}) as unknown as ChildProcess;
	(client as unknown as RpcClientPrivate).process = child;

	let buffer = "";
	const responses: RpcExtensionHostResponse[] = [];
	stdin.on("data", (chunk) => {
		buffer += chunk.toString();
		while (buffer.includes("\n")) {
			const newline = buffer.indexOf("\n");
			const line = buffer.slice(0, newline);
			buffer = buffer.slice(newline + 1);
			responses.push(JSON.parse(line) as RpcExtensionHostResponse);
		}
	});
	return { child, responses };
}

function hostRequest(id: string, service: string, payload: unknown): string {
	return JSON.stringify({ type: "extension_host_request", id, service, payload });
}

function nestedArray(depth: number): unknown {
	let value: unknown = null;
	for (let index = 0; index < depth; index++) value = [value];
	return value;
}

function errorOf(response: RpcExtensionHostResponse): string {
	if (response.success) throw new Error("Expected an error response");
	return response.error;
}

function flushAsyncHandler(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}

describe("RpcClient extension host requests", () => {
	it("dispatches concurrent handlers and writes correlated out-of-order responses", async () => {
		const resolvers = new Map<number, (value: { completed: number }) => void>();
		const client = new RpcClient({
			hostRequestHandlers: {
				work: (payload) =>
					new Promise((resolve) => {
						resolvers.set((payload as { order: number }).order, resolve);
					}),
			},
		});
		const { responses } = createFakeProcess(client);
		const firstId = crypto.randomUUID();
		const secondId = crypto.randomUUID();
		const privateClient = client as unknown as RpcClientPrivate;
		privateClient.handleLine(hostRequest(firstId, "work", { order: 1 }));
		privateClient.handleLine(hostRequest(secondId, "work", { order: 2 }));

		await vi.waitFor(() => expect(resolvers.size).toBe(2));
		resolvers.get(2)?.({ completed: 2 });
		await vi.waitFor(() => expect(responses).toHaveLength(1));
		resolvers.get(1)?.({ completed: 1 });
		await vi.waitFor(() => expect(responses).toHaveLength(2));

		expect(responses).toEqual([
			{ type: "extension_host_response", id: secondId, success: true, data: { completed: 2 } },
			{ type: "extension_host_response", id: firstId, success: true, data: { completed: 1 } },
		]);
	});

	it("uses fixed bounded failures without exposing handler or provider text", async () => {
		const secret = "secret-provider-body-with-token";
		const client = new RpcClient({
			hostRequestHandlers: {
				failure: () => {
					throw new Error(secret);
				},
				invalid: () =>
					({
						response: new Proxy(
							{},
							{
								ownKeys: () => {
									throw new Error(secret);
								},
							},
						),
					}) as never,
				oversized: () => "x".repeat(RPC_HOST_MAX_LINE_BYTES),
			},
		});
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "missing", null));
		await privateClient.handleHostRequest(
			{ type: "extension_host_request", id: crypto.randomUUID(), service: "Invalid", payload: null },
			100,
		);
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "failure", null));
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "invalid", null));
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "oversized", null));

		await vi.waitFor(() => expect(responses).toHaveLength(5));
		expect(responses.map(errorOf)).toEqual([
			"Host service unavailable",
			"Malformed extension host request",
			"Host handler failed",
			"Invalid host handler response",
			"Host handler response exceeds maximum size",
		]);
		expect(JSON.stringify(responses)).not.toContain(secret);
	});

	it("rejects strict request-envelope violations without coercion", async () => {
		const handler = vi.fn(() => null);
		const client = new RpcClient({ hostRequestHandlers: { work: handler } });
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		const invalidRequests = [
			{ service: 1, payload: null },
			{ service: "work" },
			{ service: "work", payload: null, extra: true },
			{ service: `work${"x".repeat(128)}`, payload: null },
		];

		for (const request of invalidRequests) {
			privateClient.handleLine(
				JSON.stringify({ type: "extension_host_request", id: crypto.randomUUID(), ...request }),
			);
		}

		await vi.waitFor(() => expect(responses).toHaveLength(invalidRequests.length));
		expect(responses.map(errorOf)).toEqual(invalidRequests.map(() => "Malformed extension host request"));
		expect(handler).not.toHaveBeenCalled();
	});

	it("rejects deeply nested malformed requests without terminating later dispatch", async () => {
		const handler = vi.fn(() => ({ ok: true }));
		const client = new RpcClient({ hostRequestHandlers: { work: handler } });
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		const deepId = crypto.randomUUID();
		const deepLine = `{"type":"extension_host_request","id":"${deepId}","service":"work","payload":${"[".repeat(3000)}null${"]".repeat(3000)}}`;

		expect(() => privateClient.handleLine(deepLine)).not.toThrow();
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "work", null));

		await vi.waitFor(() => expect(responses).toHaveLength(2));
		expect(errorOf(responses[0])).toBe("Malformed extension host request");
		expect(responses[1]).toMatchObject({ success: true, data: { ok: true } });
		expect(handler).toHaveBeenCalledOnce();
	});

	it("rejects a deeply nested result while serializing a shared DAG and later request", async () => {
		const shared = { value: 1 };
		const client = new RpcClient({
			hostRequestHandlers: {
				deep: () => nestedArray(3000) as never,
				shared: () => ({ left: shared, right: shared }),
				valid: () => ({ ok: true }),
			},
		});
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "deep", null));
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "shared", null));
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "valid", null));

		await vi.waitFor(() => expect(responses).toHaveLength(3));
		expect(responses.map((response) => (response.success ? response.data : response.error))).toEqual([
			"Invalid host handler response",
			{ left: { value: 1 }, right: { value: 1 } },
			{ ok: true },
		]);
	});

	it("retires and aborts the original request on a duplicate active id, with exactly one failure", async () => {
		let observedSignal: AbortSignal | undefined;
		const handler = vi.fn(
			(_payload: unknown, { signal }: { signal: AbortSignal }) =>
				new Promise<null>((_resolve, reject) => {
					observedSignal = signal;
					signal.addEventListener("abort", () => reject(signal.reason), { once: true });
				}),
		);
		const client = new RpcClient({ hostRequestHandlers: { hold: handler } });
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		const id = crypto.randomUUID();
		privateClient.handleLine(hostRequest(id, "hold", null));
		await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce());
		privateClient.handleLine(hostRequest(id, "hold", null));

		await vi.waitFor(() => expect(responses).toHaveLength(1));
		expect(observedSignal?.aborted).toBe(true);
		expect(responses[0]).toEqual({
			type: "extension_host_response",
			id,
			success: false,
			error: "Duplicate extension host request id",
		});
		await flushAsyncHandler();
		expect(responses).toHaveLength(1);
		expect(handler).toHaveBeenCalledOnce();
	});

	it("caps actual handler executions after duplicate retirement until ignored aborts settle", async () => {
		const resolvers: Array<() => void> = [];
		let running = 0;
		let maxRunning = 0;
		const handler = vi.fn(
			() =>
				new Promise<null>((resolve) => {
					running++;
					maxRunning = Math.max(maxRunning, running);
					resolvers.push(() => {
						running--;
						resolve(null);
					});
				}),
		);
		const client = new RpcClient({ hostRequestHandlers: { hold: handler } });
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;

		for (let index = 0; index < RPC_HOST_MAX_PENDING_REQUESTS + 4; index++) {
			const id = crypto.randomUUID();
			privateClient.handleLine(hostRequest(id, "hold", null));
			privateClient.handleLine(hostRequest(id, "hold", null));
		}

		await vi.waitFor(() => expect(responses.length).toBeGreaterThanOrEqual(RPC_HOST_MAX_PENDING_REQUESTS + 4));
		expect(handler).toHaveBeenCalledTimes(RPC_HOST_MAX_PENDING_REQUESTS);
		expect(maxRunning).toBe(RPC_HOST_MAX_PENDING_REQUESTS);
		const errors = responses.map(errorOf);
		expect(errors.filter((error) => error === "Duplicate extension host request id")).toHaveLength(
			RPC_HOST_MAX_PENDING_REQUESTS,
		);
		expect(errors.filter((error) => error === "Host request limit exceeded").length).toBeGreaterThanOrEqual(4);

		const responseCount = responses.length;
		for (const resolve of resolvers) resolve();
		await flushAsyncHandler();
		expect(responses).toHaveLength(responseCount);
	});

	it("enforces the active request bound with a stable failure", async () => {
		const resolvers: Array<(value: null) => void> = [];
		const handler = vi.fn(
			() =>
				new Promise<null>((resolve) => {
					resolvers.push(resolve);
				}),
		);
		const client = new RpcClient({ hostRequestHandlers: { hold: handler } });
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		for (let index = 0; index < RPC_HOST_MAX_PENDING_REQUESTS; index++) {
			privateClient.handleLine(hostRequest(crypto.randomUUID(), "hold", null));
		}
		await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(RPC_HOST_MAX_PENDING_REQUESTS));
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "hold", null));
		await vi.waitFor(() => expect(responses).toHaveLength(1));
		expect(errorOf(responses[0])).toBe("Host request limit exceeded");

		for (const resolve of resolvers) resolve(null);
		await vi.waitFor(() => expect(responses).toHaveLength(RPC_HOST_MAX_PENDING_REQUESTS + 1));
	});

	it("rejects an oversized incoming request before invoking its handler", async () => {
		const handler = vi.fn(() => null);
		const client = new RpcClient({ hostRequestHandlers: { work: handler } });
		const { responses } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		await privateClient.handleHostRequest(
			{ type: "extension_host_request", id: crypto.randomUUID(), service: "work", payload: null },
			RPC_HOST_MAX_LINE_BYTES + 1,
		);

		expect(handler).not.toHaveBeenCalled();
		expect(responses).toHaveLength(1);
		expect(errorOf(responses[0])).toBe("Malformed extension host request");
	});

	it("treats stdout EOF as terminal and aborts active handlers", async () => {
		let signal: AbortSignal | undefined;
		const handler = vi.fn(
			(_payload: unknown, context: { signal: AbortSignal }) =>
				new Promise<null>(() => {
					signal = context.signal;
				}),
		);
		const client = new RpcClient({ hostRequestHandlers: { hold: handler } });
		const { child } = createFakeProcess(client);
		const privateClient = client as unknown as RpcClientPrivate;
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "hold", null));
		await vi.waitFor(() => expect(handler).toHaveBeenCalledOnce());

		privateClient.onStdoutEnd(child);

		expect(signal?.aborted).toBe(true);
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "hold", null));
		await flushAsyncHandler();
		expect(handler).toHaveBeenCalledOnce();
	});

	it("never rejects host-request dispatch when response output fails", async () => {
		const client = new RpcClient({ hostRequestHandlers: { work: () => null } });
		const { child } = createFakeProcess(client);
		const stdin = child.stdin;
		if (!stdin) throw new Error("Fake process stdin is missing");
		stdin.write = (() => {
			throw new Error("synthetic output failure");
		}) as typeof stdin.write;
		const id = crypto.randomUUID();

		await expect(
			(client as unknown as RpcClientPrivate).handleHostRequest(
				{ type: "extension_host_request", id, service: "work", payload: null },
				100,
			),
		).resolves.toBeUndefined();
	});

	it("does not start new handlers after shutdown begins", async () => {
		const handler = vi.fn(() => null);
		const client = new RpcClient({ hostRequestHandlers: { work: handler } });
		const { responses } = createFakeProcess(client);
		(client as unknown as { stopping: boolean }).stopping = true;
		(client as unknown as RpcClientPrivate).handleLine(hostRequest(crypto.randomUUID(), "work", null));
		await Promise.resolve();

		expect(handler).not.toHaveBeenCalled();
		expect(responses).toEqual([]);
	});
});
