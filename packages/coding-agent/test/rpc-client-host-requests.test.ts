import type { ChildProcess } from "node:child_process";
import * as crypto from "node:crypto";
import { EventEmitter } from "node:events";
import { PassThrough } from "node:stream";
import { describe, expect, it, vi } from "vitest";
import {
	RPC_HOST_MAX_ERROR_BYTES,
	RPC_HOST_MAX_LINE_BYTES,
	RPC_HOST_MAX_PENDING_REQUESTS,
} from "../src/modes/rpc/extension-host-transport.ts";
import { RpcClient } from "../src/modes/rpc/rpc-client.ts";
import type { RpcExtensionHostResponse } from "../src/modes/rpc/rpc-types.ts";

interface RpcClientPrivate {
	process: ChildProcess | null;
	handleLine(line: string): void;
	handleHostRequest(value: unknown, lineBytes: number): Promise<void>;
}

function createFakeProcess(client: RpcClient): { responses: RpcExtensionHostResponse[] } {
	const stdin = new PassThrough();
	const child = Object.assign(new EventEmitter(), {
		stdin,
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
	return { responses };
}

function hostRequest(id: string, service: string, payload: unknown): string {
	return JSON.stringify({ type: "extension_host_request", id, service, payload });
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

	it("returns bounded failures for unknown services, malformed requests, handler errors, and oversized results", async () => {
		const client = new RpcClient({
			hostRequestHandlers: {
				failure: () => {
					throw new Error("x".repeat(RPC_HOST_MAX_ERROR_BYTES * 2));
				},
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
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "oversized", null));

		await vi.waitFor(() => expect(responses).toHaveLength(4));
		expect(responses.every((response) => !response.success)).toBe(true);
		const errors = responses.map((response) => (response.success ? "" : response.error));
		expect(errors[0]).toContain("No host handler registered");
		expect(errors[1]).toBe("Malformed extension host request");
		expect(Buffer.byteLength(errors[2])).toBe(RPC_HOST_MAX_ERROR_BYTES);
		expect(errors[3]).toContain("response exceeds");
	});

	it("ignores duplicate ids and enforces the active request bound", async () => {
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
		const firstId = crypto.randomUUID();
		privateClient.handleLine(hostRequest(firstId, "hold", null));
		privateClient.handleLine(hostRequest(firstId, "hold", null));

		for (let index = 1; index < RPC_HOST_MAX_PENDING_REQUESTS; index++) {
			privateClient.handleLine(hostRequest(crypto.randomUUID(), "hold", null));
		}
		await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(RPC_HOST_MAX_PENDING_REQUESTS));
		privateClient.handleLine(hostRequest(crypto.randomUUID(), "hold", null));
		await vi.waitFor(() => expect(responses).toHaveLength(1));
		expect(responses[0]).toMatchObject({ success: false, error: expect.stringContaining("Too many active") });

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
		expect(responses[0]).toMatchObject({ success: false, error: expect.stringContaining("request exceeds") });
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
