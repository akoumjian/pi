import * as crypto from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import {
	RPC_HOST_MAX_ERROR_BYTES,
	RPC_HOST_MAX_LINE_BYTES,
	RPC_HOST_MAX_PENDING_REQUESTS,
	RpcExtensionHostTransport,
} from "../src/modes/rpc/extension-host-transport.ts";
import type { RpcExtensionHostRequest } from "../src/modes/rpc/rpc-types.ts";

function requestFrom(line: string): RpcExtensionHostRequest {
	return JSON.parse(line) as RpcExtensionHostRequest;
}

describe("RPC extension host transport", () => {
	it("correlates concurrent responses received out of order", async () => {
		const lines: string[] = [];
		const transport = new RpcExtensionHostTransport((line) => lines.push(line));
		const first = transport.request("alpha.service", { order: 1 });
		const second = transport.request("beta-service", { order: 2 });
		const firstRequest = requestFrom(lines[0]);
		const secondRequest = requestFrom(lines[1]);

		expect(
			transport.handleResponse(
				{ type: "extension_host_response", id: secondRequest.id, success: true, data: { result: "second" } },
				100,
			),
		).toBe(true);
		expect(
			transport.handleResponse(
				{ type: "extension_host_response", id: firstRequest.id, success: true, data: { result: "first" } },
				100,
			),
		).toBe(true);

		await expect(first).resolves.toEqual({ result: "first" });
		await expect(second).resolves.toEqual({ result: "second" });
	});

	it("rejects invalid services, payloads, timeouts, oversized lines, and excess concurrency", async () => {
		const lines: string[] = [];
		const transport = new RpcExtensionHostTransport((line) => lines.push(line));

		await expect(transport.request("Invalid", null)).rejects.toThrow("Invalid host service");
		await expect(transport.request("valid", { bad: undefined } as never)).rejects.toThrow("JSON-compatible");
		await expect(transport.request("valid", null, { timeout: 0 })).rejects.toThrow("integer from 1");
		await expect(transport.request("valid", "x".repeat(RPC_HOST_MAX_LINE_BYTES))).rejects.toThrow("exceeds");

		const pending = Array.from({ length: RPC_HOST_MAX_PENDING_REQUESTS }, (_, index) =>
			transport.request("valid", { index }),
		);
		await expect(transport.request("valid", null)).rejects.toThrow("Too many pending host requests");
		transport.close();
		const settled = await Promise.allSettled(pending);
		expect(settled.every((result) => result.status === "rejected")).toBe(true);
	});

	it("handles timeout and AbortSignal cancellation and ignores late responses", async () => {
		vi.useFakeTimers();
		try {
			const lines: string[] = [];
			const transport = new RpcExtensionHostTransport((line) => lines.push(line));
			const timedOut = transport.request("timeout", null, { timeout: 25 });
			const timedOutId = requestFrom(lines[0]).id;
			const timedOutExpectation = expect(timedOut).rejects.toThrow("timed out after 25 ms");
			await vi.advanceTimersByTimeAsync(25);
			await timedOutExpectation;
			expect(
				transport.handleResponse(
					{ type: "extension_host_response", id: timedOutId, success: true, data: null },
					100,
				),
			).toBe(false);

			const controller = new AbortController();
			const aborted = transport.request("abort", null, { signal: controller.signal });
			const abortedId = requestFrom(lines[1]).id;
			const abortedExpectation = expect(aborted).rejects.toThrow("Host request aborted");
			controller.abort();
			await abortedExpectation;
			expect(
				transport.handleResponse(
					{ type: "extension_host_response", id: abortedId, success: true, data: null },
					100,
				),
			).toBe(false);
		} finally {
			vi.useRealTimers();
		}
	});

	it("rejects malformed and oversized matched responses and ignores unknown or duplicate replies", async () => {
		const lines: string[] = [];
		const transport = new RpcExtensionHostTransport((line) => lines.push(line));
		const malformed = transport.request("malformed", null);
		const malformedId = requestFrom(lines[0]).id;
		expect(transport.handleResponse({ id: malformedId, success: true }, 100)).toBe(true);
		await expect(malformed).rejects.toThrow("Malformed host response data");

		const unknownId = crypto.randomUUID();
		expect(
			transport.handleResponse({ type: "extension_host_response", id: unknownId, success: true, data: null }, 100),
		).toBe(false);

		const successful = transport.request("success", null);
		const successfulId = requestFrom(lines[1]).id;
		expect(
			transport.handleResponse(
				{ type: "extension_host_response", id: successfulId, success: true, data: "ok" },
				100,
			),
		).toBe(true);
		await expect(successful).resolves.toBe("ok");
		expect(
			transport.handleResponse(
				{ type: "extension_host_response", id: successfulId, success: true, data: "duplicate" },
				100,
			),
		).toBe(false);

		const oversized = transport.request("oversized", null);
		const oversizedId = requestFrom(lines[2]).id;
		expect(
			transport.handleResponse(
				{ type: "extension_host_response", id: oversizedId, success: true, data: null },
				RPC_HOST_MAX_LINE_BYTES + 1,
			),
		).toBe(true);
		await expect(oversized).rejects.toThrow("response exceeds");

		const error = transport.request("error", null);
		const errorId = requestFrom(lines[3]).id;
		expect(
			transport.handleResponse(
				{
					type: "extension_host_response",
					id: errorId,
					success: false,
					error: "x".repeat(RPC_HOST_MAX_ERROR_BYTES + 1),
				},
				100,
			),
		).toBe(true);
		await expect(error).rejects.toThrow("error exceeds 2048 bytes");
	});
});
