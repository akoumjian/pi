import * as crypto from "node:crypto";
import type { JsonValue } from "@earendil-works/pi-ai";
import type { ExtensionHostRequestOptions } from "../../core/extensions/types.ts";
import { serializeJsonLine } from "./jsonl.ts";
import type { RpcExtensionHostRequest, RpcExtensionHostResponse } from "./rpc-types.ts";

export const RPC_HOST_MAX_LINE_BYTES = 256 * 1024;
export const RPC_HOST_MAX_PENDING_REQUESTS = 16;
export const RPC_HOST_DEFAULT_TIMEOUT_MS = 30_000;
export const RPC_HOST_MAX_TIMEOUT_MS = 120_000;
export const RPC_HOST_MAX_ERROR_BYTES = 2 * 1024;
export const RPC_HOST_MAX_ID_BYTES = 128;

const SERVICE_PATTERN = /^[a-z][a-z0-9._-]{0,127}$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export function isRpcHostService(value: unknown): value is string {
	return typeof value === "string" && SERVICE_PATTERN.test(value);
}

export function isRpcHostCorrelationId(value: unknown): value is string {
	return typeof value === "string" && Buffer.byteLength(value) <= RPC_HOST_MAX_ID_BYTES && UUID_PATTERN.test(value);
}

export function isJsonValue(value: unknown, ancestors = new Set<object>()): value is JsonValue {
	if (value === null || typeof value === "string" || typeof value === "boolean") return true;
	if (typeof value === "number") return Number.isFinite(value);
	if (typeof value !== "object") return false;
	if (ancestors.has(value)) return false;

	ancestors.add(value);
	try {
		if (Array.isArray(value)) {
			return value.every((item) => isJsonValue(item, ancestors));
		}
		const prototype = Object.getPrototypeOf(value);
		if (prototype !== Object.prototype && prototype !== null) return false;
		if (Object.getOwnPropertySymbols(value).length > 0) return false;
		return Object.values(value).every((item) => isJsonValue(item, ancestors));
	} finally {
		ancestors.delete(value);
	}
}

export function serializeRpcHostMessage(value: RpcExtensionHostRequest | RpcExtensionHostResponse): string {
	const line = serializeJsonLine(value);
	if (Buffer.byteLength(line) > RPC_HOST_MAX_LINE_BYTES) {
		throw new Error(`RPC host message exceeds ${RPC_HOST_MAX_LINE_BYTES} bytes`);
	}
	return line;
}

export function boundedRpcHostError(error: unknown): string {
	let raw: string;
	try {
		raw = error instanceof Error ? error.message : String(error);
	} catch {
		raw = "Host request failed";
	}
	const message = raw.length > 0 ? raw : "Host request failed";
	if (Buffer.byteLength(message) <= RPC_HOST_MAX_ERROR_BYTES) return message;

	let low = 0;
	let high = message.length;
	while (low < high) {
		const middle = Math.ceil((low + high) / 2);
		if (Buffer.byteLength(message.slice(0, middle)) <= RPC_HOST_MAX_ERROR_BYTES) {
			low = middle;
		} else {
			high = middle - 1;
		}
	}
	return message.slice(0, low);
}

interface PendingHostRequest {
	resolve: (value: JsonValue) => void;
	reject: (error: Error) => void;
	timeout: ReturnType<typeof setTimeout>;
	signal: AbortSignal | undefined;
	onAbort: () => void;
}

export class RpcExtensionHostTransport {
	private readonly pending = new Map<string, PendingHostRequest>();
	private readonly outputLine: (line: string) => void;
	private closedError: Error | undefined;

	constructor(outputLine: (line: string) => void) {
		this.outputLine = outputLine;
	}

	request<TResponse extends JsonValue = JsonValue>(
		service: string,
		payload: JsonValue,
		options: ExtensionHostRequestOptions = {},
	): Promise<TResponse> {
		if (this.closedError) return Promise.reject(this.closedError);
		if (!isRpcHostService(service)) {
			return Promise.reject(new Error(`Invalid host service: ${service}`));
		}
		try {
			if (!isJsonValue(payload)) {
				return Promise.reject(new Error("Host request payload must be JSON-compatible"));
			}
		} catch {
			return Promise.reject(new Error("Host request payload must be JSON-compatible"));
		}
		if (this.pending.size >= RPC_HOST_MAX_PENDING_REQUESTS) {
			return Promise.reject(new Error(`Too many pending host requests (maximum ${RPC_HOST_MAX_PENDING_REQUESTS})`));
		}

		const timeoutMs = options.timeout ?? RPC_HOST_DEFAULT_TIMEOUT_MS;
		if (!Number.isInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > RPC_HOST_MAX_TIMEOUT_MS) {
			return Promise.reject(
				new Error(`Host request timeout must be an integer from 1 to ${RPC_HOST_MAX_TIMEOUT_MS} ms`),
			);
		}
		if (options.signal?.aborted) {
			return Promise.reject(new Error("Host request aborted"));
		}

		const id = crypto.randomUUID();
		let line: string;
		try {
			line = serializeRpcHostMessage({ type: "extension_host_request", id, service, payload });
		} catch (error) {
			return Promise.reject(error instanceof Error ? error : new Error(String(error)));
		}

		return new Promise<TResponse>((resolve, reject) => {
			const cleanup = () => {
				const pending = this.pending.get(id);
				if (!pending) return;
				clearTimeout(pending.timeout);
				pending.signal?.removeEventListener("abort", pending.onAbort);
				this.pending.delete(id);
			};
			const rejectAndCleanup = (error: Error) => {
				cleanup();
				reject(error);
			};
			const onAbort = () => rejectAndCleanup(new Error("Host request aborted"));
			const timeout = setTimeout(
				() => rejectAndCleanup(new Error(`Host request timed out after ${timeoutMs} ms`)),
				timeoutMs,
			);

			this.pending.set(id, {
				resolve: (value) => {
					cleanup();
					resolve(value as TResponse);
				},
				reject: rejectAndCleanup,
				timeout,
				signal: options.signal,
				onAbort,
			});
			options.signal?.addEventListener("abort", onAbort, { once: true });

			try {
				this.outputLine(line);
			} catch (error) {
				rejectAndCleanup(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	handleResponse(value: unknown, lineBytes: number): boolean {
		if (typeof value !== "object" || value === null || !("id" in value)) return false;
		const id = value.id;
		if (!isRpcHostCorrelationId(id)) return false;
		const pending = this.pending.get(id);
		if (!pending) return false;
		if (lineBytes > RPC_HOST_MAX_LINE_BYTES) {
			pending.reject(new Error(`RPC host response exceeds ${RPC_HOST_MAX_LINE_BYTES} bytes`));
			return true;
		}
		if (!("success" in value) || typeof value.success !== "boolean") {
			pending.reject(new Error("Malformed host response"));
			return true;
		}
		if (value.success) {
			if (!("data" in value) || !isJsonValue(value.data)) {
				pending.reject(new Error("Malformed host response data"));
				return true;
			}
			pending.resolve(value.data);
			return true;
		}
		if (!("error" in value) || typeof value.error !== "string") {
			pending.reject(new Error("Malformed host response error"));
			return true;
		}
		if (Buffer.byteLength(value.error) > RPC_HOST_MAX_ERROR_BYTES) {
			pending.reject(new Error("Host response error exceeds 2048 bytes"));
			return true;
		}
		pending.reject(new Error(value.error));
		return true;
	}

	close(error = new Error("RPC host transport closed")): void {
		if (!this.closedError) this.closedError = error;
		for (const pending of [...this.pending.values()]) {
			pending.reject(this.closedError);
		}
	}
}
