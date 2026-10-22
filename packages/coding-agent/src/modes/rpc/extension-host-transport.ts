import * as crypto from "node:crypto";
import { isProxy } from "node:util/types";
import type { JsonValue } from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { Compile } from "typebox/compile";
import type { ExtensionHostRequestOptions } from "../../core/extensions/types.ts";
import { serializeJsonLine } from "./jsonl.ts";
import type { RpcExtensionHostRequest, RpcExtensionHostResponse } from "./rpc-types.ts";

export const RPC_HOST_MAX_LINE_BYTES = 256 * 1024;
export const RPC_HOST_MAX_PENDING_REQUESTS = 16;
export const RPC_HOST_DEFAULT_TIMEOUT_MS = 30_000;
export const RPC_HOST_MAX_TIMEOUT_MS = 120_000;
export const RPC_HOST_MAX_ERROR_BYTES = 2 * 1024;
export const RPC_HOST_MAX_ID_BYTES = 128;
export const RPC_HOST_MAX_JSON_DEPTH = 64;
export const RPC_HOST_MAX_JSON_NODES = 65_536;

const SERVICE_PATTERN_SOURCE = "^[a-z][a-z0-9._-]{0,127}$";
const UUID_PATTERN_SOURCE =
	"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$";

const RpcHostServiceSchema = Type.String({ pattern: SERVICE_PATTERN_SOURCE, maxLength: 128 });
const RpcHostCorrelationIdSchema = Type.String({ pattern: UUID_PATTERN_SOURCE, maxLength: RPC_HOST_MAX_ID_BYTES });
const RpcExtensionHostRequestSchema = Type.Object(
	{
		type: Type.Literal("extension_host_request"),
		id: RpcHostCorrelationIdSchema,
		service: RpcHostServiceSchema,
		payload: Type.Unknown(),
	},
	{ additionalProperties: false },
);
const RpcExtensionHostResponseSchema = Type.Union([
	Type.Object(
		{
			type: Type.Literal("extension_host_response"),
			id: RpcHostCorrelationIdSchema,
			success: Type.Literal(true),
			data: Type.Unknown(),
		},
		{ additionalProperties: false },
	),
	Type.Object(
		{
			type: Type.Literal("extension_host_response"),
			id: RpcHostCorrelationIdSchema,
			success: Type.Literal(false),
			error: Type.String({ maxLength: RPC_HOST_MAX_ERROR_BYTES }),
		},
		{ additionalProperties: false },
	),
]);

const validateRpcHostService = Compile(RpcHostServiceSchema);
const validateRpcHostCorrelationId = Compile(RpcHostCorrelationIdSchema);
const validateRpcExtensionHostRequest = Compile(RpcExtensionHostRequestSchema);
const validateRpcExtensionHostResponse = Compile(RpcExtensionHostResponseSchema);

export function isRpcHostService(value: unknown): value is string {
	try {
		return validateRpcHostService.Check(value);
	} catch {
		return false;
	}
}

export function isRpcHostCorrelationId(value: unknown): value is string {
	try {
		return validateRpcHostCorrelationId.Check(value);
	} catch {
		return false;
	}
}

export function isRpcExtensionHostRequest(value: unknown): value is RpcExtensionHostRequest {
	try {
		return validateRpcExtensionHostRequest.Check(value);
	} catch {
		return false;
	}
}

export function isRpcExtensionHostResponse(value: unknown): value is RpcExtensionHostResponse {
	try {
		return validateRpcExtensionHostResponse.Check(value);
	} catch {
		return false;
	}
}

type JsonTraversalEntry = { phase: "enter"; value: unknown; depth: number } | { phase: "exit"; value: object };

export function isJsonValue(value: unknown): value is JsonValue {
	try {
		const pending: JsonTraversalEntry[] = [{ phase: "enter", value, depth: 0 }];
		const activeAncestors = new Set<object>();
		let scheduledNodes = 1;
		let visitedNodes = 0;

		while (pending.length > 0) {
			const current = pending.pop()!;
			if (current.phase === "exit") {
				activeAncestors.delete(current.value);
				continue;
			}

			visitedNodes++;
			if (visitedNodes > RPC_HOST_MAX_JSON_NODES) return false;

			const currentValue = current.value;
			if (currentValue === null || typeof currentValue === "string" || typeof currentValue === "boolean") {
				continue;
			}
			if (typeof currentValue === "number") {
				if (!Number.isFinite(currentValue)) return false;
				continue;
			}
			if (typeof currentValue !== "object" || current.depth >= RPC_HOST_MAX_JSON_DEPTH) return false;
			if (isProxy(currentValue) || activeAncestors.has(currentValue)) return false;
			activeAncestors.add(currentValue);
			pending.push({ phase: "exit", value: currentValue });

			const keys = Reflect.ownKeys(currentValue);
			if (Array.isArray(currentValue)) {
				if (Object.getPrototypeOf(currentValue) !== Array.prototype) return false;
				const lengthDescriptor = Object.getOwnPropertyDescriptor(currentValue, "length");
				if (!lengthDescriptor || !("value" in lengthDescriptor) || typeof lengthDescriptor.value !== "number") {
					return false;
				}
				const length = lengthDescriptor.value;
				if (!Number.isSafeInteger(length) || length < 0 || length > RPC_HOST_MAX_JSON_NODES - scheduledNodes) {
					return false;
				}
				let indices = 0;
				for (const key of keys) {
					if (key === "length") continue;
					if (typeof key !== "string" || !/^(0|[1-9]\d*)$/.test(key)) return false;
					const index = Number(key);
					if (!Number.isSafeInteger(index) || index < 0 || index >= length) return false;
					const descriptor = Object.getOwnPropertyDescriptor(currentValue, key);
					if (!descriptor?.enumerable || !("value" in descriptor)) return false;
					pending.push({ phase: "enter", value: descriptor.value, depth: current.depth + 1 });
					indices++;
				}
				if (indices !== length) return false;
				scheduledNodes += length;
				continue;
			}

			const prototype = Object.getPrototypeOf(currentValue);
			if (prototype !== Object.prototype && prototype !== null) return false;
			if (keys.length > RPC_HOST_MAX_JSON_NODES - scheduledNodes) return false;
			for (const key of keys) {
				if (typeof key !== "string") return false;
				const descriptor = Object.getOwnPropertyDescriptor(currentValue, key);
				if (!descriptor?.enumerable || !("value" in descriptor)) return false;
				pending.push({ phase: "enter", value: descriptor.value, depth: current.depth + 1 });
			}
			scheduledNodes += keys.length;
		}
		return true;
	} catch {
		return false;
	}
}

const ABORTED_GETTER = Object.getOwnPropertyDescriptor(AbortSignal.prototype, "aborted")?.get;

function getAbortSignalAborted(value: AbortSignal): boolean {
	if (!ABORTED_GETTER) throw new Error("AbortSignal is unavailable");
	return ABORTED_GETTER.call(value) as boolean;
}

function isAbortSignal(value: unknown): value is AbortSignal {
	try {
		return (
			typeof value === "object" && value !== null && typeof getAbortSignalAborted(value as AbortSignal) === "boolean"
		);
	} catch {
		return false;
	}
}

type HostRequestOptionsValidation =
	| { valid: true; timeout: number; signal: AbortSignal | undefined }
	| { valid: false; invalidTimeout: boolean };

function validateHostRequestOptions(value: unknown): HostRequestOptionsValidation {
	try {
		if (typeof value !== "object" || value === null) return { valid: false, invalidTimeout: false };
		const prototype = Object.getPrototypeOf(value);
		if (prototype !== Object.prototype && prototype !== null) return { valid: false, invalidTimeout: false };
		const keys = Reflect.ownKeys(value);
		if (keys.some((key) => key !== "timeout" && key !== "signal")) {
			return { valid: false, invalidTimeout: false };
		}
		const timeoutDescriptor = Object.getOwnPropertyDescriptor(value, "timeout");
		const signalDescriptor = Object.getOwnPropertyDescriptor(value, "signal");
		if (
			(timeoutDescriptor && !("value" in timeoutDescriptor)) ||
			(signalDescriptor && !("value" in signalDescriptor))
		) {
			return { valid: false, invalidTimeout: false };
		}
		const timeout = timeoutDescriptor?.value ?? RPC_HOST_DEFAULT_TIMEOUT_MS;
		if (
			typeof timeout !== "number" ||
			!Number.isInteger(timeout) ||
			timeout <= 0 ||
			timeout > RPC_HOST_MAX_TIMEOUT_MS
		) {
			return { valid: false, invalidTimeout: true };
		}
		const signal = signalDescriptor?.value;
		if (signal !== undefined && !isAbortSignal(signal)) return { valid: false, invalidTimeout: false };
		return { valid: true, timeout, signal };
	} catch {
		return { valid: false, invalidTimeout: false };
	}
}

export function serializeRpcHostMessage(value: RpcExtensionHostRequest | RpcExtensionHostResponse): string {
	const line = serializeJsonLine(value);
	if (Buffer.byteLength(line) > RPC_HOST_MAX_LINE_BYTES) {
		throw new Error(`RPC host message exceeds ${RPC_HOST_MAX_LINE_BYTES} bytes`);
	}
	return line;
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
			return Promise.reject(new Error("Invalid host service name"));
		}
		if (!isJsonValue(payload)) {
			return Promise.reject(new Error("Host request payload must be JSON-compatible"));
		}
		if (this.pending.size >= RPC_HOST_MAX_PENDING_REQUESTS) {
			return Promise.reject(new Error(`Too many pending host requests (maximum ${RPC_HOST_MAX_PENDING_REQUESTS})`));
		}

		const validatedOptions = validateHostRequestOptions(options);
		if (!validatedOptions.valid) {
			return Promise.reject(
				new Error(
					validatedOptions.invalidTimeout
						? `Host request timeout must be an integer from 1 to ${RPC_HOST_MAX_TIMEOUT_MS} ms`
						: "Invalid host request options",
				),
			);
		}
		const { timeout: timeoutMs, signal } = validatedOptions;
		if (signal && getAbortSignalAborted(signal)) return Promise.reject(new Error("Host request aborted"));

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
				if (pending.signal) {
					try {
						EventTarget.prototype.removeEventListener.call(pending.signal, "abort", pending.onAbort);
					} catch {
						// The pending slot must still be released if signal cleanup fails.
					}
				}
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
				signal,
				onAbort,
			});
			try {
				if (signal) EventTarget.prototype.addEventListener.call(signal, "abort", onAbort, { once: true });
				this.outputLine(line);
			} catch (error) {
				rejectAndCleanup(error instanceof Error ? error : new Error(String(error)));
			}
		});
	}

	handleResponse(value: unknown, lineBytes: number): boolean {
		let pending: PendingHostRequest | undefined;
		try {
			if (typeof value !== "object" || value === null || !("id" in value)) return false;
			const id = value.id;
			if (!isRpcHostCorrelationId(id)) return false;
			pending = this.pending.get(id);
			if (!pending) return false;
			if (lineBytes > RPC_HOST_MAX_LINE_BYTES) {
				pending.reject(new Error(`RPC host response exceeds ${RPC_HOST_MAX_LINE_BYTES} bytes`));
				return true;
			}
			if (!isRpcExtensionHostResponse(value)) {
				pending.reject(new Error("Malformed host response"));
				return true;
			}
			if (value.success) {
				if (!isJsonValue(value.data)) {
					pending.reject(new Error("Malformed host response data"));
					return true;
				}
				pending.resolve(value.data);
				return true;
			}
			if (Buffer.byteLength(value.error) > RPC_HOST_MAX_ERROR_BYTES) {
				pending.reject(new Error("Host response error exceeds 2048 bytes"));
				return true;
			}
			pending.reject(new Error(value.error));
			return true;
		} catch {
			pending?.reject(new Error("Malformed host response"));
			return pending !== undefined;
		}
	}

	close(error = new Error("RPC host transport closed")): void {
		if (!this.closedError) this.closedError = error;
		for (const pending of [...this.pending.values()]) {
			pending.reject(this.closedError);
		}
	}
}
