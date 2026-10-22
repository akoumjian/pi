import { describe, expect, it } from "vitest";
import { registerRawOutputFailureHandler, restoreStdout, takeOverStdout } from "../src/core/output-guard.ts";

describe("output guard fatal channel handling", () => {
	it("leaves stderr and its listeners untouched by default", () => {
		const originalStdoutWrite = process.stdout.write;
		const originalStderrWrite = process.stderr.write;
		const stderrErrorListeners = process.stderr.listenerCount("error");
		const stderrCloseListeners = process.stderr.listenerCount("close");
		const replacementStderrWrite = (() => true) as typeof process.stderr.write;
		let stderrUnchanged = false;
		let listenersUnchanged = false;

		try {
			process.stderr.write = replacementStderrWrite;
			takeOverStdout();
			stderrUnchanged = process.stderr.write === replacementStderrWrite;
			listenersUnchanged =
				process.stderr.listenerCount("error") === stderrErrorListeners &&
				process.stderr.listenerCount("close") === stderrCloseListeners;
		} finally {
			restoreStdout();
			process.stdout.write = originalStdoutWrite;
			process.stderr.write = originalStderrWrite;
		}

		expect(stderrUnchanged).toBe(true);
		expect(listenersUnchanged).toBe(true);
	});

	it("reports a synchronous guarded stderr failure once and suppresses later writes", async () => {
		const originalStdoutWrite = process.stdout.write;
		const originalStderrWrite = process.stderr.write;
		const stdoutErrorListeners = process.stdout.listenerCount("error");
		const stdoutCloseListeners = process.stdout.listenerCount("close");
		const stderrErrorListeners = process.stderr.listenerCount("error");
		const stderrCloseListeners = process.stderr.listenerCount("close");
		let failureCount = 0;
		let callbackError: Error | null | undefined = new Error("callback was not called");
		let threw = false;
		let removeFailureHandler: (() => void) | undefined;
		let listenersAttached = false;
		let listenersRemoved = false;
		let redirectedChunk: string | Uint8Array | undefined;
		let redirectedEncoding: BufferEncoding | undefined;

		try {
			process.stderr.write = ((
				chunk: string | Uint8Array,
				encoding?: BufferEncoding | ((error?: Error | null) => void),
			) => {
				redirectedChunk = chunk;
				redirectedEncoding = typeof encoding === "string" ? encoding : undefined;
				throw new Error("synthetic stderr failure");
			}) as typeof process.stderr.write;
			takeOverStdout();
			takeOverStdout({ guardStderr: true });
			takeOverStdout({ guardStderr: true });
			removeFailureHandler = registerRawOutputFailureHandler(() => {
				failureCount++;
			});
			listenersAttached =
				process.stdout.listenerCount("error") === stdoutErrorListeners + 1 &&
				process.stdout.listenerCount("close") === stdoutCloseListeners + 1 &&
				process.stderr.listenerCount("error") === stderrErrorListeners + 1 &&
				process.stderr.listenerCount("close") === stderrCloseListeners + 1;

			try {
				process.stdout.write(new Uint8Array([102, 105, 114, 115, 116]), "utf8", (error) => {
					callbackError = error;
				});
				process.stdout.write("suppressed stdout");
				process.stderr.write("suppressed stderr");
			} catch {
				threw = true;
			}
			await new Promise<void>((resolve) => setImmediate(resolve));
		} finally {
			removeFailureHandler?.();
			listenersRemoved =
				process.stdout.listenerCount("error") === stdoutErrorListeners &&
				process.stdout.listenerCount("close") === stdoutCloseListeners &&
				process.stderr.listenerCount("error") === stderrErrorListeners &&
				process.stderr.listenerCount("close") === stderrCloseListeners;
			restoreStdout();
			process.stdout.write = originalStdoutWrite;
			process.stderr.write = originalStderrWrite;
		}

		expect(threw).toBe(false);
		expect(callbackError).toBeNull();
		expect(failureCount).toBe(1);
		expect(redirectedChunk).toEqual(new Uint8Array([102, 105, 114, 115, 116]));
		expect(redirectedEncoding).toBe("utf8");
		expect(listenersAttached).toBe(true);
		expect(listenersRemoved).toBe(true);
	});
});
