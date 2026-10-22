import { describe, expect, it } from "vitest";
import { registerRawOutputFailureHandler, restoreStdout, takeOverStdout } from "../src/core/output-guard.ts";

describe("output guard fatal channel handling", () => {
	it("reports a synchronous redirected stderr failure once and suppresses later writes", () => {
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

		try {
			process.stderr.write = (() => {
				throw new Error("synthetic stderr failure");
			}) as typeof process.stderr.write;
			takeOverStdout();
			removeFailureHandler = registerRawOutputFailureHandler(() => {
				failureCount++;
			});
			listenersAttached =
				process.stdout.listenerCount("error") === stdoutErrorListeners + 1 &&
				process.stdout.listenerCount("close") === stdoutCloseListeners + 1 &&
				process.stderr.listenerCount("error") === stderrErrorListeners + 1 &&
				process.stderr.listenerCount("close") === stderrCloseListeners + 1;

			try {
				process.stdout.write("first", (error) => {
					callbackError = error;
				});
				process.stdout.write("suppressed stdout");
				process.stderr.write("suppressed stderr");
			} catch {
				threw = true;
			}
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
		expect(callbackError).toBeUndefined();
		expect(failureCount).toBe(1);
		expect(listenersAttached).toBe(true);
		expect(listenersRemoved).toBe(true);
	});
});
