type RawWrite = (
	chunk: string | Uint8Array,
	encodingOrCallback?: BufferEncoding | ((error?: Error | null) => void),
	callback?: (error?: Error | null) => void,
) => boolean;

interface StdoutTakeoverState {
	rawStdoutWrite: RawWrite;
	rawStderrWrite: RawWrite;
	originalStdoutWrite: typeof process.stdout.write;
	originalStderrWrite: typeof process.stderr.write;
	stderrGuarded: boolean;
}

export interface StdoutTakeoverOptions {
	/** Treat redirected/direct stderr failure as fatal. Reserved for RPC transport containment. */
	guardStderr?: boolean;
}

type RawOutputFailureHandler = (error: Error) => void;

let stdoutTakeoverState: StdoutTakeoverState | undefined;
let rawOutputFailure: Error | undefined;
let rawOutputFailureHandler: RawOutputFailureHandler | undefined;

const RAW_STDOUT_RETRY_DELAY_MS = 10;

let rawStdoutWriteTail: Promise<void> = Promise.resolve();

function getRawStdoutWrite(): RawWrite {
	if (stdoutTakeoverState) {
		return stdoutTakeoverState.rawStdoutWrite;
	}
	return process.stdout.write.bind(process.stdout) as RawWrite;
}

function normalizeError(error: unknown): Error {
	return error instanceof Error ? error : new Error(String(error));
}

function reportRawOutputFailure(error: unknown): void {
	if (rawOutputFailure) return;
	rawOutputFailure = normalizeError(error);
	if (rawOutputFailureHandler) {
		rawOutputFailureHandler(rawOutputFailure);
		return;
	}
	process.exit(1);
}

function enableGuardedStderr(state: StdoutTakeoverState): void {
	if (state.stderrGuarded) return;

	const guardedStderrWrite: RawWrite = (chunk, encodingOrCallback, callback) => {
		const writeCallback = typeof encodingOrCallback === "function" ? encodingOrCallback : callback;
		let callbackQueued = false;
		const completeWrite = () => {
			if (callbackQueued || !writeCallback) return;
			callbackQueued = true;
			queueMicrotask(() => writeCallback(null));
		};
		if (rawOutputFailure) {
			completeWrite();
			return true;
		}
		try {
			const onWrite = (error?: Error | null) => {
				if (error) reportRawOutputFailure(error);
				completeWrite();
			};
			if (typeof encodingOrCallback === "string") {
				return state.rawStderrWrite(chunk, encodingOrCallback, onWrite);
			}
			return state.rawStderrWrite(chunk, onWrite);
		} catch (error) {
			reportRawOutputFailure(error);
			completeWrite();
			return true;
		}
	};

	process.stdout.write = guardedStderrWrite as typeof process.stdout.write;
	process.stderr.write = guardedStderrWrite as typeof process.stderr.write;
	state.stderrGuarded = true;
}

async function writeRawStdoutChunk(text: string): Promise<void> {
	while (!rawOutputFailure) {
		try {
			await new Promise<void>((resolve, reject) => {
				try {
					getRawStdoutWrite()(text, (error) => {
						if (error) reject(error);
						else resolve();
					});
				} catch (error) {
					reject(normalizeError(error));
				}
			});
			return;
		} catch (error) {
			const writeError = normalizeError(error);
			const code = (writeError as Error & { code?: unknown }).code;
			if (code !== "ENOBUFS" && code !== "EAGAIN" && code !== "EWOULDBLOCK") {
				reportRawOutputFailure(writeError);
				return;
			}
			await new Promise<void>((resolve) => setTimeout(resolve, RAW_STDOUT_RETRY_DELAY_MS));
		}
	}
}

export function takeOverStdout(options: StdoutTakeoverOptions = {}): void {
	if (stdoutTakeoverState) {
		if (options.guardStderr) enableGuardedStderr(stdoutTakeoverState);
		return;
	}

	const rawStdoutWrite = process.stdout.write.bind(process.stdout) as RawWrite;
	const rawStderrWrite = process.stderr.write.bind(process.stderr) as RawWrite;
	const originalStdoutWrite = process.stdout.write;
	const originalStderrWrite = process.stderr.write;

	process.stdout.write = ((chunk, encodingOrCallback, callback): boolean => {
		if (typeof encodingOrCallback === "function") {
			return rawStderrWrite(String(chunk), encodingOrCallback);
		}
		return rawStderrWrite(String(chunk), callback);
	}) as typeof process.stdout.write;

	stdoutTakeoverState = {
		rawStdoutWrite,
		rawStderrWrite,
		originalStdoutWrite,
		originalStderrWrite,
		stderrGuarded: false,
	};
	if (options.guardStderr) enableGuardedStderr(stdoutTakeoverState);
}

export function restoreStdout(): void {
	if (!stdoutTakeoverState) {
		return;
	}

	process.stdout.write = stdoutTakeoverState.originalStdoutWrite;
	if (stdoutTakeoverState.stderrGuarded) process.stderr.write = stdoutTakeoverState.originalStderrWrite;
	stdoutTakeoverState = undefined;
}

export function isStdoutTakenOver(): boolean {
	return stdoutTakeoverState !== undefined;
}

/** Register mode-specific cleanup for fatal raw output failure or stream closure. */
export function registerRawOutputFailureHandler(handler: RawOutputFailureHandler): () => void {
	rawOutputFailureHandler = handler;
	const stdout = process.stdout;
	const stderr = process.stderr;
	const onError = (error: Error) => reportRawOutputFailure(error);
	const onClose = () => reportRawOutputFailure(new Error("Raw output closed"));
	stdout.on("error", onError);
	stdout.on("close", onClose);
	stderr.on("error", onError);
	stderr.on("close", onClose);
	const existingFailure = rawOutputFailure;
	if (existingFailure) queueMicrotask(() => handler(existingFailure));
	else if (stdout.destroyed || stderr.destroyed) queueMicrotask(onClose);

	return () => {
		stdout.off("error", onError);
		stdout.off("close", onClose);
		stderr.off("error", onError);
		stderr.off("close", onClose);
		if (rawOutputFailureHandler === handler) rawOutputFailureHandler = undefined;
	};
}

export function writeRawStdout(text: string): void {
	if (text.length === 0 || rawOutputFailure) {
		return;
	}
	rawStdoutWriteTail = rawStdoutWriteTail.then(() => writeRawStdoutChunk(text));
}

export async function waitForRawStdoutBackpressure(): Promise<void> {
	while (true) {
		const tail = rawStdoutWriteTail;
		await tail;
		if (tail === rawStdoutWriteTail) {
			return;
		}
	}
}

export async function flushRawStdout(): Promise<void> {
	if (rawOutputFailure) return;
	await waitForRawStdoutBackpressure();
	if (rawOutputFailure) return;
	await writeRawStdoutChunk("");
}
