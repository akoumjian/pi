interface StdoutTakeoverState {
	rawStdoutWrite: (chunk: string, callback?: (error?: Error | null) => void) => boolean;
	rawStderrWrite: (chunk: string, callback?: (error?: Error | null) => void) => boolean;
	originalStdoutWrite: typeof process.stdout.write;
	originalStderrWrite: typeof process.stderr.write;
}

type RawOutputFailureHandler = (error: Error) => void;

let stdoutTakeoverState: StdoutTakeoverState | undefined;
let rawOutputFailure: Error | undefined;
let rawOutputFailureHandler: RawOutputFailureHandler | undefined;

const RAW_STDOUT_RETRY_DELAY_MS = 10;

let rawStdoutWriteTail: Promise<void> = Promise.resolve();

function getRawStdoutWrite(): StdoutTakeoverState["rawStdoutWrite"] {
	if (stdoutTakeoverState) {
		return stdoutTakeoverState.rawStdoutWrite;
	}
	return process.stdout.write.bind(process.stdout) as StdoutTakeoverState["rawStdoutWrite"];
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

export function takeOverStdout(): void {
	if (stdoutTakeoverState) {
		return;
	}

	const rawStdoutWrite = process.stdout.write.bind(process.stdout) as StdoutTakeoverState["rawStdoutWrite"];
	const rawStderrWrite = process.stderr.write.bind(process.stderr) as StdoutTakeoverState["rawStderrWrite"];
	const originalStdoutWrite = process.stdout.write;
	const originalStderrWrite = process.stderr.write;

	const guardedStderrWrite = (
		chunk: string | Uint8Array,
		encodingOrCallback?: BufferEncoding | ((error?: Error | null) => void),
		callback?: (error?: Error | null) => void,
	): boolean => {
		const writeCallback = typeof encodingOrCallback === "function" ? encodingOrCallback : callback;
		if (rawOutputFailure) {
			writeCallback?.();
			return true;
		}
		try {
			return rawStderrWrite(String(chunk), (error) => {
				if (error) reportRawOutputFailure(error);
				writeCallback?.();
			});
		} catch (error) {
			reportRawOutputFailure(error);
			writeCallback?.();
			return true;
		}
	};

	process.stdout.write = guardedStderrWrite as typeof process.stdout.write;
	process.stderr.write = guardedStderrWrite as typeof process.stderr.write;

	stdoutTakeoverState = {
		rawStdoutWrite,
		rawStderrWrite,
		originalStdoutWrite,
		originalStderrWrite,
	};
}
export function restoreStdout(): void {
	if (!stdoutTakeoverState) {
		return;
	}

	process.stdout.write = stdoutTakeoverState.originalStdoutWrite;
	process.stderr.write = stdoutTakeoverState.originalStderrWrite;
	stdoutTakeoverState = undefined;
}

export function isStdoutTakenOver(): boolean {
	return stdoutTakeoverState !== undefined;
}

/** Register mode-specific cleanup for fatal raw stdout or redirected stderr failure. */
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
