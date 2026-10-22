interface StdoutTakeoverState {
	rawStdoutWrite: (chunk: string, callback?: (error?: Error | null) => void) => boolean;
	rawStderrWrite: (chunk: string, callback?: (error?: Error | null) => void) => boolean;
	originalStdoutWrite: typeof process.stdout.write;
}

type RawStdoutFailureHandler = (error: Error) => void;

let stdoutTakeoverState: StdoutTakeoverState | undefined;
let rawStdoutFailure: Error | undefined;
let rawStdoutFailureHandler: RawStdoutFailureHandler | undefined;

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

function reportRawStdoutFailure(error: unknown): void {
	if (rawStdoutFailure) return;
	rawStdoutFailure = normalizeError(error);
	if (rawStdoutFailureHandler) {
		rawStdoutFailureHandler(rawStdoutFailure);
		return;
	}
	process.exit(1);
}

async function writeRawStdoutChunk(text: string): Promise<void> {
	while (!rawStdoutFailure) {
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
				reportRawStdoutFailure(writeError);
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

	process.stdout.write = ((
		chunk: string | Uint8Array,
		encodingOrCallback?: BufferEncoding | ((error?: Error | null) => void),
		callback?: (error?: Error | null) => void,
	): boolean => {
		if (typeof encodingOrCallback === "function") {
			return rawStderrWrite(String(chunk), encodingOrCallback);
		}
		return rawStderrWrite(String(chunk), callback);
	}) as typeof process.stdout.write;

	stdoutTakeoverState = {
		rawStdoutWrite,
		rawStderrWrite,
		originalStdoutWrite,
	};
}

export function restoreStdout(): void {
	if (!stdoutTakeoverState) {
		return;
	}

	process.stdout.write = stdoutTakeoverState.originalStdoutWrite;
	stdoutTakeoverState = undefined;
}

export function isStdoutTakenOver(): boolean {
	return stdoutTakeoverState !== undefined;
}

/** Register mode-specific cleanup for fatal raw stdout write errors or stream closure. */
export function registerRawStdoutFailureHandler(handler: RawStdoutFailureHandler): () => void {
	rawStdoutFailureHandler = handler;
	const stdout = process.stdout;
	const onError = (error: Error) => reportRawStdoutFailure(error);
	const onClose = () => reportRawStdoutFailure(new Error("Raw stdout closed"));
	stdout.on("error", onError);
	stdout.on("close", onClose);
	const existingFailure = rawStdoutFailure;
	if (existingFailure) queueMicrotask(() => handler(existingFailure));
	else if (stdout.destroyed) queueMicrotask(onClose);

	return () => {
		stdout.off("error", onError);
		stdout.off("close", onClose);
		if (rawStdoutFailureHandler === handler) rawStdoutFailureHandler = undefined;
	};
}

export function writeRawStdout(text: string): void {
	if (text.length === 0 || rawStdoutFailure) {
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
	if (rawStdoutFailure) return;
	await waitForRawStdoutBackpressure();
	if (rawStdoutFailure) return;
	await writeRawStdoutChunk("");
}
