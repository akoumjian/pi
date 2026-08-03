import type { AgentTool } from "@earendil-works/pi-agent-core";
import {
	type AssistantMessage,
	createAssistantMessageEventStream,
	fauxAssistantMessage,
	type Model,
} from "@earendil-works/pi-ai";
import { Type } from "typebox";
import { afterEach, describe, expect, it, vi } from "vitest";
import { estimateTokens } from "../../src/core/compaction/index.ts";
import { createHarness, getMessageText, type Harness } from "./harness.ts";

type SessionWithCompactionInternals = {
	_checkCompaction: (assistantMessage: AssistantMessage, skipAbortedCheck?: boolean) => Promise<boolean>;
	_runAutoCompaction: (reason: "overflow" | "threshold", willRetry: boolean) => Promise<boolean>;
};

function createUsage(totalTokens: number) {
	return {
		input: totalTokens,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

function createAssistant(
	harness: Harness,
	options: {
		stopReason?: AssistantMessage["stopReason"];
		errorMessage?: string;
		totalTokens?: number;
		timestamp?: number;
	},
): AssistantMessage {
	const model = harness.getModel();
	return {
		...fauxAssistantMessage("", {
			stopReason: options.stopReason,
			errorMessage: options.errorMessage,
			timestamp: options.timestamp,
		}),
		api: model.api,
		provider: model.provider,
		model: model.id,
		usage: createUsage(options.totalTokens ?? 0),
	};
}

function useSummaryStreamFn(harness: Harness, summary: string): () => number {
	let callCount = 0;
	harness.session.agent.streamFunction = (model) => {
		callCount++;
		const stream = createAssistantMessageEventStream();
		queueMicrotask(() => {
			const message: AssistantMessage = {
				...fauxAssistantMessage(summary),
				api: model.api,
				provider: model.provider,
				model: model.id,
				usage: createUsage(10),
			};
			stream.push({ type: "done", reason: "stop", message });
		});
		return stream;
	};
	return () => callCount;
}

const echoToolSchema = Type.Object({ text: Type.String() });

function createLargeResultTool(): AgentTool<typeof echoToolSchema> {
	return {
		name: "large-result",
		label: "Large result",
		description: "Return enough text to cross the next-turn context budget",
		parameters: echoToolSchema,
		execute: async () => ({
			content: [{ type: "text", text: "tool result ".repeat(400) }],
			details: {},
		}),
	};
}

function seedCompactableSession(harness: Harness): void {
	harness.settingsManager.applyOverrides({ compaction: { keepRecentTokens: 1 } });
	const now = Date.now();
	harness.sessionManager.appendMessage({
		role: "user",
		content: [{ type: "text", text: "message to compact ".repeat(200) }],
		timestamp: now - 1000,
	});
	const assistant = createAssistant(harness, {
		stopReason: "stop",
		totalTokens: 100,
		timestamp: now - 500,
	});
	assistant.content = [{ type: "text", text: "assistant response to compact ".repeat(200) }];
	harness.sessionManager.appendMessage(assistant);
	harness.session.agent.state.messages = harness.sessionManager.buildSessionContext().messages;
}

describe("AgentSession compaction characterization", () => {
	const harnesses: Harness[] = [];

	afterEach(() => {
		vi.useRealTimers();
		vi.restoreAllMocks();
		while (harnesses.length > 0) {
			harnesses.pop()?.cleanup();
		}
	});

	it("manually compacts using an extension-provided summary", async () => {
		const summaryUsage = {
			input: 10,
			output: 20,
			cacheRead: 30,
			cacheWrite: 40,
			totalTokens: 100,
			cost: { input: 0.1, output: 0.2, cacheRead: 0.3, cacheWrite: 0.4, total: 1 },
		};
		const harness = await createHarness({
			settings: { compaction: { keepRecentTokens: 1 } },
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => ({
						compaction: {
							summary: "summary from extension",
							firstKeptEntryId: event.preparation.firstKeptEntryId,
							tokensBefore: event.preparation.tokensBefore,
							usage: summaryUsage,
							details: { source: "extension" },
						},
					}));
				},
			],
		});
		harnesses.push(harness);

		await harness.session.prompt("one");
		await harness.session.prompt("two");
		const statsBefore = harness.session.getSessionStats();

		const result = await harness.session.compact();
		const compactionEntries = harness.sessionManager.getEntries().filter((entry) => entry.type === "compaction");
		const estimatedTokensAfter = harness.session.messages.reduce((sum, message) => sum + estimateTokens(message), 0);

		expect(result.summary).toBe("summary from extension");
		expect(result.usage).toEqual(summaryUsage);
		expect(result.estimatedTokensAfter).toBe(estimatedTokensAfter);
		expect(compactionEntries).toHaveLength(1);
		const compactionEntry = compactionEntries[0];
		if (compactionEntry?.type === "compaction") {
			expect(compactionEntry.usage).toEqual(summaryUsage);
		}
		const statsAfter = harness.session.getSessionStats();
		expect(statsAfter.tokens.input).toBe(statsBefore.tokens.input + summaryUsage.input);
		expect(statsAfter.tokens.output).toBe(statsBefore.tokens.output + summaryUsage.output);
		expect(statsAfter.tokens.cacheRead).toBe(statsBefore.tokens.cacheRead + summaryUsage.cacheRead);
		expect(statsAfter.tokens.cacheWrite).toBe(statsBefore.tokens.cacheWrite + summaryUsage.cacheWrite);
		expect(statsAfter.cost).toBe(statsBefore.cost + summaryUsage.cost.total);
		expect(harness.session.messages[0]?.role).toBe("compactionSummary");
	});

	it("throws when compacting without a model", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		harness.session.agent.state.model = undefined as unknown as Model<any>;

		await expect(harness.session.compact()).rejects.toThrow("No model selected");
	});

	it("throws when compacting without configured auth", async () => {
		const harness = await createHarness({ withConfiguredAuth: false });
		harnesses.push(harness);

		await expect(harness.session.compact()).rejects.toThrow(`No API key found for ${harness.getModel().provider}.`);
	});

	it("manually compacts with a custom streamFn when registry auth is absent", async () => {
		const harness = await createHarness({ withConfiguredAuth: false });
		harnesses.push(harness);
		seedCompactableSession(harness);
		const getStreamCallCount = useSummaryStreamFn(harness, "summary from custom stream");

		const result = await harness.session.compact();

		expect(result.summary).toContain("summary from custom stream");
		expect(getStreamCallCount()).toBe(1);
	});

	it("persists usage from pi-generated manual compaction", async () => {
		const harness = await createHarness({ withConfiguredAuth: false });
		harnesses.push(harness);
		seedCompactableSession(harness);
		useSummaryStreamFn(harness, "summary from custom stream");

		const result = await harness.session.compact();

		const compactionEntries = harness.sessionManager.getEntries().filter((entry) => entry.type === "compaction");
		expect(result.usage).toEqual(createUsage(10));
		expect(compactionEntries).toHaveLength(1);
		expect(compactionEntries[0]?.type === "compaction" ? compactionEntries[0].usage : undefined).toEqual(
			createUsage(10),
		);
	});

	it("compacts an over-window tool-result context before the next request", async () => {
		const tool = createLargeResultTool();
		let summarizedTokensBefore = 0;
		const harness = await createHarness({
			models: [{ id: "faux-1", contextWindow: 5_000, maxTokens: 100 }],
			settings: { compaction: { enabled: true, keepRecentTokens: 100, reserveTokens: 100 } },
			tools: [tool],
			initialActiveToolNames: [tool.name],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => {
						summarizedTokensBefore = event.preparation.tokensBefore;
						return {
							compaction: {
								summary: "extension-provided oversized-history summary",
								firstKeptEntryId: event.preparation.firstKeptEntryId,
								tokensBefore: event.preparation.tokensBefore,
								details: {},
							},
						};
					});
				},
			],
		});
		harnesses.push(harness);
		seedCompactableSession(harness);
		harness.settingsManager.applyOverrides({ compaction: { keepRecentTokens: 100 } });

		const toolUse = createAssistant(harness, { stopReason: "toolUse", totalTokens: 4_950 });
		toolUse.content = [{ type: "toolCall", id: "large-1", name: tool.name, arguments: { text: "result" } }];
		let requestCount = 0;
		let secondRequestRoles: string[] = [];
		harness.session.agent.streamFunction = (_model, context) => {
			const stream = createAssistantMessageEventStream();
			const message = requestCount === 0 ? toolUse : createAssistant(harness, { stopReason: "stop" });
			if (requestCount > 0) {
				message.content = [{ type: "text", text: "done" }];
				secondRequestRoles = context.messages.map((contextMessage) => contextMessage.role);
			}
			requestCount++;
			const reason = message.content.some((block) => block.type === "toolCall") ? "toolUse" : "stop";
			queueMicrotask(() => stream.push({ type: "done", reason, message }));
			return stream;
		};
		const continueSpy = vi.spyOn(harness.session.agent, "continue");

		await harness.session.prompt("start");

		const branch = harness.sessionManager.getBranch();
		const compactionIndex = branch.findIndex((entry) => entry.type === "compaction");
		const toolResultIndex = branch.findIndex(
			(entry) => entry.type === "message" && entry.message.role === "toolResult",
		);
		expect(summarizedTokensBefore).toBeGreaterThan(5_000);
		expect(compactionIndex).toBeGreaterThan(toolResultIndex);
		expect(secondRequestRoles).toContain("toolResult");
		expect(requestCount).toBe(2);
		expect(continueSpy).not.toHaveBeenCalled();
		expect(harness.session.agent.state.messages).toEqual(harness.sessionManager.buildSessionContext().messages);
		expect(harness.eventsOfType("compaction_end").at(-1)).toMatchObject({
			reason: "overflow",
			aborted: false,
			willRetry: false,
		});
	});

	it("does not retry a cancelled next-turn compaction after the loop settles", async () => {
		const tool = createLargeResultTool();
		let compactionAttempts = 0;
		const harness = await createHarness({
			models: [{ id: "faux-1", contextWindow: 5_000, maxTokens: 100 }],
			settings: { compaction: { enabled: true, keepRecentTokens: 2_000, reserveTokens: 100 } },
			tools: [tool],
			initialActiveToolNames: [tool.name],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async () => {
						compactionAttempts++;
						return { cancel: true };
					});
				},
			],
		});
		harnesses.push(harness);
		seedCompactableSession(harness);
		harness.settingsManager.applyOverrides({ compaction: { keepRecentTokens: 2_000 } });
		const toolUse = createAssistant(harness, { stopReason: "toolUse", totalTokens: 4_950 });
		toolUse.content = [{ type: "toolCall", id: "large-1", name: tool.name, arguments: { text: "result" } }];
		let requestCount = 0;
		harness.session.agent.streamFunction = () => {
			requestCount++;
			const stream = createAssistantMessageEventStream();
			queueMicrotask(() => stream.push({ type: "done", reason: "toolUse", message: toolUse }));
			return stream;
		};

		await harness.session.prompt("start");

		expect(requestCount).toBe(1);
		expect(compactionAttempts).toBe(1);
		expect(harness.eventsOfType("compaction_end")).toHaveLength(1);
		expect(harness.eventsOfType("compaction_end").at(-1)).toMatchObject({
			reason: "overflow",
			aborted: true,
		});
		expect(harness.session.messages.at(-1)).toMatchObject({
			role: "assistant",
			stopReason: "error",
			errorMessage: "Compaction cancelled before the next turn",
		});
	});

	it("aborts in-flight next-turn compaction without sending another request", async () => {
		const tool = createLargeResultTool();
		let markCompactionStarted!: () => void;
		const compactionStarted = new Promise<void>((resolve) => {
			markCompactionStarted = resolve;
		});
		const harness = await createHarness({
			models: [{ id: "faux-1", contextWindow: 5_000, maxTokens: 100 }],
			settings: { compaction: { enabled: true, keepRecentTokens: 2_000, reserveTokens: 100 } },
			tools: [tool],
			initialActiveToolNames: [tool.name],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => {
						markCompactionStarted();
						await new Promise<never>((_resolve, reject) => {
							event.signal.addEventListener(
								"abort",
								() => reject(event.signal.reason ?? new DOMException("Aborted", "AbortError")),
								{ once: true },
							);
						});
					});
				},
			],
		});
		harnesses.push(harness);
		seedCompactableSession(harness);
		harness.settingsManager.applyOverrides({ compaction: { keepRecentTokens: 2_000 } });
		const toolUse = createAssistant(harness, { stopReason: "toolUse", totalTokens: 4_950 });
		toolUse.content = [{ type: "toolCall", id: "large-1", name: tool.name, arguments: { text: "result" } }];
		let requestCount = 0;
		harness.session.agent.streamFunction = () => {
			requestCount++;
			const stream = createAssistantMessageEventStream();
			queueMicrotask(() => stream.push({ type: "done", reason: "toolUse", message: toolUse }));
			return stream;
		};

		const prompt = harness.session.prompt("start");
		await compactionStarted;
		await harness.session.abort();
		await prompt;

		expect(requestCount).toBe(1);
		expect(harness.sessionManager.getBranch().some((entry) => entry.type === "compaction")).toBe(false);
		expect(harness.eventsOfType("compaction_end").at(-1)).toMatchObject({
			reason: "overflow",
			aborted: true,
		});
		expect(harness.eventsOfType("agent_end")).toHaveLength(1);
		expect(harness.eventsOfType("agent_settled")).toHaveLength(1);
		expect(harness.session.messages.at(-1)).toMatchObject({ role: "assistant", stopReason: "aborted" });
		expect(harness.session.isIdle).toBe(true);
	});

	it("injects steering queued during next-turn compaction exactly once", async () => {
		const tool = createLargeResultTool();
		let markCompactionStarted!: () => void;
		const compactionStarted = new Promise<void>((resolve) => {
			markCompactionStarted = resolve;
		});
		let releaseCompaction!: () => void;
		const compactionReleased = new Promise<void>((resolve) => {
			releaseCompaction = resolve;
		});
		const harness = await createHarness({
			models: [{ id: "faux-1", contextWindow: 5_000, maxTokens: 100 }],
			settings: { compaction: { enabled: true, keepRecentTokens: 2_000, reserveTokens: 100 } },
			tools: [tool],
			initialActiveToolNames: [tool.name],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => {
						markCompactionStarted();
						await compactionReleased;
						return {
							compaction: {
								summary: "queued-steering summary",
								firstKeptEntryId: event.preparation.firstKeptEntryId,
								tokensBefore: event.preparation.tokensBefore,
								details: {},
							},
						};
					});
				},
			],
		});
		harnesses.push(harness);
		seedCompactableSession(harness);
		harness.settingsManager.applyOverrides({ compaction: { keepRecentTokens: 2_000 } });
		const toolUse = createAssistant(harness, { stopReason: "toolUse", totalTokens: 4_950 });
		toolUse.content = [{ type: "toolCall", id: "large-1", name: tool.name, arguments: { text: "result" } }];
		let requestCount = 0;
		let secondRequestUserTexts: string[] = [];
		harness.session.agent.streamFunction = (_model, context) => {
			const stream = createAssistantMessageEventStream();
			const message = requestCount === 0 ? toolUse : createAssistant(harness, { stopReason: "stop" });
			if (requestCount > 0) {
				message.content = [{ type: "text", text: "done" }];
				secondRequestUserTexts = context.messages
					.filter((contextMessage) => contextMessage.role === "user")
					.map(getMessageText);
			}
			requestCount++;
			const reason = message.content.some((block) => block.type === "toolCall") ? "toolUse" : "stop";
			queueMicrotask(() => stream.push({ type: "done", reason, message }));
			return stream;
		};

		const prompt = harness.session.prompt("start");
		await compactionStarted;
		await harness.session.prompt("queued during compaction", { streamingBehavior: "steer" });
		releaseCompaction();
		await prompt;

		expect(requestCount).toBe(2);
		expect(secondRequestUserTexts).toContain("queued during compaction");
		const branch = harness.sessionManager.getBranch();
		const compactionIndex = branch.findIndex((entry) => entry.type === "compaction");
		const queuedIndex = branch.findIndex(
			(entry) => entry.type === "message" && getMessageText(entry.message) === "queued during compaction",
		);
		expect(queuedIndex).toBeGreaterThan(compactionIndex);
		expect(harness.session.agent.state.messages).toEqual(harness.sessionManager.buildSessionContext().messages);
		expect(
			harness.session.agent.state.messages.filter(
				(message) => message.role === "user" && getMessageText(message) === "queued during compaction",
			),
		).toHaveLength(1);
	});

	it("fails before another request when the compacted context remains over budget", async () => {
		const tool = createLargeResultTool();
		const harness = await createHarness({
			models: [{ id: "faux-1", contextWindow: 5_000, maxTokens: 100 }],
			settings: { compaction: { enabled: true, keepRecentTokens: 2_000, reserveTokens: 100 } },
			tools: [tool],
			initialActiveToolNames: [tool.name],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => ({
						compaction: {
							summary: "y".repeat(25_000),
							firstKeptEntryId: event.preparation.firstKeptEntryId,
							tokensBefore: event.preparation.tokensBefore,
							details: {},
						},
					}));
				},
			],
		});
		harnesses.push(harness);
		seedCompactableSession(harness);
		harness.settingsManager.applyOverrides({ compaction: { keepRecentTokens: 2_000 } });
		const toolUse = createAssistant(harness, { stopReason: "toolUse", totalTokens: 4_950 });
		toolUse.content = [{ type: "toolCall", id: "large-1", name: tool.name, arguments: { text: "result" } }];
		let requestCount = 0;
		harness.session.agent.streamFunction = () => {
			requestCount++;
			const stream = createAssistantMessageEventStream();
			queueMicrotask(() => stream.push({ type: "done", reason: "toolUse", message: toolUse }));
			return stream;
		};

		await harness.session.prompt("start");

		expect(requestCount).toBe(1);
		expect(harness.sessionManager.getBranch().some((entry) => entry.type === "compaction")).toBe(true);
		expect(harness.session.messages.at(-1)).toMatchObject({
			role: "assistant",
			stopReason: "error",
		});
		expect((harness.session.messages.at(-1) as AssistantMessage).errorMessage).toContain(
			"Context still exceeds the next-turn budget after compaction",
		);
	});

	it("persists parallel tool results in source order before next-turn compaction", async () => {
		const slowTool: AgentTool<typeof echoToolSchema> = {
			...createLargeResultTool(),
			name: "slow",
			execute: async () => {
				await new Promise((resolve) => setTimeout(resolve, 20));
				return { content: [{ type: "text", text: "slow" }], details: {} };
			},
		};
		const fastTool: AgentTool<typeof echoToolSchema> = {
			...createLargeResultTool(),
			name: "fast",
			execute: async () => ({ content: [{ type: "text", text: "fast" }], details: {} }),
		};
		const harness = await createHarness({
			models: [{ id: "faux-1", contextWindow: 5_000, maxTokens: 100 }],
			settings: { compaction: { enabled: true, keepRecentTokens: 20, reserveTokens: 100 } },
			tools: [slowTool, fastTool],
			initialActiveToolNames: [slowTool.name, fastTool.name],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => ({
						compaction: {
							summary: "parallel summary",
							firstKeptEntryId: event.preparation.firstKeptEntryId,
							tokensBefore: event.preparation.tokensBefore,
							details: {},
						},
					}));
				},
			],
		});
		harnesses.push(harness);
		seedCompactableSession(harness);
		harness.settingsManager.applyOverrides({ compaction: { keepRecentTokens: 20 } });
		const toolUse = createAssistant(harness, { stopReason: "toolUse", totalTokens: 4_950 });
		toolUse.content = [
			{ type: "toolCall", id: "slow-1", name: "slow", arguments: { text: "slow" } },
			{ type: "toolCall", id: "fast-1", name: "fast", arguments: { text: "fast" } },
		];
		let requestCount = 0;
		harness.session.agent.streamFunction = () => {
			const stream = createAssistantMessageEventStream();
			const message = requestCount === 0 ? toolUse : createAssistant(harness, { stopReason: "stop" });
			if (requestCount > 0) message.content = [{ type: "text", text: "done" }];
			requestCount++;
			const reason = message.content.some((block) => block.type === "toolCall") ? "toolUse" : "stop";
			queueMicrotask(() => stream.push({ type: "done", reason, message }));
			return stream;
		};

		await harness.session.prompt("start");

		const branch = harness.sessionManager.getBranch();
		const toolResultIds = branch.flatMap((entry) =>
			entry.type === "message" && entry.message.role === "toolResult" ? [entry.message.toolCallId] : [],
		);
		expect(toolResultIds.slice(-2)).toEqual(["slow-1", "fast-1"]);
		const compactionIndex = branch.findIndex((entry) => entry.type === "compaction");
		const lastToolResultIndex = branch
			.map((entry) => entry.type === "message" && entry.message.role === "toolResult")
			.lastIndexOf(true);
		expect(compactionIndex).toBeGreaterThan(lastToolResultIndex);
		expect(requestCount).toBe(2);
	});

	it("auto-compacts with a custom streamFn when registry auth is absent", async () => {
		const harness = await createHarness({ withConfiguredAuth: false });
		harnesses.push(harness);
		seedCompactableSession(harness);
		const getStreamCallCount = useSummaryStreamFn(harness, "auto summary from custom stream");
		const sessionInternals = harness.session as unknown as SessionWithCompactionInternals;

		await sessionInternals._runAutoCompaction("threshold", false);

		const compactionEntries = harness.sessionManager.getEntries().filter((entry) => entry.type === "compaction");
		const compactionEnd = harness.eventsOfType("compaction_end").at(-1);
		expect(compactionEntries).toHaveLength(1);
		expect(compactionEnd?.result?.estimatedTokensAfter).toBeGreaterThan(0);
		expect(getStreamCallCount()).toBe(1);
	});

	it("cancels in-progress manual compaction when abortCompaction is called", async () => {
		const harness = await createHarness({
			settings: { compaction: { keepRecentTokens: 1 } },
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => {
						return await new Promise<{ cancel: true }>((resolve) => {
							event.signal.addEventListener("abort", () => resolve({ cancel: true }), { once: true });
						});
					});
				},
			],
		});
		harnesses.push(harness);

		await harness.session.prompt("one");
		await harness.session.prompt("two");

		const compactPromise = harness.session.compact();
		await new Promise((resolve) => setTimeout(resolve, 0));
		harness.session.abortCompaction();

		await expect(compactPromise).rejects.toThrow("Compaction cancelled");
	});

	it("resumes after threshold compaction when only agent-level queued messages exist", async () => {
		vi.useFakeTimers();
		const harness = await createHarness({
			settings: { compaction: { keepRecentTokens: 1 } },
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => ({
						compaction: {
							summary: "auto compacted",
							firstKeptEntryId: event.preparation.firstKeptEntryId,
							tokensBefore: event.preparation.tokensBefore,
							details: {},
						},
					}));
				},
			],
		});
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage("one"), fauxAssistantMessage("two")]);
		await harness.session.prompt("first");
		await harness.session.prompt("second");

		harness.session.agent.followUp({
			role: "custom",
			customType: "test",
			content: [{ type: "text", text: "queued custom" }],
			display: false,
			timestamp: Date.now(),
		});

		const sessionInternals = harness.session as unknown as SessionWithCompactionInternals;

		await expect(sessionInternals._runAutoCompaction("threshold", false)).resolves.toBe(true);
	});

	it("does not retry overflow recovery more than once", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const sessionInternals = harness.session as unknown as SessionWithCompactionInternals;
		const overflowMessage = createAssistant(harness, {
			stopReason: "error",
			errorMessage: "prompt is too long",
			timestamp: Date.now(),
		});
		const runAutoCompactionSpy = vi.spyOn(sessionInternals, "_runAutoCompaction").mockResolvedValue(false);
		const compactionErrors: string[] = [];
		harness.session.subscribe((event) => {
			if (event.type === "compaction_end" && event.errorMessage) {
				compactionErrors.push(event.errorMessage);
			}
		});

		await sessionInternals._checkCompaction(overflowMessage);
		await sessionInternals._checkCompaction({ ...overflowMessage, timestamp: Date.now() + 1 });

		expect(runAutoCompactionSpy).toHaveBeenCalledTimes(1);
		expect(compactionErrors).toContain(
			"Context overflow recovery failed after one compact-and-retry attempt. Try reducing context or switching to a larger-context model.",
		);
	});

	it("compacts successful overflow responses without retrying", async () => {
		const harness = await createHarness({
			settings: { compaction: { enabled: true, keepRecentTokens: 1, reserveTokens: 0 } },
			models: [{ id: "faux-1", contextWindow: 1, maxTokens: 100 }],
			extensionFactories: [
				(pi) => {
					pi.on("session_before_compact", async (event) => ({
						compaction: {
							summary: "successful overflow compacted",
							firstKeptEntryId: event.preparation.firstKeptEntryId,
							tokensBefore: event.preparation.tokensBefore,
							details: {},
						},
					}));
				},
			],
		});
		harnesses.push(harness);
		harness.setResponses([fauxAssistantMessage("completed answer")]);

		await expect(harness.session.prompt("hello")).resolves.toBeUndefined();

		const compactionEnd = harness.eventsOfType("compaction_end").at(-1);
		expect(compactionEnd).toMatchObject({
			reason: "overflow",
			aborted: false,
			willRetry: false,
		});
		expect(harness.faux.state.callCount).toBe(1);
	});

	it("ignores stale pre-compaction assistant usage on pre-prompt checks", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const sessionInternals = harness.session as unknown as SessionWithCompactionInternals;
		const staleTimestamp = Date.now() - 10_000;
		const staleAssistant = createAssistant(harness, {
			stopReason: "stop",
			totalTokens: 610_000,
			timestamp: staleTimestamp,
		});

		harness.sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "before compaction" }],
			timestamp: staleTimestamp - 1000,
		});
		harness.sessionManager.appendMessage(staleAssistant);
		const firstKeptEntryId = harness.sessionManager.getEntries()[0]!.id;
		harness.sessionManager.appendCompaction(
			"summary",
			firstKeptEntryId,
			staleAssistant.usage.totalTokens,
			undefined,
			false,
		);
		harness.sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "after compaction" }],
			timestamp: Date.now(),
		});

		const runAutoCompactionSpy = vi.spyOn(sessionInternals, "_runAutoCompaction").mockResolvedValue(false);

		await sessionInternals._checkCompaction(staleAssistant, false);

		expect(runAutoCompactionSpy).not.toHaveBeenCalled();
	});

	it("triggers threshold compaction for error messages using the last successful usage", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const sessionInternals = harness.session as unknown as SessionWithCompactionInternals;
		const successfulAssistant = createAssistant(harness, {
			stopReason: "stop",
			totalTokens: 190_000,
			timestamp: Date.now(),
		});
		const errorAssistant = createAssistant(harness, {
			stopReason: "error",
			errorMessage: "529 overloaded",
			timestamp: Date.now() + 1000,
		});
		harness.session.agent.state.messages = [
			{ role: "user", content: [{ type: "text", text: "hello" }], timestamp: Date.now() - 1000 },
			successfulAssistant,
			{ role: "user", content: [{ type: "text", text: "retry" }], timestamp: Date.now() + 500 },
			errorAssistant,
		];

		const runAutoCompactionSpy = vi.spyOn(sessionInternals, "_runAutoCompaction").mockResolvedValue(false);

		await sessionInternals._checkCompaction(errorAssistant);

		expect(runAutoCompactionSpy).toHaveBeenCalledWith("threshold", false);
	});

	it("does not trigger threshold compaction for error messages when no prior usage exists", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const sessionInternals = harness.session as unknown as SessionWithCompactionInternals;
		const errorAssistant = createAssistant(harness, {
			stopReason: "error",
			errorMessage: "529 overloaded",
			timestamp: Date.now(),
		});
		harness.session.agent.state.messages = [
			{ role: "user", content: [{ type: "text", text: "hello" }], timestamp: Date.now() - 1000 },
			errorAssistant,
		];

		const runAutoCompactionSpy = vi.spyOn(sessionInternals, "_runAutoCompaction").mockResolvedValue(false);

		await sessionInternals._checkCompaction(errorAssistant);

		expect(runAutoCompactionSpy).not.toHaveBeenCalled();
	});

	it("does not trigger threshold compaction when only kept pre-compaction usage exists", async () => {
		const harness = await createHarness();
		harnesses.push(harness);
		const sessionInternals = harness.session as unknown as SessionWithCompactionInternals;
		const preCompactionTimestamp = Date.now() - 10_000;
		const keptAssistant = createAssistant(harness, {
			stopReason: "stop",
			totalTokens: 190_000,
			timestamp: preCompactionTimestamp,
		});

		harness.sessionManager.appendMessage({
			role: "user",
			content: [{ type: "text", text: "before compaction" }],
			timestamp: preCompactionTimestamp - 1000,
		});
		harness.sessionManager.appendMessage(keptAssistant);
		const firstKeptEntryId = harness.sessionManager.getEntries()[0]!.id;
		harness.sessionManager.appendCompaction(
			"summary",
			firstKeptEntryId,
			keptAssistant.usage.totalTokens,
			undefined,
			false,
		);

		const errorAssistant = createAssistant(harness, {
			stopReason: "error",
			errorMessage: "529 overloaded",
			timestamp: Date.now(),
		});
		harness.session.agent.state.messages = [
			{ role: "user", content: [{ type: "text", text: "kept user" }], timestamp: preCompactionTimestamp - 1000 },
			keptAssistant,
			{ role: "user", content: [{ type: "text", text: "new prompt" }], timestamp: Date.now() - 500 },
			errorAssistant,
		];

		const runAutoCompactionSpy = vi.spyOn(sessionInternals, "_runAutoCompaction").mockResolvedValue(false);

		await sessionInternals._checkCompaction(errorAssistant);

		expect(runAutoCompactionSpy).not.toHaveBeenCalled();
	});

	it("does not trigger threshold compaction below the threshold or when disabled", async () => {
		const belowThresholdHarness = await createHarness({
			settings: { compaction: { enabled: true, reserveTokens: 1000 } },
			models: [{ id: "faux-1", contextWindow: 200_000 }],
		});
		harnesses.push(belowThresholdHarness);
		const disabledHarness = await createHarness({ settings: { compaction: { enabled: false } } });
		harnesses.push(disabledHarness);

		const belowThresholdInternals = belowThresholdHarness.session as unknown as SessionWithCompactionInternals;
		const disabledInternals = disabledHarness.session as unknown as SessionWithCompactionInternals;
		const belowThresholdSpy = vi.spyOn(belowThresholdInternals, "_runAutoCompaction").mockResolvedValue(false);
		const disabledSpy = vi.spyOn(disabledInternals, "_runAutoCompaction").mockResolvedValue(false);

		await belowThresholdInternals._checkCompaction(
			createAssistant(belowThresholdHarness, { stopReason: "stop", totalTokens: 1_000, timestamp: Date.now() }),
		);
		await disabledInternals._checkCompaction(
			createAssistant(disabledHarness, { stopReason: "stop", totalTokens: 1_000_000, timestamp: Date.now() }),
		);

		expect(belowThresholdSpy).not.toHaveBeenCalled();
		expect(disabledSpy).not.toHaveBeenCalled();
	});
});
