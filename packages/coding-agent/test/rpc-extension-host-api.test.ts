import { describe, expect, it, vi } from "vitest";
import { createEventBus } from "../src/core/event-bus.ts";
import { createExtensionRuntime, loadExtensionFromFactory } from "../src/core/extensions/loader.ts";
import type { ExtensionAPI } from "../src/core/extensions/types.ts";

describe("extension host request API", () => {
	it("rejects outside RPC mode and delegates after the RPC runtime binds a handler", async () => {
		const runtime = createExtensionRuntime();
		let api: ExtensionAPI | undefined;
		await loadExtensionFromFactory(
			(extensionApi) => {
				api = extensionApi;
			},
			process.cwd(),
			createEventBus(),
			runtime,
		);

		await expect(api!.requestHost("example", null)).rejects.toThrow("only available in RPC mode");

		const requestHost = vi.fn(async () => ({ accepted: true }));
		runtime.requestHost = requestHost;
		await expect(api!.requestHost<{ accepted: boolean }>("example", { value: 1 })).resolves.toEqual({
			accepted: true,
		});
		expect(requestHost).toHaveBeenCalledWith("example", { value: 1 }, undefined);
	});
});
