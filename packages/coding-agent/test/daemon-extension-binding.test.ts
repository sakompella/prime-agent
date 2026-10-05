import { existsSync, mkdirSync, rmSync } from "node:fs";
import { Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fauxAssistantMessage, registerFauxProvider } from "@earendil-works/pi-ai";
import { afterEach, describe, expect, it } from "vitest";
import {
	type CreateAgentSessionRuntimeFactory,
	createAgentSessionFromServices,
	createAgentSessionRuntime,
	createAgentSessionServices,
} from "../src/core/agent-session-runtime.js";
import { AuthStorage } from "../src/core/auth-storage.js";

import { SessionManager } from "../src/core/session-manager.js";
import type { ExtensionAPI, ExtensionFactory, ExtensionUIContext } from "../src/index.js";

import type { ActiveSessionState, DaemonSocketClient } from "../src/modes/daemon/active-session-state.js";
import { bindActiveSessionState } from "../src/modes/daemon/daemon-extension-binding.js";
import {
	setDaemonClientSessionCapabilities,
	shouldSendDaemonOutboundToClient,
} from "../src/modes/daemon/daemon-mode.js";
import type { DaemonOutbound } from "../src/modes/daemon/daemon-protocol.js";

describe("daemon extension binding", () => {
	const cleanups: Array<() => Promise<void> | void> = [];

	afterEach(async () => {
		while (cleanups.length > 0) {
			await cleanups.pop()?.();
		}
	});

	async function createRuntimeForTest(extensionFactory: ExtensionFactory, responses: string[]) {
		const tempDir = join(tmpdir(), `pi-daemon-extension-${Date.now()}-${Math.random().toString(36).slice(2)}`);
		mkdirSync(tempDir, { recursive: true });

		const faux = registerFauxProvider({
			models: [{ id: "faux-daemon", reasoning: false }],
		});
		faux.setResponses(responses.map((response) => fauxAssistantMessage(response)));

		const authStorage = AuthStorage.inMemory();
		authStorage.setRuntimeApiKey(faux.getModel().provider, "faux-key");

		const createRuntime: CreateAgentSessionRuntimeFactory = async ({ cwd, sessionManager, sessionStartEvent }) => {
			const services = await createAgentSessionServices({
				cwd,
				agentDir: tempDir,
				authStorage,
				resourceLoaderOptions: {
					extensionFactories: [
						(pi: ExtensionAPI) => {
							pi.registerProvider(faux.getModel().provider, {
								baseUrl: faux.getModel().baseUrl,
								apiKey: "faux-key",
								api: faux.api,
								models: faux.models.map((registeredModel) => ({
									id: registeredModel.id,
									name: registeredModel.name,
									api: registeredModel.api,
									reasoning: registeredModel.reasoning,
									input: registeredModel.input,
									cost: registeredModel.cost,
									contextWindow: registeredModel.contextWindow,
									maxTokens: registeredModel.maxTokens,
								})),
							});
							extensionFactory(pi);
						},
					],
					noSkills: true,
					noPromptTemplates: true,
					noThemes: true,
				},
			});
			return {
				...(await createAgentSessionFromServices({
					services,
					sessionManager,
					sessionStartEvent,
					model: faux.getModel(),
				})),
				services,
				diagnostics: services.diagnostics,
			};
		};

		const runtime = await createAgentSessionRuntime(createRuntime, {
			cwd: tempDir,
			agentDir: tempDir,
			sessionManager: SessionManager.create(tempDir, join(tempDir, "sessions")),
		});

		cleanups.push(async () => {
			await runtime.dispose();
			faux.unregister();
			if (existsSync(tempDir)) {
				rmSync(tempDir, { recursive: true, force: true });
			}
		});

		return runtime;
	}

	it.each(["parent", "headless-child", "legacy"])(
		"only waits for dialogs when the %s session has a UI recipient",
		async (activeSessionId) => {
			let ui: ExtensionUIContext | undefined;
			const runtime = await createRuntimeForTest((pi) => {
				pi.on("session_start", (_event, ctx) => {
					ui = ctx.ui;
				});
			}, []);
			const socket = new Socket();
			cleanups.push(() => {
				socket.destroy();
			});
			const client: DaemonSocketClient = {
				id: "multiplexed-client",
				socket,
				detachInput: () => {},
				attachedActiveSessionIds: new Set(["parent", "headless-child", "legacy"]),
				capabilities: new Set(["extension_ui"]),
				supportsExtensionUi: true,
			};
			setDaemonClientSessionCapabilities(client, "parent", new Set(["extension_ui"]));
			setDaemonClientSessionCapabilities(client, "headless-child", new Set());
			const state: ActiveSessionState = {
				activeSessionId,
				runtime,
				clients: new Set([client]),
				pendingAttaches: 0,
				extensionUiRequests: new Map(),
				eventGeneration: "dialogs",
				lastEventSequence: 0,
			};
			const delivered: DaemonOutbound[] = [];
			await bindActiveSessionState(state, {
				broadcast: (_state, message) => {
					if (shouldSendDaemonOutboundToClient(client, message)) delivered.push(message);
				},
				shutdown: () => {},
			});
			if (!ui) throw new Error("Extension UI was not bound");
			const dialogs = [
				{
					method: "select",
					result: ui.select("Choose", ["selected"]),
					fallback: undefined,
					response: { value: "selected" },
					expected: "selected",
				},
				{
					method: "confirm",
					result: ui.confirm("Approval", "Allow?"),
					fallback: false,
					response: { confirmed: true },
					expected: true,
				},
				{
					method: "input",
					result: ui.input("Input"),
					fallback: undefined,
					response: { value: "typed" },
					expected: "typed",
				},
				{
					method: "editor",
					result: ui.editor("Editor"),
					fallback: undefined,
					response: { value: "edited" },
					expected: "edited",
				},
			];
			try {
				if (activeSessionId === "headless-child") {
					expect(delivered.filter((message) => message.type === "extension_ui_request")).toEqual([]);
					expect(state.extensionUiRequests.size).toBe(0);
					for (const dialog of dialogs) await expect(dialog.result).resolves.toBe(dialog.fallback);
				} else {
					for (const dialog of dialogs) {
						const request = delivered.find(
							(message) => message.type === "extension_ui_request" && message.method === dialog.method,
						);
						if (!request || request.type !== "extension_ui_request") throw new Error("Dialog was not delivered");
						const pending = state.extensionUiRequests.get(request.id);
						if (!pending) throw new Error("Dialog did not wait for a response");
						pending.resolve(dialog.response);
						await expect(dialog.result).resolves.toBe(dialog.expected);
					}
				}
				expect(state.extensionUiRequests.size).toBe(0);
			} finally {
				for (const pending of [...state.extensionUiRequests.values()]) pending.resolve({ cancelled: true });
				await Promise.all(dialogs.map((dialog) => dialog.result));
			}
		},
	);

	it("strips the duplicated partial message from broadcast message_update events", async () => {
		const runtime = await createRuntimeForTest(() => {}, ["streamed reply"]);

		const outbound: DaemonOutbound[] = [];
		const state: ActiveSessionState = {
			activeSessionId: "active-slim",
			runtime,
			clients: new Set(),
			pendingAttaches: 0,
			extensionUiRequests: new Map(),
			eventGeneration: "generation-slim",
			lastEventSequence: 0,
		};
		await bindActiveSessionState(state, {
			broadcast: (_state, message) => {
				outbound.push(message);
			},
			shutdown: () => {},
		});

		await runtime.session.prompt("hello");

		const updates = outbound.filter(
			(message): message is Extract<DaemonOutbound, { type: "session_event" }> =>
				message.type === "session_event" && message.event.type === "message_update",
		);
		expect(updates.length).toBeGreaterThan(0);
		for (const update of updates) {
			expect(update.event).toHaveProperty("message");
			expect(update.event).toHaveProperty("assistantMessageEvent");
			expect((update.event as { assistantMessageEvent: object }).assistantMessageEvent).not.toHaveProperty(
				"partial",
			);
		}
	});
});
