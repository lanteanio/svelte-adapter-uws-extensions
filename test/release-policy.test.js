import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import semver from "semver";
import { releaseKey, validateCandidateLifecycles, validateRoutingEvents, channelForVersion, eventTime } from "../scripts/release-policy.js";

const read = (path) =>
	readFileSync(fileURLToPath(new URL("../" + path, import.meta.url)), "utf8");

const MANIFEST = read("docs/release-manifest.md");

function cells(line) {
	return line
		.split("|")
		.slice(1, -1)
		.map((cell) => cell.trim());
}

function markdownTable(heading) {
	const lines = MANIFEST.split("\n");
	const start = lines.indexOf(heading);
	if (start === -1) throw new Error("missing manifest heading " + heading);
	const end = lines.findIndex(
		(line, index) => index > start && line.startsWith("## "),
	);
	const section = lines.slice(start + 1, end === -1 ? lines.length : end);
	const table = section.filter((line) => line.startsWith("|"));
	if (table.length < 2) throw new Error("missing table under " + heading);
	return {
		headers: cells(table[0]),
		rows: table.slice(2).map(cells),
	};
}

function objects(table) {
	return table.rows.map((row) =>
		Object.fromEntries(
			table.headers.map((header, index) => [header, row[index]]),
		),
	);
}

function publishedRows() {
	return objects(markdownTable("## Published releases"));
}

function candidateEvents() {
	return objects(markdownTable("## Candidate event log"));
}

function routingEvents() {
	return objects(
		markdownTable("## Routing, corrections and rollback events"),
	);
}

describe("release lineage contract", () => {
	it("separates knowable candidate events from immutable registry identities", () => {
		expect(markdownTable("## Candidate event log").headers).toEqual([
			"Candidate",
			"Event",
			"Event UTC",
			"Git head",
			"Planned tag",
			"Target channel",
			"Evidence",
		]);
		expect(markdownTable("## Published releases").headers).toEqual([
			"Package",
			"Version",
			"Target channel",
			"Git head",
			"Git tag",
			"npm integrity",
			"npm shasum",
			"Published UTC",
			"Notes",
		]);
		expect(
			markdownTable("## Routing, corrections and rollback events")
				.headers,
		).toEqual([
			"Event ID",
			"Event UTC",
			"Event",
			"Corrected event",
			"Corrected UTC",
			"Package",
			"Channel",
			"From version",
			"To version",
			"Corrects event",
			"Evidence",
			"Notes",
		]);

		const events = candidateEvents();
		for (const event of events) {
			const version = event.Candidate.slice("svelte-adapter-uws-extensions@".length);
			expect(() => channelForVersion(version)).not.toThrow();
			expect(["proposed", "tagged", "aborted", "published"]).toContain(
				event.Event,
			);
			expect(() =>
				eventTime(event["Event UTC"], "candidate event"),
			).not.toThrow();
			expect(event["Git head"]).toMatch(/^[0-9a-f]{40}$/);
			expect(event["Planned tag"]).toBe(event.Candidate);
			expect(["latest", "next"]).toContain(event["Target channel"]);
			expect(event.Evidence.length).toBeGreaterThan(0);
		}
		const lifecycles = validateCandidateLifecycles(events, publishedRows());
		expect(() =>
			validateRoutingEvents(routingEvents(), publishedRows(), lifecycles),
		).not.toThrow();
	});

	it("enforces transitions and one immutable identity throughout a candidate lifecycle", () => {
		const candidate = "svelte-adapter-uws-extensions@9.9.9-next.1";
		const gitHead = "a".repeat(40);
		const event = (name, overrides = {}) => ({
			Candidate: candidate,
			Event: name,
			"Event UTC": "2026-08-01T01:00:00.000Z",
			"Git head": gitHead,
			"Planned tag": candidate,
			"Target channel": "next",
			Evidence: "retained artifact",
			...overrides,
		});
		const release = {
			Package: "svelte-adapter-uws-extensions",
			Version: "9.9.9-next.1",
			"Target channel": "next",
			"Git head": gitHead,
			"Git tag": candidate,
			"Published UTC": "2026-08-01T01:00:00.000Z",
		};
		const publishedEvents = [
			event("proposed"),
			event("tagged"),
			event("published"),
		];

		expect(() =>
			validateCandidateLifecycles(publishedEvents, [release]),
		).not.toThrow();
		expect(() =>
			validateCandidateLifecycles(
				[event("proposed"), event("tagged"), event("aborted")],
				[],
			),
		).not.toThrow();
		expect(() =>
			validateCandidateLifecycles(
				[event("proposed"), event("aborted")],
				[],
			),
		).not.toThrow();

		expect(() =>
			validateCandidateLifecycles(
				[event("proposed"), event("published")],
				[release],
			),
		).toThrow("invalid candidate transition");
		expect(() =>
			validateCandidateLifecycles(
				[event("proposed"), event("tagged"), event("published")],
				[],
			),
		).toThrow("no immutable row");
		expect(() =>
			validateCandidateLifecycles(
				[event("proposed"), event("aborted")],
				[release],
			),
		).toThrow("aborted candidate has an immutable row");

		for (const [field, value, message] of [
			["Git head", "b".repeat(40), "candidate identity changed"],
			[
				"Planned tag",
				"svelte-adapter-uws-extensions@9.9.9-next.2",
				"planned tag differs",
			],
			[
				"Target channel",
				"latest",
				"candidate version has the wrong target channel",
			],
		]) {
			const mutated = publishedEvents.map((entry, index) =>
				index === 1 ? { ...entry, [field]: value } : entry,
			);
			expect(() =>
				validateCandidateLifecycles(mutated, [release]),
			).toThrow(message);
		}
		expect(() =>
			validateCandidateLifecycles(publishedEvents, [
				{ ...release, "Git head": "b".repeat(40) },
			]),
		).toThrow("published Git head differs");
		expect(() =>
			validateCandidateLifecycles(publishedEvents, [
				{ ...release, "Git tag": "svelte-adapter-uws-extensions@9.9.9-next.2" },
			]),
		).toThrow("published Git tag differs");
		expect(() =>
			validateCandidateLifecycles(publishedEvents, [
				{ ...release, "Target channel": "latest" },
			]),
		).toThrow("release version has the wrong target channel");
		expect(() =>
			validateCandidateLifecycles(
				[event("proposed", { "Target channel": "latest" })],
				[],
			),
		).toThrow("candidate version has the wrong target channel");
		expect(() =>
			validateCandidateLifecycles(
				[
					event("proposed", {
						Candidate: "svelte-adapter-uws-extensions@9.9.9",
						"Planned tag": "svelte-adapter-uws-extensions@9.9.9",
						"Target channel": "next",
					}),
				],
				[],
			),
		).toThrow("candidate version has the wrong target channel");
		expect(() =>
			validateCandidateLifecycles(
				[
					event("proposed", {
						"Event UTC": "2026-08-01T00:00:00.000Z",
					}),
					event("tagged", {
						"Event UTC": "2026-08-01T00:30:00.000Z",
					}),
					event("published", {
						"Event UTC": "2026-08-01T00:59:59.000Z",
					}),
				],
				[release],
			),
		).toThrow("published candidate event predates registry publication");
		expect(() =>
			validateCandidateLifecycles(
				[
					event("proposed", {
						"Event UTC": "2026-08-01T01:01:00.000Z",
					}),
					event("tagged", {
						"Event UTC": "2026-08-01T01:02:00.000Z",
					}),
					event("published", {
						"Event UTC": "2026-08-01T01:03:00.000Z",
					}),
				],
				[release],
			),
		).toThrow("proposed candidate event follows registry publication");
		expect(() =>
			validateCandidateLifecycles(
				[
					event("proposed", {
						Candidate: "svelte-adapter-uws-extensions@1.2.3-next..1",
						"Planned tag": "svelte-adapter-uws-extensions@1.2.3-next..1",
					}),
				],
				[],
			),
		).toThrow("invalid candidate identity");
		expect(() =>
			validateCandidateLifecycles(
				[
					event("proposed", {
						Candidate: "svelte-adapter-uws-extensions@1.2.3+duplicate",
						"Planned tag": "svelte-adapter-uws-extensions@1.2.3+duplicate",
						"Target channel": "latest",
					}),
				],
				[],
			),
		).toThrow("invalid candidate identity");
		expect(() =>
			validateCandidateLifecycles(
				[event("proposed", { "Event UTC": "2026-08-01" })],
				[],
			),
		).toThrow("invalid candidate event");
		expect(() =>
			validateCandidateLifecycles(publishedEvents, [
				{ ...release, "Published UTC": "2026-08-01" },
			]),
		).toThrow("invalid published release");
	});

	it("validates stateful quarantine, routing, rollback and complete corrections", () => {
		const release = (version, channel, options = {}) => ({
			Package: "svelte-adapter-uws-extensions",
			Version: version,
			"Target channel": channel,
			"Git head": options.gitHead ?? "f".repeat(40),
			"Git tag": options.tag ?? "legacy-none",
			"Published UTC": options.published ?? "2026-07-31T23:00:00.000Z",
		});
		const nextRelease = release("0.6.0-next.91", "next", {
			tag: "svelte-adapter-uws-extensions@0.6.0-next.91",
			gitHead: "a".repeat(40),
			published: "2026-08-01T01:00:00.000Z",
		});
		const stableRelease = release("0.6.0", "latest", {
			tag: "svelte-adapter-uws-extensions@0.6.0",
			gitHead: "b".repeat(40),
			published: "2026-08-01T01:05:00.000Z",
		});
		const fixedNextRelease = release("0.6.0-next.92", "next", {
			tag: "svelte-adapter-uws-extensions@0.6.0-next.92",
			gitHead: "c".repeat(40),
			published: "2026-08-01T03:00:00.000Z",
		});
		const hotfixRelease = release("0.5.9", "latest", {
			tag: "svelte-adapter-uws-extensions@0.5.9",
			gitHead: "d".repeat(40),
			published: "2026-08-01T01:06:00.000Z",
		});
		const stableDowngradeRelease = release("0.5.7", "latest", {
			tag: "svelte-adapter-uws-extensions@0.5.7",
			gitHead: "e".repeat(40),
			published: "2026-08-01T01:07:00.000Z",
		});
		const nextDowngradeRelease = release("0.5.0-next.1", "next", {
			tag: "svelte-adapter-uws-extensions@0.5.0-next.1",
			gitHead: "1".repeat(40),
			published: "2026-08-01T01:08:00.000Z",
		});
		const prereleaseEdgeRelease = release("0.6.0-next.90.1", "next", {
			tag: "svelte-adapter-uws-extensions@0.6.0-next.90.1",
			gitHead: "2".repeat(40),
			published: "2026-08-01T01:09:00.000Z",
		});
		const majorRelease = release("1.0.0", "latest", {
			tag: "svelte-adapter-uws-extensions@1.0.0",
			gitHead: "3".repeat(40),
			published: "2026-08-01T01:10:00.000Z",
		});
		const releases = [
			release("0.5.8", "latest"),
			release("0.6.0-next.90", "next"),
			nextRelease,
			stableRelease,
		];
		const candidateEvent = (releaseRow, name, utc) => ({
			Candidate: releaseKey(releaseRow),
			Event: name,
			"Event UTC": utc,
			"Git head": releaseRow["Git head"],
			"Planned tag": releaseRow["Git tag"],
			"Target channel": releaseRow["Target channel"],
			Evidence: "retained release evidence",
		});
		const lifecycleEvents = [
			candidateEvent(nextRelease, "proposed", "2026-08-01T00:00:00.000Z"),
			candidateEvent(nextRelease, "tagged", "2026-08-01T00:30:00.000Z"),
			candidateEvent(
				nextRelease,
				"published",
				"2026-08-01T01:00:00.000Z",
			),
			candidateEvent(
				stableRelease,
				"proposed",
				"2026-08-01T00:05:00.000Z",
			),
			candidateEvent(stableRelease, "tagged", "2026-08-01T00:35:00.000Z"),
			candidateEvent(
				stableRelease,
				"published",
				"2026-08-01T01:05:00.000Z",
			),
		];
		const lifecycles = validateCandidateLifecycles(
			lifecycleEvents,
			releases,
		);
		const nextOnlyReleases = releases.slice(0, 3);
		const nextOnlyLifecycles = validateCandidateLifecycles(
			lifecycleEvents.slice(0, 3),
			nextOnlyReleases,
		);
		const fixedNextEvents = [
			candidateEvent(
				fixedNextRelease,
				"proposed",
				"2026-08-01T02:10:00.000Z",
			),
			candidateEvent(
				fixedNextRelease,
				"tagged",
				"2026-08-01T02:20:00.000Z",
			),
			candidateEvent(
				fixedNextRelease,
				"published",
				"2026-08-01T03:00:00.000Z",
			),
		];
		const fixForwardReleases = [...nextOnlyReleases, fixedNextRelease];
		const fixForwardLifecycles = validateCandidateLifecycles(
			[...lifecycleEvents.slice(0, 3), ...fixedNextEvents],
			fixForwardReleases,
		);
		const hotfixReleases = [releases[0], releases[1], hotfixRelease];
		const hotfixLifecycles = validateCandidateLifecycles(
			[
				candidateEvent(
					hotfixRelease,
					"proposed",
					"2026-08-01T00:06:00.000Z",
				),
				candidateEvent(
					hotfixRelease,
					"tagged",
					"2026-08-01T00:36:00.000Z",
				),
				candidateEvent(
					hotfixRelease,
					"published",
					"2026-08-01T01:06:00.000Z",
				),
			],
			hotfixReleases,
		);
		const route = (
			id,
			eventName,
			channel,
			from,
			to,
			utc,
			overrides = {},
		) => ({
			"Event ID": id,
			"Event UTC": utc,
			Event: eventName,
			"Corrected event": "none",
			"Corrected UTC": "none",
			Package: "svelte-adapter-uws-extensions",
			Channel: channel,
			"From version": from,
			"To version": to,
			"Corrects event": "none",
			Evidence: "npm dist-tags exact response",
			Notes: "verified pointer mutation",
			...overrides,
		});
		const correction = (
			id,
			corrects,
			correctedEvent,
			correctedUtc,
			channel,
			from,
			to,
			utc,
			overrides = {},
		) =>
			route(id, "correction", channel, from, to, utc, {
				"Corrected event": correctedEvent,
				"Corrected UTC": correctedUtc,
				"Corrects event": corrects,
				...overrides,
			});
		const lifecycleFor = (releaseRow) =>
			validateCandidateLifecycles(
				[
					candidateEvent(
						releaseRow,
						"proposed",
						"2026-08-01T00:00:00.000Z",
					),
					candidateEvent(
						releaseRow,
						"tagged",
						"2026-08-01T00:30:00.000Z",
					),
					candidateEvent(
						releaseRow,
						"published",
						releaseRow["Published UTC"],
					),
				],
				[releases[0], releases[1], releaseRow],
			);
		const publicRoute = (releaseRow, fromVersion, prefix) => [
			route(
				prefix + "-candidate-open",
				"routed",
				"candidate",
				"none",
				releaseRow.Version,
				"2026-08-01T02:00:00.000Z",
			),
			route(
				prefix + "-target-route",
				"routed",
				releaseRow["Target channel"],
				fromVersion,
				releaseRow.Version,
				"2026-08-01T02:01:00.000Z",
			),
			route(
				prefix + "-candidate-clear",
				"routed",
				"candidate",
				releaseRow.Version,
				"none",
				"2026-08-01T02:02:00.000Z",
			),
		];
		const valid = [
			route(
				"candidate-route-1",
				"routed",
				"candidate",
				"none",
				"0.6.0-next.91",
				"2026-08-01T02:00:00.000Z",
			),
			route(
				"next-route-1",
				"routed",
				"next",
				"0.6.0-next.90",
				"0.6.0-next.91",
				"2026-08-01T02:01:00.000Z",
			),
			route(
				"candidate-clear-1",
				"routed",
				"candidate",
				"0.6.0-next.91",
				"none",
				"2026-08-01T02:02:00.000Z",
			),
			route(
				"next-rollback-1",
				"rollback",
				"next",
				"0.6.0-next.91",
				"0.6.0-next.90",
				"2026-08-01T02:03:00.000Z",
			),
			route(
				"stable-candidate-route-1",
				"routed",
				"candidate",
				"none",
				"0.6.0",
				"2026-08-01T02:04:00.000Z",
			),
			route(
				"latest-route-1",
				"routed",
				"latest",
				"0.5.8",
				"0.6.0",
				"2026-08-01T02:05:00.000Z",
			),
			route(
				"stable-candidate-clear-1",
				"routed",
				"candidate",
				"0.6.0",
				"none",
				"2026-08-01T02:06:00.000Z",
			),
			route(
				"latest-rollback-1",
				"rollback",
				"latest",
				"0.6.0",
				"0.5.8",
				"2026-08-01T02:07:00.000Z",
			),
			correction(
				"next-route-correction-1",
				"next-route-1",
				"routed",
				"2026-08-01T02:01:00.000Z",
				"next",
				"0.6.0-next.90",
				"0.6.0-next.91",
				"2026-08-01T03:00:00.000Z",
				{
					Evidence: "corrected immutable npm evidence",
				},
			),
			correction(
				"next-route-correction-2",
				"next-route-correction-1",
				"routed",
				"2026-08-01T02:01:30.000Z",
				"next",
				"0.6.0-next.90",
				"0.6.0-next.91",
				"2026-08-01T04:00:00.000Z",
				{
					Evidence: "corrected immutable npm evidence",
					Notes: "corrects the routing time in the latest correction",
				},
			),
		];
		const routed = validateRoutingEvents(valid, releases, lifecycles);
		expect(routed.get("latest")).toBe("0.5.8");
		expect(routed.get("next")).toBe("0.6.0-next.90");
		expect(routed.get("candidate")).toBe("none");

		const hotfixForward = validateRoutingEvents(
			publicRoute(hotfixRelease, "0.5.8", "hotfix-forward"),
			[releases[0], releases[1], hotfixRelease],
			lifecycleFor(hotfixRelease),
		);
		expect(hotfixForward.get("latest")).toBe("0.5.9");
		const prereleaseEdgeForward = validateRoutingEvents(
			publicRoute(
				prereleaseEdgeRelease,
				"0.6.0-next.90",
				"prerelease-edge-forward",
			),
			[releases[0], releases[1], prereleaseEdgeRelease],
			lifecycleFor(prereleaseEdgeRelease),
		);
		expect(prereleaseEdgeForward.get("next")).toBe("0.6.0-next.90.1");
		const majorForward = validateRoutingEvents(
			publicRoute(majorRelease, "0.5.8", "major-forward"),
			[releases[0], releases[1], majorRelease],
			lifecycleFor(majorRelease),
		);
		expect(majorForward.get("latest")).toBe("1.0.0");

		expect(() =>
			validateRoutingEvents(
				publicRoute(
					stableDowngradeRelease,
					"0.5.8",
					"stable-downgrade",
				),
				[releases[0], releases[1], stableDowngradeRelease],
				lifecycleFor(stableDowngradeRelease),
			),
		).toThrow("routed public transition must strictly advance SemVer");
		expect(() =>
			validateRoutingEvents(
				publicRoute(
					nextDowngradeRelease,
					"0.6.0-next.90",
					"next-downgrade",
				),
				[releases[0], releases[1], nextDowngradeRelease],
				lifecycleFor(nextDowngradeRelease),
			),
		).toThrow("routed public transition must strictly advance SemVer");

		const correctedForward = validateRoutingEvents(
			[
				publicRoute(
					nextRelease,
					"0.6.0-next.90",
					"corrected-forward",
				)[0],
				route(
					"corrected-forward-fact",
					"routed",
					"next",
					"0.6.0-next.90",
					"0.5.0-next.1",
					"2026-08-01T02:01:00.000Z",
				),
				correction(
					"corrected-forward-replacement",
					"corrected-forward-fact",
					"routed",
					"2026-08-01T02:01:00.000Z",
					"next",
					"0.6.0-next.90",
					"0.6.0-next.91",
					"2026-08-01T03:00:00.000Z",
				),
			],
			nextOnlyReleases,
			nextOnlyLifecycles,
		);
		expect(correctedForward.get("next")).toBe("0.6.0-next.91");
		expect(() =>
			validateRoutingEvents(
				[
					publicRoute(
						nextDowngradeRelease,
						"0.6.0-next.90",
						"corrected-downgrade",
					)[0],
					route(
						"corrected-downgrade-fact",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					correction(
						"corrected-downgrade-replacement",
						"corrected-downgrade-fact",
						"routed",
						"2026-08-01T02:01:00.000Z",
						"next",
						"0.6.0-next.90",
						"0.5.0-next.1",
						"2026-08-01T03:00:00.000Z",
					),
				],
				[releases[0], releases[1], nextDowngradeRelease],
				lifecycleFor(nextDowngradeRelease),
			),
		).toThrow("routed public transition must strictly advance SemVer");

		const quarantineOnly = validateRoutingEvents(
			[
				route(
					"candidate-route-2",
					"routed",
					"candidate",
					"none",
					"0.6.0-next.91",
					"2026-08-01T02:00:00.000Z",
				),
			],
			nextOnlyReleases,
			nextOnlyLifecycles,
		);
		expect(quarantineOnly.get("next")).toBe("0.6.0-next.90");
		expect(quarantineOnly.get("candidate")).toBe("0.6.0-next.91");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"candidate-route-3",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"candidate-abort-3",
						"routing-aborted",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:01:00.000Z",
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).not.toThrow();

		expect(() =>
			validateRoutingEvents([], nextOnlyReleases, nextOnlyLifecycles),
		).toThrow("published identity has no candidate quarantine route");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"only-one-candidate-route",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"only-one-candidate-abort",
						"routing-aborted",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:01:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow(
			"published identity has no candidate quarantine route for svelte-adapter-uws-extensions@0.6.0",
		);
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"first-candidate-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"first-candidate-abort",
						"routing-aborted",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"second-candidate-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:02:00.000Z",
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).toThrow("cannot open candidate quarantine more than once");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"rollback-candidate-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"rollback-forward-route",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"rollback-reject-route",
						"rollback",
						"next",
						"0.6.0-next.91",
						"0.6.0-next.90",
						"2026-08-01T02:02:00.000Z",
					),
					route(
						"rollback-reroute-survivor",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:03:00.000Z",
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).toThrow("rejected identity cannot be routed forward again");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"reverse-before-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"reverse-before-forward",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"reverse-before-reject",
						"rollback",
						"next",
						"0.6.0-next.91",
						"0.6.0-next.90",
						"2026-08-01T02:02:00.000Z",
					),
					route(
						"reverse-before-survivor",
						"rollback",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:03:00.000Z",
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).toThrow("rejected identity cannot be routed forward again");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"reverse-after-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"reverse-after-forward",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"reverse-after-cleanup",
						"routed",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:02:00.000Z",
					),
					route(
						"reverse-after-reject",
						"rollback",
						"next",
						"0.6.0-next.91",
						"0.6.0-next.90",
						"2026-08-01T02:03:00.000Z",
					),
					route(
						"reverse-after-survivor",
						"rollback",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:04:00.000Z",
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).toThrow("rejected identity cannot be routed forward again");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"persistent-route-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"persistent-route-forward",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"persistent-route-reject",
						"rollback",
						"next",
						"0.6.0-next.91",
						"0.6.0-next.90",
						"2026-08-01T02:02:00.000Z",
					),
					route(
						"persistent-route-cleanup",
						"routed",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:03:00.000Z",
					),
					route(
						"persistent-routed-survivor",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:04:00.000Z",
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).toThrow("rejected identity cannot be routed forward again");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"cleanup-after-rollback-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"cleanup-after-rollback-forward",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"cleanup-after-rollback-reject",
						"rollback",
						"next",
						"0.6.0-next.91",
						"0.6.0-next.90",
						"2026-08-01T02:02:00.000Z",
					),
					route(
						"cleanup-after-rollback-clear",
						"routed",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:03:00.000Z",
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).not.toThrow();
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"corrected-route-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"corrected-route-fact",
						"rollback",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"corrected-route-cleanup",
						"routed",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:02:00.000Z",
					),
					correction(
						"corrected-route-replacement",
						"corrected-route-fact",
						"routed",
						"2026-08-01T02:01:00.000Z",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T03:00:00.000Z",
						{
							Notes: "corrects a mistaken rollback event kind",
						},
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).not.toThrow();
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"corrected-reverse-open",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"corrected-reverse-forward",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"corrected-reverse-reject",
						"rollback",
						"next",
						"0.6.0-next.91",
						"0.6.0-next.90",
						"2026-08-01T02:02:00.000Z",
					),
					route(
						"corrected-reverse-cleanup",
						"routed",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:03:00.000Z",
					),
					route(
						"corrected-reverse-fact",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:04:00.000Z",
					),
					correction(
						"corrected-reverse-replacement",
						"corrected-reverse-fact",
						"rollback",
						"2026-08-01T02:04:00.000Z",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T03:00:00.000Z",
						{
							Notes: "records the effective reverse rollback kind",
						},
					),
				],
				nextOnlyReleases,
				nextOnlyLifecycles,
			),
		).toThrow("rejected identity cannot be routed forward again");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"hotfix-reverse-open",
						"routed",
						"candidate",
						"none",
						"0.5.9",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"hotfix-reverse-forward",
						"routed",
						"latest",
						"0.5.8",
						"0.5.9",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"hotfix-reverse-cleanup",
						"routed",
						"candidate",
						"0.5.9",
						"none",
						"2026-08-01T02:02:00.000Z",
					),
					route(
						"hotfix-reverse-reject",
						"rollback",
						"latest",
						"0.5.9",
						"0.5.8",
						"2026-08-01T02:03:00.000Z",
					),
					route(
						"hotfix-reverse-survivor",
						"rollback",
						"latest",
						"0.5.8",
						"0.5.9",
						"2026-08-01T02:04:00.000Z",
					),
				],
				hotfixReleases,
				hotfixLifecycles,
			),
		).toThrow("rejected identity cannot be routed forward again");

		const fixedForward = validateRoutingEvents(
			[
				route(
					"fix-forward-rejected-open",
					"routed",
					"candidate",
					"none",
					"0.6.0-next.91",
					"2026-08-01T03:10:00.000Z",
				),
				route(
					"fix-forward-rejected-route",
					"routed",
					"next",
					"0.6.0-next.90",
					"0.6.0-next.91",
					"2026-08-01T03:11:00.000Z",
				),
				route(
					"fix-forward-rejected-rollback",
					"rollback",
					"next",
					"0.6.0-next.91",
					"0.6.0-next.90",
					"2026-08-01T03:12:00.000Z",
				),
				route(
					"fix-forward-rejected-cleanup",
					"routed",
					"candidate",
					"0.6.0-next.91",
					"none",
					"2026-08-01T03:13:00.000Z",
				),
				route(
					"fix-forward-fixed-open",
					"routed",
					"candidate",
					"none",
					"0.6.0-next.92",
					"2026-08-01T04:00:00.000Z",
				),
				route(
					"fix-forward-fixed-route",
					"routed",
					"next",
					"0.6.0-next.90",
					"0.6.0-next.92",
					"2026-08-01T04:01:00.000Z",
				),
				route(
					"fix-forward-fixed-cleanup",
					"routed",
					"candidate",
					"0.6.0-next.92",
					"none",
					"2026-08-01T04:02:00.000Z",
				),
			],
			fixForwardReleases,
			fixForwardLifecycles,
		);
		expect(fixedForward.get("next")).toBe("0.6.0-next.92");
		expect(fixedForward.get("candidate")).toBe("none");

		expect(() =>
			validateRoutingEvents(
				[
					route(
						"never-routed-rollback",
						"rollback",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("rollback target never previously occupied");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"candidate-route-4",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"next-route-4",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					route(
						"late-abort-4",
						"routing-aborted",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:02:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("routing-aborted is invalid after target routing");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"candidate-route-5",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"wrong-prior-abort-5",
						"routing-aborted",
						"candidate",
						"0.6.0-next.91",
						"0.6.0",
						"2026-08-01T02:01:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("restore the captured candidate pointer");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"candidate-route-6",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"early-cleanup-6",
						"routed",
						"candidate",
						"0.6.0-next.91",
						"none",
						"2026-08-01T02:01:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("cleanup requires target routing");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"prepublication-route-7",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T00:59:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("predates registry publication");
		const delayedLifecycle = validateCandidateLifecycles(
			[
				candidateEvent(
					nextRelease,
					"proposed",
					"2026-08-01T00:00:00.000Z",
				),
				candidateEvent(
					nextRelease,
					"tagged",
					"2026-08-01T00:30:00.000Z",
				),
				candidateEvent(
					nextRelease,
					"published",
					"2026-08-01T01:10:00.000Z",
				),
			],
			releases.slice(0, 3),
		);
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"preledger-route-7b",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T01:05:00.000Z",
					),
				],
				releases.slice(0, 3),
				delayedLifecycle,
			),
		).toThrow("predates the published candidate event");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"target-without-quarantine-8",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("requires the active quarantine identity");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"legacy-candidate-9",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.90",
						"2026-08-01T02:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("newly published identity");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"missing-target-10",
						"routed",
						"candidate",
						"none",
						"9.9.9",
						"2026-08-01T02:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("not an immutable release");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"wrong-channel-11",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0",
						"2026-08-01T02:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("wrong release channel");
		expect(() =>
			validateRoutingEvents(
				[
					correction(
						"orphan-correction-12",
						"missing-event",
						"routed",
						"2026-08-01T02:00:00.000Z",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T03:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("correction target must be an earlier event");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"candidate-route-13",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					correction(
						"empty-correction-13",
						"candidate-route-13",
						"routed",
						"2026-08-01T02:00:00.000Z",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T03:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("correction changes no recorded fact");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"candidate-route-14",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					route(
						"next-route-14",
						"routed",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T02:01:00.000Z",
					),
					correction(
						"event-correction-14",
						"next-route-14",
						"rollback",
						"2026-08-01T02:01:00.000Z",
						"next",
						"0.6.0-next.90",
						"0.6.0-next.91",
						"2026-08-01T03:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("rollback target never previously occupied");
		expect(() =>
			validateRoutingEvents(
				[
					route(
						"candidate-route-15",
						"routed",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T02:00:00.000Z",
					),
					correction(
						"time-correction-15",
						"candidate-route-15",
						"routed",
						"2026-08-01T00:59:00.000Z",
						"candidate",
						"none",
						"0.6.0-next.91",
						"2026-08-01T03:00:00.000Z",
					),
				],
				releases,
				lifecycles,
			),
		).toThrow("predates registry publication");
	});

	it("records complete, unique rollback identities", () => {
		const rows = publishedRows();
		expect(rows.length).toBeGreaterThanOrEqual(2);
		const identities = new Set();
		for (const row of rows) {
			const pkg = row.Package;
			const version = row.Version;
			const channel = row["Target channel"];
			const gitHead = row["Git head"];
			const gitTag = row["Git tag"];
			const integrity = row["npm integrity"];
			const shasum = row["npm shasum"];
			const published = row["Published UTC"];
			expect(pkg).toBe("svelte-adapter-uws-extensions");
			expect(semver.valid(version)).toBe(version);
			expect(["latest", "next"]).toContain(channel);
			expect(channel).toBe(channelForVersion(version));
			expect(gitHead).toMatch(/^[0-9a-f]{40}$/);
			expect(
				gitTag === "legacy-none" || gitTag === pkg + "@" + version,
			).toBe(true);
			expect(integrity).toMatch(/^sha512-[A-Za-z0-9+/]+={0,2}$/);
			expect(shasum).toMatch(/^[0-9a-f]{40}$/);
			expect(() =>
				eventTime(published, "published release"),
			).not.toThrow();
			const identity = pkg + "@" + version;
			expect(
				identities.has(identity),
				"duplicate manifest identity " + identity,
			).toBe(false);
			identities.add(identity);
		}
	});

	it("anchors the verified legacy rollback channels", () => {
		const releases = publishedRows().filter(row => row["Git tag"] === "legacy-none");
		const lifecycles = validateCandidateLifecycles(
			[],
			releases,
		);
		const current = validateRoutingEvents(
			[],
			releases,
			lifecycles,
		);
		expect(current.get("latest")).toBe("0.5.10");
		expect(current.get("next")).toBe("0.6.0-next.63");
	});
});
