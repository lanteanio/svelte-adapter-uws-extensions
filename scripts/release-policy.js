import semver from "semver";

export function releaseKey(row) {
	return row.Package + "@" + row.Version;
}

export function channelForVersion(version) {
	if (semver.valid(version) !== version || version.includes("+"))
		throw new Error("invalid release version " + version);
	return semver.prerelease(version) === null ? "latest" : "next";
}

export function eventTime(value, context) {
	if (
		typeof value !== "string" ||
		!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)
	) {
		throw new Error("invalid " + context + " UTC");
	}
	const parsed = Date.parse(value);
	if (Number.isNaN(parsed) || new Date(parsed).toISOString() !== value) {
		throw new Error("invalid " + context + " UTC");
	}
	return parsed;
}

export function validateCandidateLifecycles(events, releases) {
	const published = new Map();
	for (const row of releases) {
		const key = releaseKey(row);
		const expectedChannel = channelForVersion(row.Version);
		if (row["Target channel"] !== expectedChannel) {
			throw new Error(
				"release version has the wrong target channel for " + key,
			);
		}
		eventTime(row["Published UTC"], "published release " + key);
		if (row["Git tag"] !== "legacy-none") {
			if (published.has(key))
				throw new Error("duplicate immutable row for " + key);
			published.set(key, row);
		}
	}
	const state = new Map();
	for (const event of events) {
		const candidate = event.Candidate;
		const packagePrefix = "svelte-adapter-uws-extensions@";
		const version = candidate.startsWith(packagePrefix)
			? candidate.slice(packagePrefix.length)
			: "";
		try {
			channelForVersion(version);
		} catch {
			throw new Error("invalid candidate identity " + candidate);
		}
		if (
			!["proposed", "tagged", "aborted", "published"].includes(
				event.Event,
			)
		) {
			throw new Error("invalid candidate event for " + candidate);
		}
		const time = eventTime(
			event["Event UTC"],
			"candidate event for " + candidate,
		);
		if (!/^[0-9a-f]{40}$/.test(event["Git head"])) {
			throw new Error("invalid candidate Git head for " + candidate);
		}
		if (event["Planned tag"] !== candidate) {
			throw new Error("planned tag differs from candidate " + candidate);
		}
		if (!["latest", "next"].includes(event["Target channel"])) {
			throw new Error(
				"invalid candidate target channel for " + candidate,
			);
		}
		if (event["Target channel"] !== channelForVersion(version)) {
			throw new Error(
				"candidate version has the wrong target channel for " +
					candidate,
			);
		}
		if (event.Evidence.length === 0)
			throw new Error("candidate evidence is required for " + candidate);
		const previous = state.get(candidate);
		const allowed =
			previous === undefined
				? ["proposed"]
				: previous.event === "proposed"
					? ["tagged", "aborted"]
					: previous.event === "tagged"
						? ["published", "aborted"]
						: [];
		if (!allowed.includes(event.Event)) {
			throw new Error(
				"invalid candidate transition " +
					candidate +
					": " +
					(previous?.event ?? "none") +
					" -> " +
					event.Event,
			);
		}
		const identity = {
			gitHead: event["Git head"],
			plannedTag: event["Planned tag"],
			targetChannel: event["Target channel"],
		};
		if (previous !== undefined) {
			for (const field of Object.keys(identity)) {
				if (identity[field] !== previous.identity[field]) {
					throw new Error(
						"candidate identity changed for " +
							candidate +
							": " +
							field,
					);
				}
			}
			if (time < previous.eventTime) {
				throw new Error(
					"candidate events are not chronological for " + candidate,
				);
			}
		}
		state.set(candidate, {
			event: event.Event,
			eventTime: time,
			identity,
			eventTimes: {
				...(previous?.eventTimes ?? {}),
				[event.Event]: time,
			},
		});
	}
	for (const [candidate, finalState] of state) {
		const release = published.get(candidate);
		if (finalState.event === "published" && release === undefined) {
			throw new Error(
				"published event has no immutable row for " + candidate,
			);
		}
		if (finalState.event === "aborted" && release !== undefined) {
			throw new Error(
				"aborted candidate has an immutable row for " + candidate,
			);
		}
		if (release !== undefined) {
			const publishedTime = eventTime(
				release["Published UTC"],
				"published release " + candidate,
			);
			if (release["Git head"] !== finalState.identity.gitHead) {
				throw new Error(
					"published Git head differs from candidate " + candidate,
				);
			}
			if (release["Git tag"] !== finalState.identity.plannedTag) {
				throw new Error(
					"published Git tag differs from candidate " + candidate,
				);
			}
			if (
				release["Target channel"] !== finalState.identity.targetChannel
			) {
				throw new Error(
					"published target channel differs from candidate " +
						candidate,
				);
			}
			for (const name of ["proposed", "tagged"]) {
				if ((finalState.eventTimes[name] ?? 0) > publishedTime) {
					throw new Error(
						name +
							" candidate event follows registry publication for " +
							candidate,
					);
				}
			}
			if (finalState.eventTime < publishedTime) {
				throw new Error(
					"published candidate event predates registry publication for " +
						candidate,
				);
			}
			finalState.registryPublishedTime = publishedTime;
		}
	}
	for (const candidate of published.keys()) {
		if (state.get(candidate)?.event !== "published") {
			throw new Error(
				"immutable row has no published event for " + candidate,
			);
		}
	}
	return state;
}

export function validateRoutingEvents(
	events,
	releases,
	lifecycles = new Map(),
) {
	const releasesByKey = new Map();
	for (const row of releases) {
		const key = releaseKey(row);
		if (releasesByKey.has(key))
			throw new Error("duplicate immutable routing identity " + key);
		if (row["Target channel"] !== channelForVersion(row.Version)) {
			throw new Error(
				"release version has the wrong target channel for " + key,
			);
		}
		eventTime(row["Published UTC"], "published release " + key);
		releasesByKey.set(key, row);
	}
	const requiredCandidateRoutes = new Set();
	for (const row of releases) {
		if (row["Git tag"] === "legacy-none") continue;
		const key = releaseKey(row);
		if (lifecycles.get(key)?.event !== "published") {
			throw new Error(
				"published identity has no complete candidate lifecycle for " +
					key,
			);
		}
		requiredCandidateRoutes.add(key);
	}
	const rowsById = new Map();
	const rootById = new Map();
	const effectiveByRoot = new Map();
	const latestCorrectionByRoot = new Map();
	let lastRecordedTime = 0;

	for (const row of events) {
		const id = row["Event ID"];
		if (!/^[a-z0-9][a-z0-9._-]{2,79}$/.test(id))
			throw new Error("invalid routing event ID " + id);
		if (rowsById.has(id))
			throw new Error("duplicate routing event ID " + id);
		const recordedTime = eventTime(
			row["Event UTC"],
			"routing event for " + id,
		);
		if (recordedTime < lastRecordedTime)
			throw new Error(
				"routing ledger records are not chronological at " + id,
			);
		lastRecordedTime = recordedTime;
		if (
			!["routed", "routing-aborted", "rollback", "correction"].includes(
				row.Event,
			)
		) {
			throw new Error("invalid routing event type for " + id);
		}
		if (row.Package !== "svelte-adapter-uws-extensions")
			throw new Error("invalid routing package for " + id);
		if (!["latest", "next", "candidate"].includes(row.Channel)) {
			throw new Error("invalid routing channel for " + id);
		}
		if (row.Evidence.length === 0 || row.Notes.length === 0) {
			throw new Error(
				"routing evidence and notes are required for " + id,
			);
		}

		if (row.Event === "correction") {
			if (
				!["routed", "routing-aborted", "rollback"].includes(
					row["Corrected event"],
				)
			) {
				throw new Error(
					"invalid corrected routing event type for " + id,
				);
			}
			const correctedTime = eventTime(
				row["Corrected UTC"],
				"corrected routing event for " + id,
			);
			if (correctedTime > recordedTime)
				throw new Error(
					"correction predates its corrected fact for " + id,
				);
			const correctedId = row["Corrects event"];
			if (!rowsById.has(correctedId)) {
				throw new Error(
					"correction target must be an earlier event for " + id,
				);
			}
			if (
				recordedTime <
				eventTime(
					rowsById.get(correctedId)["Event UTC"],
					"corrected routing record " + correctedId,
				)
			) {
				throw new Error(
					"correction predates the event it corrects for " + id,
				);
			}
			const root = rootById.get(correctedId);
			if (latestCorrectionByRoot.get(root) !== correctedId) {
				throw new Error(
					"correction must extend the latest correction for " + root,
				);
			}
			const prior = effectiveByRoot.get(root);
			const replacement = {
				...prior,
				Event: row["Corrected event"],
				"Event UTC": row["Corrected UTC"],
				Package: row.Package,
				Channel: row.Channel,
				"From version": row["From version"],
				"To version": row["To version"],
				Evidence: row.Evidence,
				Notes: row.Notes,
			};
			const correctedFields = [
				"Event",
				"Event UTC",
				"Package",
				"Channel",
				"From version",
				"To version",
				"Evidence",
				"Notes",
			];
			if (
				correctedFields.every(
					(field) => replacement[field] === prior[field],
				)
			) {
				throw new Error(
					"correction changes no recorded fact for " + id,
				);
			}
			rootById.set(id, root);
			effectiveByRoot.set(root, replacement);
			latestCorrectionByRoot.set(root, id);
		} else {
			if (row["Corrects event"] !== "none") {
				throw new Error(
					"non-correction event cannot correct another event for " +
						id,
				);
			}
			if (
				row["Corrected event"] !== "none" ||
				row["Corrected UTC"] !== "none"
			) {
				throw new Error(
					"non-correction event cannot carry corrected fields for " +
						id,
				);
			}
			rootById.set(id, id);
			effectiveByRoot.set(id, row);
			latestCorrectionByRoot.set(id, id);
		}
		rowsById.set(id, row);
	}

	// The newest legacy row on each channel is that channel's routing
	// baseline; every earlier legacy row records an identity that previously
	// occupied the channel and is a valid rollback destination.
	const current = new Map([["candidate", "none"]]);
	const previouslyRouted = new Map([["candidate", new Set(["none"])]]);
	const baselineTime = new Map();
	for (const row of releases.filter(
		(release) => release["Git tag"] === "legacy-none",
	)) {
		const channel = row["Target channel"];
		const publishedTime = eventTime(
			row["Published UTC"],
			"published release " + releaseKey(row),
		);
		if (!previouslyRouted.has(channel)) {
			previouslyRouted.set(channel, new Set());
		}
		previouslyRouted.get(channel).add(row.Version);
		if ((baselineTime.get(channel) ?? -1) < publishedTime) {
			baselineTime.set(channel, publishedTime);
			current.set(channel, row.Version);
		}
	}
	let lastEffectiveTime = 0;
	let activeQuarantine = null;
	const quarantinedIdentities = new Set();
	const rejectedRoutingIdentities = new Set();
	for (const [root, row] of effectiveByRoot) {
		const fromVersion = row["From version"];
		const toVersion = row["To version"];
		const fromRelease =
			fromVersion === "none"
				? undefined
				: releasesByKey.get(row.Package + "@" + fromVersion);
		const toRelease =
			toVersion === "none"
				? undefined
				: releasesByKey.get(row.Package + "@" + toVersion);
		if (fromVersion !== "none" && fromRelease === undefined) {
			throw new Error(
				"routing source is not an immutable release for " + root,
			);
		}
		if (toVersion !== "none" && toRelease === undefined) {
			throw new Error(
				"routing target is not an immutable release for " + root,
			);
		}
		if (fromVersion === toVersion)
			throw new Error(
				"routing event does not move a pointer for " + root,
			);
		if (current.get(row.Channel) !== fromVersion) {
			throw new Error(
				"routing history is discontinuous for " +
					row.Channel +
					" at " +
					root,
			);
		}
		const time = eventTime(
			row["Event UTC"],
			"effective routing event for " + root,
		);
		if (time < lastEffectiveTime) {
			throw new Error(
				"effective routing events are not chronological at " + root,
			);
		}
		lastEffectiveTime = time;
		for (const release of [fromRelease, toRelease].filter(Boolean)) {
			const key = releaseKey(release);
			const publishedTime = eventTime(
				release["Published UTC"],
				"published release " + key,
			);
			if (time < publishedTime)
				throw new Error(
					"routing event predates registry publication for " + root,
				);
			if (release["Git tag"] !== "legacy-none") {
				const lifecycle = lifecycles.get(key);
				if (lifecycle?.event !== "published") {
					throw new Error(
						"routing identity has no published lifecycle for " +
							root,
					);
				}
				if (time < lifecycle.eventTime) {
					throw new Error(
						"routing event predates the published candidate event for " +
							root,
					);
				}
			}
		}
		if (
			row.Channel !== "candidate" &&
			toRelease !== undefined &&
			rejectedRoutingIdentities.has(releaseKey(toRelease))
		) {
			throw new Error(
				"rejected identity cannot be routed forward again for " + root,
			);
		}
		if (row.Event === "routed") {
			if (row.Channel === "candidate") {
				const closesQuarantine =
					activeQuarantine !== null &&
					fromVersion === activeQuarantine.version &&
					toVersion === activeQuarantine.prior;
				if (closesQuarantine) {
					if (!activeQuarantine.targetRouted) {
						throw new Error(
							"successful quarantine cleanup requires target routing for " +
								root,
						);
					}
					activeQuarantine = null;
				} else {
					if (activeQuarantine !== null)
						throw new Error(
							"active quarantine must be restored before " + root,
						);
					if (
						toRelease === undefined ||
						toRelease["Git tag"] === "legacy-none"
					) {
						throw new Error(
							"candidate routing requires a newly published identity for " +
								root,
						);
					}
					const quarantineIdentity = releaseKey(toRelease);
					if (quarantinedIdentities.has(quarantineIdentity)) {
						throw new Error(
							"immutable identity cannot open candidate quarantine more than once for " +
								root,
						);
					}
					quarantinedIdentities.add(quarantineIdentity);
					activeQuarantine = {
						identity: quarantineIdentity,
						version: toVersion,
						prior: fromVersion,
						targetChannel: toRelease["Target channel"],
						targetRouted: false,
						quarantineTime: time,
					};
				}
			} else {
				if (toRelease?.["Target channel"] !== row.Channel) {
					throw new Error(
						"routed target has the wrong release channel for " +
							root,
					);
				}
				if (!semver.gt(toVersion, fromVersion)) {
					throw new Error(
						"routed public transition must strictly advance SemVer for " +
							root,
					);
				}
				if (
					activeQuarantine === null ||
					activeQuarantine.version !== toVersion ||
					activeQuarantine.targetChannel !== row.Channel
				) {
					throw new Error(
						"target routing requires the active quarantine identity for " +
							root,
					);
				}
				if (time < activeQuarantine.quarantineTime) {
					throw new Error(
						"target routing predates quarantine publication for " +
							root,
					);
				}
				if (activeQuarantine.rolledBack) {
					throw new Error(
						"rolled-back identity cannot be routed forward again for " +
							root,
					);
				}
				activeQuarantine.targetRouted = true;
				activeQuarantine.targetRouteTime = time;
			}
		} else if (row.Event === "rollback") {
			if (
				row.Channel === "candidate" ||
				fromRelease === undefined ||
				toRelease === undefined
			) {
				throw new Error(
					"rollback requires two target-channel identities for " +
						root,
				);
			}
			if (
				fromRelease["Target channel"] !== row.Channel ||
				toRelease["Target channel"] !== row.Channel
			) {
				throw new Error(
					"rollback identity has the wrong release channel for " +
						root,
				);
			}
			if (!previouslyRouted.get(row.Channel)?.has(toVersion)) {
				throw new Error(
					"rollback target never previously occupied " +
						row.Channel +
						" for " +
						root,
				);
			}
			if (!semver.lt(toVersion, fromVersion)) {
				throw new Error(
					"rollback must move backward in SemVer for " + root,
				);
			}
			if (
				activeQuarantine !== null &&
				activeQuarantine.version === fromVersion &&
				activeQuarantine.targetChannel === row.Channel
			) {
				activeQuarantine.rolledBack = true;
			}
			rejectedRoutingIdentities.add(releaseKey(fromRelease));
		} else if (row.Event === "routing-aborted") {
			if (
				row.Channel !== "candidate" ||
				fromRelease === undefined ||
				fromRelease["Git tag"] === "legacy-none" ||
				activeQuarantine === null ||
				activeQuarantine.version !== fromVersion
			) {
				throw new Error(
					"routing-aborted must restore the candidate pointer for " +
						root,
				);
			}
			if (activeQuarantine.targetRouted) {
				throw new Error(
					"routing-aborted is invalid after target routing for " +
						root,
				);
			}
			if (toVersion !== activeQuarantine.prior) {
				throw new Error(
					"routing-aborted must restore the captured candidate pointer for " +
						root,
				);
			}
			activeQuarantine = null;
		}
		current.set(row.Channel, toVersion);
		if (!previouslyRouted.has(row.Channel))
			previouslyRouted.set(row.Channel, new Set());
		previouslyRouted.get(row.Channel).add(toVersion);
	}
	for (const identity of requiredCandidateRoutes) {
		if (!quarantinedIdentities.has(identity)) {
			throw new Error(
				"published identity has no candidate quarantine route for " +
					identity,
			);
		}
	}
	return current;
}
