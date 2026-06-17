/**
 * Mover count past which a flush builds the transient spatial index instead of
 * bounds-testing every entry per subscriber. Below it a flat scan is cheaper
 * than per-cell Map probes; above it the index earns a multiple-x CPU win at
 * the thousands-of-simultaneous-movers tail. Measured against the per-instance
 * combined mover count (this replica's local + inbound entries).
 */
export const INDEX_CROSSOVER = 512;

/**
 * Pack a grid cell coordinate pair into one numeric key. Covers +-32k cells per
 * axis (at the default 256-unit cell, +-8.3M board units). A key collision can
 * only over-deliver - every pulled entry is re-tested against the exact bounds -
 * never blank a region.
 * @param {number} cx
 * @param {number} cy
 */
export function packCell(cx, cy) {
	return ((cx & 0xffff) << 16) | (cy & 0xffff);
}

/**
 * Create a per-subscriber viewport culler. Owns the per-flush spatial-index
 * scratch (the packed-cell index, its bucket pool, the cull-output buffer,
 * and the reused padded-bounds object). Given a flush's combined frame
 * (flushEntries / flushPos / alwaysVisible) and a subscriber's reported rect,
 * it returns the slice of movers inside that rect plus the padding overscan.
 * Cluster-neutral: culling runs per replica against the combined local + peer
 * set, so a peer-origin cursor is culled identically to a local one and the
 * rect never crosses Redis.
 *
 * @param {{ viewportCell: number, viewportPadding: number }} opts - viewport
 *   options resolved by resolveCursorOptions (cell size + overscan padding).
 */
export function createViewportCuller({ viewportCell, viewportPadding }) {
	/** @type {Map<number, number[]>} - transient spatial index (packed cell key -> entry indices) */
	const flushCells = new Map();
	/** @type {number[][]} - recycled bucket arrays for flushCells */
	const cellPool = [];
	/** @type {Array<{ key: string, data: any }>} - per-subscriber cull result */
	const cullOut = [];
	/** Reused padded-bounds object so culling allocates nothing per call. */
	const bounds = { minX: 0, minY: 0, maxX: 0, maxY: 0 };

	/**
	 * Build the transient spatial index over this flush's positioned movers.
	 * Bucket arrays are drawn from `cellPool` and returned by `releaseFlushCells`
	 * after the walk, so a dense flush recycles them.
	 * @param {Array<{ x: number, y: number } | null>} flushPos - resolved position per entry index (null = always-visible)
	 * @param {number} n - flushEntries.length
	 */
	function buildFlushCells(flushPos, n) {
		releaseFlushCells();
		for (let i = 0; i < n; i++) {
			const pos = flushPos[i];
			if (pos === null) continue; // null-pos delivered via alwaysVisible
			const ck = packCell(Math.floor(pos.x / viewportCell), Math.floor(pos.y / viewportCell));
			let bucket = flushCells.get(ck);
			if (!bucket) {
				bucket = cellPool.pop() || [];
				bucket.length = 0;
				flushCells.set(ck, bucket);
			}
			bucket.push(i);
		}
	}

	/** Return this flush's bucket arrays to the pool and empty the index. */
	function releaseFlushCells() {
		for (const bucket of flushCells.values()) cellPool.push(bucket);
		flushCells.clear();
	}

	/**
	 * Resolve a reported rect's padded board bounds. Width/height are board units
	 * already (the client reports the visible board region), so only the overscan
	 * is widened by 1/zoom when zoomed out, keeping it roughly constant on screen.
	 * Writes into the shared `bounds` object to avoid per-call alloc.
	 * @param {{ x: number, y: number, w: number, h: number, zoom: number }} rect
	 */
	function rectBounds(rect) {
		const pad = rect.zoom < 1 ? viewportPadding / rect.zoom : viewportPadding;
		bounds.minX = rect.x - pad;
		bounds.minY = rect.y - pad;
		bounds.maxX = rect.x + rect.w + pad;
		bounds.maxY = rect.y + rect.h + pad;
		return bounds;
	}

	/**
	 * Flat bounds test over every mover this flush. Used below INDEX_CROSSOVER,
	 * where the combined set is small enough that building an index does not pay.
	 * @param {{ x: number, y: number, w: number, h: number, zoom: number }} rect
	 * @param {Array<{ key: string, data: any }>} flushEntries - this flush's combined movers
	 * @param {Array<{ x: number, y: number } | null>} flushPos - resolved position per entry index
	 * @returns {Array<{ key: string, data: any }>}
	 */
	function cullDirect(rect, flushEntries, flushPos) {
		const bb = rectBounds(rect);
		cullOut.length = 0;
		for (let i = 0; i < flushEntries.length; i++) {
			const pos = flushPos[i];
			if (pos === null) { cullOut.push(flushEntries[i]); continue; }
			if (pos.x >= bb.minX && pos.x <= bb.maxX && pos.y >= bb.minY && pos.y <= bb.maxY) {
				cullOut.push(flushEntries[i]);
			}
		}
		return cullOut;
	}

	/**
	 * Spatial-index cull: walk only the cells the viewport covers and bounds-test
	 * their movers. Per-subscriber cost is O(visible cells + movers in them), not
	 * O(all movers). A viewport spanning more cells than the flush has movers sees
	 * ~the whole board, so it delivers everything (the deliver-all clamp),
	 * bounding worst-case cost at O(movers).
	 * @param {{ x: number, y: number, w: number, h: number, zoom: number }} rect
	 * @param {Array<{ key: string, data: any }>} flushEntries - this flush's combined movers
	 * @param {Array<{ x: number, y: number } | null>} flushPos - resolved position per entry index
	 * @param {number[]} alwaysVisible - entry indices with no resolvable position (delivered to all)
	 * @returns {Array<{ key: string, data: any }>}
	 */
	function cullIndexed(rect, flushEntries, flushPos, alwaysVisible) {
		const bb = rectBounds(rect);
		const cx0 = Math.floor(bb.minX / viewportCell);
		const cy0 = Math.floor(bb.minY / viewportCell);
		const cx1 = Math.floor(bb.maxX / viewportCell);
		const cy1 = Math.floor(bb.maxY / viewportCell);
		if ((cx1 - cx0 + 1) * (cy1 - cy0 + 1) > flushEntries.length) return flushEntries;
		cullOut.length = 0;
		for (let a = 0; a < alwaysVisible.length; a++) cullOut.push(flushEntries[alwaysVisible[a]]);
		for (let cy = cy0; cy <= cy1; cy++) {
			for (let cx = cx0; cx <= cx1; cx++) {
				const bucket = flushCells.get(packCell(cx, cy));
				if (!bucket) continue;
				for (let bi = 0; bi < bucket.length; bi++) {
					const i = bucket[bi];
					const pos = flushPos[i];
					if (pos.x >= bb.minX && pos.x <= bb.maxX && pos.y >= bb.minY && pos.y <= bb.maxY) {
						cullOut.push(flushEntries[i]);
					}
				}
			}
		}
		return cullOut;
	}

	/** Reset all per-flush scratch (tracker clear/destroy). */
	function reset() {
		cullOut.length = 0;
		cellPool.length = 0;
		flushCells.clear();
	}

	return { buildFlushCells, releaseFlushCells, cullDirect, cullIndexed, reset };
}
