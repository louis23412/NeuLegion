import os from 'os';
import fs from 'fs';
import path from 'path';
import readline from 'readline';
import Database from 'better-sqlite3';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';
import { availableParallelism } from 'node:os';

let cache = [];
let structureMap;
let candleCounter = 0;
let httpWorker = null;

const CONFIG = {
    cutoff: null,
    baseProcessCount: 1,
    forceMin: true,
    httpPort: 3001,
    maxWorkers: Math.max(1, Math.floor(availableParallelism() * 0.25)),
    file: path.join(import.meta.dirname, 'candles.jsonl'),
    stateFolder : path.join(import.meta.dirname, '..', 'state'),

    dims: [2, 2, 2, 4],

    basePop: 64,
    groupPopBoost: 0.05,
    sectionPopBoost: 0.15,
    layerPopBoost: 0.25,

    baseCache: 500,
    groupCacheBoost: 0.15,
    sectionCacheBoost: 0.25,
    layerCacheBoost: 0.35,

    baseAtr: 2,
    baseStop: 1,
    minPriceMove: 0.0025,
    maxPriceMove: 0.05,
    groupPriceBoost: 0.05,
    sectionPriceBoost: 0.10,
    layerPriceBoost: 0.15,

    broadcastRatio : 0.025,
    injectionRatio : 0.025,

    volatileMemoryDecayFactor: 0.999,
    coreMemoryDecayFactor: 0.9995,
    memoryDecayFloor: 1,
    volatileConsolidationThreshold: 0.02,
    coreConsolidationThreshold: 0.01,
    consolidationPromoteCount: 25,
    coreCapacityRatio: 0.333,
    maxVaultCandidates: 5000,
    memoryVaultCapacity: 5000000,
    volatileConsolidationLimit: 1000,
    coreConsolidationLimit: 750,

    coreHierarchyThreshold: 0.03,
    volatileHierarchyThreshold: 0.05,
    hierarchyTraversalDepth: 5,
    maxHierarchyProtos: 250,

    coreMinNeighbors: 6,
    coreMaxNeighbors: 24,
    volatileMinNeighbors: 4,
    volatileMaxNeighbors: 16,

    newMemorySize: 1.0,
    newMemoryAccessCount: 1.0,
    newMemoryImportance: 10.0,
    baseAccessBoost: 1.001,
    performanceBoostFactor: 1.002
};

class PriorityQueue {
    constructor() {
        this.heap = [];
    }

    push(item) {
        this.heap.push(item);
        this._bubbleUp(this.heap.length - 1);
    }

    pop() {
        if (this.heap.length === 0) return null;
        if (this.heap.length === 1) return this.heap.pop();

        const min = this.heap[0];
        const end = this.heap.pop();
        this.heap[0] = end;
        this._sinkDown(0);
        return min;
    }

    get size() {
        return this.heap.length;
    }

    _bubbleUp(idx) {
        const element = this.heap[idx];
        while (idx > 0) {
            const parentIdx = Math.floor((idx - 1) / 2);
            const parent = this.heap[parentIdx];
            if (parent.accumDist <= element.accumDist) break;
            [this.heap[parentIdx], this.heap[idx]] = [this.heap[idx], this.heap[parentIdx]];
            idx = parentIdx;
        }
    }

    _sinkDown(idx) {
        const length = this.heap.length;
        const element = this.heap[idx];
        while (true) {
            let leftIdx = 2 * idx + 1;
            let rightIdx = 2 * idx + 2;
            let smallest = idx;

            if (leftIdx < length && this.heap[leftIdx].accumDist < element.accumDist) {
                smallest = leftIdx;
            }
            if (rightIdx < length && this.heap[rightIdx].accumDist < this.heap[smallest].accumDist) {
                smallest = rightIdx;
            }
            if (smallest === idx) break;

            [this.heap[idx], this.heap[smallest]] = [this.heap[smallest], this.heap[idx]];
            idx = smallest;
        }
    }
}

fs.existsSync(path.join(CONFIG.stateFolder, 'main')) ? null : fs.mkdirSync(path.join(CONFIG.stateFolder, 'main'), { recursive: true });

const db = new Database(path.join(CONFIG.stateFolder, 'main', 'legion_state.db'), { fileMustExist: false });
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('temp_store = MEMORY');
db.pragma('cache_size = -32000');

const memoryDb = new Database(path.join(CONFIG.stateFolder, 'main', 'memory_vault.db'), { fileMustExist: false });
memoryDb.pragma('journal_mode = WAL');
memoryDb.pragma('synchronous = NORMAL');
memoryDb.pragma('temp_store = MEMORY');
memoryDb.pragma('cache_size = -128000');

db.exec(`
    CREATE TABLE IF NOT EXISTS legion_state (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        candle_counter INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS legion_controllers (
        group_id INTEGER NOT NULL,
        section_id INTEGER NOT NULL,
        layer_id INTEGER NOT NULL,
        cluster_id INTEGER NOT NULL,
        controller_type TEXT NOT NULL,
        directory_path TEXT NOT NULL,
        signal_speed REAL NOT NULL DEFAULT 0,
        mem_connections INTEGER NOT NULL DEFAULT 0,
        last_signal TEXT NOT NULL DEFAULT '{}',
        signal_history TEXT NOT NULL DEFAULT '[]',
        prob_history TEXT NOT NULL DEFAULT '[]',
        score_history TEXT NOT NULL DEFAULT '[]',
        PRIMARY KEY (group_id, section_id, layer_id, cluster_id)
    );
`);

memoryDb.exec(`
    CREATE TABLE IF NOT EXISTS compat_positive (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        compatibility TEXT UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS compat_negative (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        compatibility TEXT UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS core_positive (
        protoId TEXT PRIMARY KEY,
        compat_id INTEGER NOT NULL REFERENCES compat_positive(id),
        mean BLOB NOT NULL,
        variance BLOB NOT NULL,
        size REAL NOT NULL,
        accessCount REAL NOT NULL,
        importance REAL NOT NULL,
        hash TEXT NOT NULL,
        lastAccessed INTEGER DEFAULT 0,
        usageCount INTEGER NOT NULL DEFAULT 0,
        merged_count INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS core_negative (
        protoId TEXT PRIMARY KEY,
        compat_id INTEGER NOT NULL REFERENCES compat_negative(id),
        mean BLOB NOT NULL,
        variance BLOB NOT NULL,
        size REAL NOT NULL,
        accessCount REAL NOT NULL,
        importance REAL NOT NULL,
        hash TEXT NOT NULL,
        lastAccessed INTEGER DEFAULT 0,
        usageCount INTEGER NOT NULL DEFAULT 0,
        merged_count INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS volatile_positive (
        protoId TEXT PRIMARY KEY,
        compat_id INTEGER NOT NULL REFERENCES compat_positive(id),
        mean BLOB NOT NULL,
        variance BLOB NOT NULL,
        size REAL NOT NULL,
        accessCount REAL NOT NULL,
        importance REAL NOT NULL,
        hash TEXT NOT NULL,
        lastAccessed INTEGER DEFAULT 0,
        usageCount INTEGER NOT NULL DEFAULT 0,
        merged_count INTEGER NOT NULL DEFAULT 1
    );
    CREATE TABLE IF NOT EXISTS volatile_negative (
        protoId TEXT PRIMARY KEY,
        compat_id INTEGER NOT NULL REFERENCES compat_negative(id),
        mean BLOB NOT NULL,
        variance BLOB NOT NULL,
        size REAL NOT NULL,
        accessCount REAL NOT NULL,
        importance REAL NOT NULL,
        hash TEXT NOT NULL,
        lastAccessed INTEGER DEFAULT 0,
        usageCount INTEGER NOT NULL DEFAULT 0,
        merged_count INTEGER NOT NULL DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS proto_edges (
        source TEXT NOT NULL CHECK(source IN ('core', 'volatile')),
        polarity TEXT NOT NULL CHECK(polarity IN ('positive', 'negative')),
        parent_proto TEXT NOT NULL,
        child_proto TEXT NOT NULL,
        accum_distance REAL NOT NULL,
        PRIMARY KEY (source, polarity, parent_proto, child_proto)
    );

    CREATE INDEX IF NOT EXISTS idx_compat_core_pos ON core_positive (compat_id);
    CREATE INDEX IF NOT EXISTS idx_compat_core_neg ON core_negative (compat_id);
    CREATE INDEX IF NOT EXISTS idx_compat_vol_pos ON volatile_positive (compat_id);
    CREATE INDEX IF NOT EXISTS idx_compat_vol_neg ON volatile_negative (compat_id);

    CREATE INDEX IF NOT EXISTS idx_purge_vol_pos ON volatile_positive (lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC);
    CREATE INDEX IF NOT EXISTS idx_purge_vol_neg ON volatile_negative (lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC);
    CREATE INDEX IF NOT EXISTS idx_purge_core_pos ON core_positive (lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC);
    CREATE INDEX IF NOT EXISTS idx_purge_core_neg ON core_negative (lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC);

    CREATE INDEX IF NOT EXISTS idx_top_core_pos ON core_positive (compat_id, importance DESC, accessCount DESC, lastAccessed DESC);
    CREATE INDEX IF NOT EXISTS idx_top_core_neg ON core_negative (compat_id, importance DESC, accessCount DESC, lastAccessed DESC);
    CREATE INDEX IF NOT EXISTS idx_top_vol_pos ON volatile_positive (compat_id, importance DESC, accessCount DESC, lastAccessed DESC);
    CREATE INDEX IF NOT EXISTS idx_top_vol_neg ON volatile_negative (compat_id, importance DESC, accessCount DESC, lastAccessed DESC);

    CREATE INDEX IF NOT EXISTS idx_proto_edges_child ON proto_edges (source, polarity, child_proto);
    CREATE INDEX IF NOT EXISTS idx_proto_edges_parent ON proto_edges (source, polarity, parent_proto);
`);

const insertCompatPos = memoryDb.prepare('INSERT INTO compat_positive (compatibility) VALUES (?) ON CONFLICT(compatibility) DO NOTHING');
const insertCompatNeg = memoryDb.prepare('INSERT INTO compat_negative (compatibility) VALUES (?) ON CONFLICT(compatibility) DO NOTHING');

const getCompatIdPos = memoryDb.prepare('SELECT id FROM compat_positive WHERE compatibility = ?');
const getCompatIdNeg = memoryDb.prepare('SELECT id FROM compat_negative WHERE compatibility = ?');

const upsertVolatilePos = memoryDb.prepare(`
    INSERT INTO volatile_positive (protoId, compat_id, mean, variance, size, accessCount, importance, hash, usageCount)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
    ON CONFLICT(protoId) DO UPDATE SET
        mean = excluded.mean,
        variance = excluded.variance,
        size = excluded.size,
        accessCount = excluded.accessCount,
        importance = excluded.importance,
        hash = excluded.hash,
        usageCount = usageCount + 1
`);

const upsertVolatileNeg = memoryDb.prepare(`
    INSERT INTO volatile_negative (protoId, compat_id, mean, variance, size, accessCount, importance, hash, usageCount)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
    ON CONFLICT(protoId) DO UPDATE SET
        mean = excluded.mean,
        variance = excluded.variance,
        size = excluded.size,
        accessCount = excluded.accessCount,
        importance = excluded.importance,
        hash = excluded.hash,
        usageCount = usageCount + 1
`);

const purgeVolatilePos = memoryDb.prepare(`
    DELETE FROM volatile_positive
    WHERE protoId IN (
        SELECT protoId FROM volatile_positive
        ORDER BY lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC
        LIMIT ?
    )
`);

const purgeVolatileNeg = memoryDb.prepare(`
    DELETE FROM volatile_negative
    WHERE protoId IN (
        SELECT protoId FROM volatile_negative
        ORDER BY lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC
        LIMIT ?
    )
`);

const purgeCorePos = memoryDb.prepare(`
    DELETE FROM core_positive
    WHERE protoId IN (
        SELECT protoId FROM core_positive
        ORDER BY lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC
        LIMIT ?
    )
`);

const purgeCoreNeg = memoryDb.prepare(`
    DELETE FROM core_negative
    WHERE protoId IN (
        SELECT protoId FROM core_negative
        ORDER BY lastAccessed ASC, importance ASC, accessCount ASC, merged_count ASC, protoId ASC
        LIMIT ?
    )
`);

const countCorePos = memoryDb.prepare('SELECT COUNT(*) FROM core_positive').pluck();
const countCoreNeg = memoryDb.prepare('SELECT COUNT(*) FROM core_negative').pluck();
const countVolPos = memoryDb.prepare('SELECT COUNT(*) FROM volatile_positive').pluck();
const countVolNeg = memoryDb.prepare('SELECT COUNT(*) FROM volatile_negative').pluck();

const getCountCorePos = memoryDb.prepare('SELECT COUNT(*) FROM core_positive WHERE compat_id = ?').pluck();
const getCountCoreNeg = memoryDb.prepare('SELECT COUNT(*) FROM core_negative WHERE compat_id = ?').pluck();
const getCountVolPos = memoryDb.prepare('SELECT COUNT(*) FROM volatile_positive WHERE compat_id = ?').pluck();
const getCountVolNeg = memoryDb.prepare('SELECT COUNT(*) FROM volatile_negative WHERE compat_id = ?').pluck();

const selectTopCorePos = memoryDb.prepare(`
    SELECT 'core' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM core_positive
    WHERE compat_id = ?
    ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
    LIMIT ?
`);

const selectTopCoreNeg = memoryDb.prepare(`
    SELECT 'core' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM core_negative
    WHERE compat_id = ?
    ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
    LIMIT ?
`);

const selectTopVolPos = memoryDb.prepare(`
    SELECT 'volatile' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM volatile_positive
    WHERE compat_id = ?
    ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
    LIMIT ?
`);

const selectTopVolNeg = memoryDb.prepare(`
    SELECT 'volatile' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM volatile_negative
    WHERE compat_id = ?
    ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
    LIMIT ?
`);

const updateLastVolPos = memoryDb.prepare(
    'UPDATE volatile_positive SET lastAccessed = ? WHERE protoId = ?'
);
const updateLastVolNeg = memoryDb.prepare(
    'UPDATE volatile_negative SET lastAccessed = ? WHERE protoId = ?'
);

const existsInCorePos = memoryDb.prepare('SELECT 1 FROM core_positive WHERE protoId = ?').pluck();
const existsInCoreNeg = memoryDb.prepare('SELECT 1 FROM core_negative WHERE protoId = ?').pluck();

const existsInVolatilePos = memoryDb.prepare('SELECT 1 FROM volatile_positive WHERE protoId = ?').pluck();
const existsInVolatileNeg = memoryDb.prepare('SELECT 1 FROM volatile_negative WHERE protoId = ?').pluck();

const updateControllerStmt = db.prepare(`
    UPDATE legion_controllers
    SET signal_speed = ?,
        mem_connections = ?,
        last_signal = ?,
        signal_history = ?,
        prob_history = ?,
        score_history = ?
    WHERE group_id = ? AND section_id = ? AND layer_id = ? AND cluster_id = ?
`);

const upsertCounterStmt = db.prepare(`
    INSERT INTO legion_state (singleton, candle_counter) VALUES (1, ?)
    ON CONFLICT(singleton) DO UPDATE SET candle_counter = excluded.candle_counter
`);

const selectAllMemoriesPos = memoryDb.prepare(`
    SELECT 'core' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM core_positive
    WHERE compat_id = ?
    UNION ALL
    SELECT 'volatile' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM volatile_positive
    WHERE compat_id = ?
`);

const selectAllMemoriesNeg = memoryDb.prepare(`
    SELECT 'core' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM core_negative
    WHERE compat_id = ?
    UNION ALL
    SELECT 'volatile' AS source, protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
    FROM volatile_negative
    WHERE compat_id = ?
`);

const accessMemoryCorePos = memoryDb.prepare(
    'UPDATE core_positive SET lastAccessed = ?, usageCount = usageCount + 1 WHERE protoId = ?'
);

const accessMemoryCoreNeg = memoryDb.prepare(
    'UPDATE core_negative SET lastAccessed = ?, usageCount = usageCount + 1 WHERE protoId = ?'
);

const accessMemoryVolPos = memoryDb.prepare(
    'UPDATE volatile_positive SET lastAccessed = ?, usageCount = usageCount + 1 WHERE protoId = ?'
);

const accessMemoryVolNeg = memoryDb.prepare(
    'UPDATE volatile_negative SET lastAccessed = ?, usageCount = usageCount + 1 WHERE protoId = ?'
);

const boostCorePos = memoryDb.prepare(`
    UPDATE core_positive
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?,
        usageCount = usageCount + 1
    WHERE protoId = ?
`);

const boostCoreNeg = memoryDb.prepare(`
    UPDATE core_negative
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?,
        usageCount = usageCount + 1
    WHERE protoId = ?
`);

const boostVolatilePos = memoryDb.prepare(`
    UPDATE volatile_positive
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?,
        usageCount = usageCount + 1
    WHERE protoId = ?
`);

const boostVolatileNeg = memoryDb.prepare(`
    UPDATE volatile_negative
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?,
        usageCount = usageCount + 1
    WHERE protoId = ?
`);

const boostExistingVolatilePos = memoryDb.prepare(`
    UPDATE volatile_positive
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?,
        usageCount = usageCount + 1
    WHERE protoId = ?
`);

const boostExistingVolatileNeg = memoryDb.prepare(`
    UPDATE volatile_negative
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?,
        usageCount = usageCount + 1
    WHERE protoId = ?
`);

const updateVolatilePosMain = memoryDb.prepare(`
    UPDATE volatile_positive
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const updateVolatileNegMain = memoryDb.prepare(`
    UPDATE volatile_negative
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const insertCorePosMain = memoryDb.prepare(`
    INSERT INTO core_positive (protoId, compat_id, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertCoreNegMain = memoryDb.prepare(`
    INSERT INTO core_negative (protoId, compat_id, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateCorePosMain = memoryDb.prepare(`
    UPDATE core_positive
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const updateCoreNegMain = memoryDb.prepare(`
    UPDATE core_negative
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const deleteProtoEdgesForProtoMain = memoryDb.prepare(`
    DELETE FROM proto_edges 
    WHERE source = ? AND polarity = ? AND (parent_proto = ? OR child_proto = ?)
`);

const insertProtoEdgeMain = memoryDb.prepare(`
    INSERT INTO proto_edges (source, polarity, parent_proto, child_proto, accum_distance)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(source, polarity, parent_proto, child_proto) DO NOTHING
`);

const selectMemCorePos = memoryDb.prepare('SELECT mean, variance, size, accessCount, importance, hash, lastAccessed FROM core_positive WHERE protoId = ?');
const selectMemCoreNeg = memoryDb.prepare('SELECT mean, variance, size, accessCount, importance, hash, lastAccessed FROM core_negative WHERE protoId = ?');
const selectMemVolPos = memoryDb.prepare('SELECT mean, variance, size, accessCount, importance, hash, lastAccessed FROM volatile_positive WHERE protoId = ?');
const selectMemVolNeg = memoryDb.prepare('SELECT mean, variance, size, accessCount, importance, hash, lastAccessed FROM volatile_negative WHERE protoId = ?');

const getNeighbors = memoryDb.prepare(`
    SELECT parent_proto AS neighbor_proto, accum_distance FROM proto_edges WHERE source = ? AND polarity = ? AND child_proto = ?
    UNION ALL
    SELECT child_proto AS neighbor_proto, accum_distance FROM proto_edges WHERE source = ? AND polarity = ? AND parent_proto = ?
`);

const storeNewMemories = memoryDb.transaction((results, currentBatch) => {
    const posCompatSet = new Set();
    const negCompatSet = new Set();
    const posMems = [];
    const negMems = [];

    for (const { signal, controller } of results) {
        if (!signal?.memoryBroadcast?.compatibility || !signal?.memoryBroadcast?.memories?.length) continue;

        const mb = signal.memoryBroadcast;
        const compatStr = canonicalJSON(mb.compatibility);
        const isPositive = controller.type === 'positive';

        (isPositive ? posCompatSet : negCompatSet).add(compatStr);

        const targetArray = isPositive ? posMems : negMems;
        for (const indivMem of mb.memories) {
            if (!indivMem?.mean || !indivMem?.variance || !indivMem?.protoId) continue;
            targetArray.push({ mem: indivMem, compatStr });
        }
    }

    for (const str of posCompatSet) insertCompatPos.run(str);
    for (const str of negCompatSet) insertCompatNeg.run(str);

    const posIdMap = {};
    for (const str of posCompatSet) {
        const row = getCompatIdPos.get(str);
        if (row) posIdMap[str] = row.id;
    }
    const negIdMap = {};
    for (const str of negCompatSet) {
        const row = getCompatIdNeg.get(str);
        if (row) negIdMap[str] = row.id;
    }

    for (const { mem, compatStr } of posMems) {
        const compat_id = posIdMap[compatStr];
        if (!compat_id) continue;

        const contentHash = computeContentHash(mem.mean);
        const protoId = mem.protoId;

        const inCore = existsInCorePos.get(protoId);
        const inVol  = existsInVolatilePos.get(protoId);

        if (inCore) {
            boostCorePos.run(
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                currentBatch, 
                protoId
            );
        } else if (inVol) {
            boostExistingVolatilePos.run(
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                currentBatch, 
                protoId
            );
        } else {
            upsertVolatilePos.run(
                protoId,
                compat_id,
                vectorToBlob(mem.mean),
                vectorToBlob(mem.variance),
                CONFIG.newMemorySize,
                CONFIG.newMemoryAccessCount,
                CONFIG.newMemoryImportance,
                contentHash
            );
            updateLastVolPos.run(currentBatch, protoId);
        }
    }

    for (const { mem, compatStr } of negMems) {
        const compat_id = negIdMap[compatStr];
        if (!compat_id) continue;

        const contentHash = computeContentHash(mem.mean);
        const protoId = mem.protoId;

        const inCore = existsInCoreNeg.get(protoId);
        const inVol  = existsInVolatileNeg.get(protoId);

        if (inCore) {
            boostCoreNeg.run(
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                currentBatch, 
                protoId
            );
        } else if (inVol) {
            boostExistingVolatileNeg.run(
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                CONFIG.baseAccessBoost, 
                currentBatch, 
                protoId
            );
        } else {
            upsertVolatileNeg.run(
                protoId,
                compat_id,
                vectorToBlob(mem.mean),
                vectorToBlob(mem.variance),
                CONFIG.newMemorySize,
                CONFIG.newMemoryAccessCount,
                CONFIG.newMemoryImportance,
                contentHash
            );
            updateLastVolNeg.run(currentBatch, protoId);
        }
    }

    return { posIdMap, negIdMap };
});

const applyBulkDeltas = memoryDb.transaction((deltas) => {
    const deletes = { volPos: [], volNeg: [], corePos: [], coreNeg: [] };
    const updates = { volPos: [], volNeg: [], corePos: [], coreNeg: [] };
    const promotes = { corePos: [], coreNeg: [] };

    for (const delta of deltas) {
        const isPos = delta.isPositive;
        const v = delta.volatile;
        const c = delta.core;

        deletes[isPos ? 'volPos' : 'volNeg'].push(...v.deletes);
        deletes[isPos ? 'corePos' : 'coreNeg'].push(...c.deletes);

        v.updates.forEach(u => {
            updates[isPos ? 'volPos' : 'volNeg'].push([
                vectorToBlob(u.mean), vectorToBlob(u.variance),
                u.size, u.accessCount, u.importance, u.hash,
                u.lastAccessed, u.usageCount, u.merged_count, u.protoId
            ]);
        });
        c.updates.forEach(u => {
            updates[isPos ? 'corePos' : 'coreNeg'].push([
                vectorToBlob(u.mean), vectorToBlob(u.variance),
                u.size, u.accessCount, u.importance, u.hash,
                u.lastAccessed, u.usageCount, u.merged_count, u.protoId
            ]);
        });

        v.promotes.forEach(p => {
            promotes[isPos ? 'corePos' : 'coreNeg'].push([
                p.protoId, delta.compat_id,
                vectorToBlob(p.mean), vectorToBlob(p.variance),
                p.size, p.accessCount, p.importance, p.hash,
                p.lastAccessed, p.usageCount, p.merged_count
            ]);
        });

        delta.edges.deletes.forEach(d => {
            deleteProtoEdgesForProtoMain.run(d.source, d.polarity, d.protoId, d.protoId);
        });
        delta.edges.inserts.forEach(e => {
            insertProtoEdgeMain.run(e.source, e.polarity, e.parent_proto, e.child_proto, e.accum_distance);
        });
    }

    const applyChunkedDelete = (table, ids) => {
        if (!ids.length) return;
        const chunkSize = 400;
        for (let i = 0; i < ids.length; i += chunkSize) {
            const chunk = ids.slice(i, i + chunkSize);
            const ph = chunk.map(() => '?').join(',');
            memoryDb.prepare(`DELETE FROM ${table} WHERE protoId IN (${ph})`).run(...chunk);
        }
    };

    applyChunkedDelete('volatile_positive', deletes.volPos);
    applyChunkedDelete('volatile_negative', deletes.volNeg);
    applyChunkedDelete('core_positive', deletes.corePos);
    applyChunkedDelete('core_negative', deletes.coreNeg);

    updates.volPos.forEach(p => updateVolatilePosMain.run(...p));
    updates.volNeg.forEach(p => updateVolatileNegMain.run(...p));
    updates.corePos.forEach(p => updateCorePosMain.run(...p));
    updates.coreNeg.forEach(p => updateCoreNegMain.run(...p));

    promotes.corePos.forEach(p => insertCorePosMain.run(...p));
    promotes.coreNeg.forEach(p => insertCoreNegMain.run(...p));
});

const spawnDedicatedHttpServer = () => {
    httpWorker = new Worker(new URL('./http_server_worker.js', import.meta.url), {
        workerData : {
            ip : getLocalIP(),
            port : CONFIG.httpPort
        }
    });

    httpWorker.on('error', (err) => {
        console.error('[HTTP Worker] Error:', err);
    });

    httpWorker.on('exit', (code) => {
        if (code !== 0) console.error(`[HTTP Worker] Exited with code ${code}`);
    });
};

const getCleanLegionState = () => {
    return structureMap.flat(3).map((c) => {
        return {
            id : c.lastSignal?.controllerId,
            polarity : c.type,
            signalSpeed : c.signalSpeed,

            hiveConnection : c.lastSignal?.hiveConnection,
            lastSaveStatus : c.lastSignal?.lastSaveStatus,

            legionPosition : [c.group, c.section, c.layer, c.id],

            params : {
                population : c.lastSignal?.pop,
                cacheSize : c.lastSignal?.cache,
                inputSize : c.lastSignal?.inputSize,
                candlesUesd : c.lastSignal?.candlesUsed,
                indicatorsUsed : c.lastSignal?.indicatorsUsed,
                atrFactor : c.lastSignal?.atrFactor,
                stopFactor : c.lastSignal?.stopFactor,
                minMove : c.lastSignal?.minPriceMovement,
                maxMove : c.lastSignal?.maxPriceMovement
            },

            price : {
                entryPrice : c.lastSignal?.entryPrice,
                exitPrice : c.lastSignal?.sellPrice,
                stopLoss : c.lastSignal?.stopLoss
            },

            stats : {
                probability : c.lastSignal?.prob,
                accuracyScore : c.lastSignal?.score,
                tradeAccuracy : c.lastSignal?.tradeAcc,
                probabilityAccuracy : c.lastSignal?.trueAcc,
                trainingSteps : c.lastSignal?.lastTrainingStep,
                skippedTraining : c.lastSignal?.skippedTraining,
                openSimulations : c.lastSignal?.openSimulations,
                pendingClosedTrades : c.lastSignal?.pendingClosedTrades
            },

            memory : {
                memoryConnections : c.memConnections,
                totalSent : c.lastSignal?.totalMemoriesSent,
                totalReceived : c.lastSignal?.totalMemoriesReceived,
                lastInjectedTotal : c.lastSignal?.lastMemoriesInjected,
                controllerMemories : c.lastSignal?.currentMemories,
                lastInjectionRatio : c.lastSignal?.lastMemoryChange,
                lastMemoriesPerMember : c.lastSignal?.lastMemoriesPerMember,
                lastBroadcastPeerMemories : c.lastSignal?.memoryBroadcast?.totalBroadcast,
                lastBroadcastVaultMemories : c.lastSignal?.memoryBroadcast?.vaultMemories
            }
        }
    });
}

const broadcastLegionState = (status) => {
    if (!httpWorker) return;

    const allControllers = getCleanLegionState();

    httpWorker.postMessage({
        type: 'UPDATE_STATE',
        state: {
            candleCounter,
            controllers: allControllers
        },
        status
    });
};

const getLocalIP = () => {
  const interfaces = os.networkInterfaces();
  let ipAddress;

  Object.keys(interfaces).forEach((ifaceName) => {
    interfaces[ifaceName].forEach((iface) => {
      if (iface.family === 'IPv4' && !iface.internal) {
        ipAddress = iface.address;
      }
    });
  });

  return ipAddress;
}

const vectorToBlob = (vec) => {
    if (!Array.isArray(vec) || vec.length === 0) return Buffer.allocUnsafe(0);
    const f64 = new Float64Array(vec);
    return Buffer.from(f64.buffer, f64.byteOffset, f64.byteLength);
};

const blobToVector = (blob) => {
    if (!Buffer.isBuffer(blob) || blob.length === 0 || blob.length % 8 !== 0) return [];
    const f64 = new Float64Array(blob.buffer, blob.byteOffset, blob.length / 8);
    return Array.from(f64);
};

const canonicalJSON = (obj) => {
    if (obj === null || typeof obj !== 'object') {
        return JSON.stringify(obj);
    }
    if (Array.isArray(obj)) {
        return '[' + obj.map(canonicalJSON).join(',') + ']';
    }
    const keys = Object.keys(obj).sort();
    const parts = keys.map(k => `${JSON.stringify(k)}:${canonicalJSON(obj[k])}`);
    return '{' + parts.join(',') + '}';
};

const computeContentHash = (mean) => {
    let hash = 2166136261;
    for (let i = 0; i < mean.length; i++) {
        let iv = Math.floor(mean[i] * 10000 + 0.5);
        hash ^= iv;
        hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
    }
    return ((hash >>> 0) % 0xFFFFFFFF).toString(16).padStart(8, '0');
};

const computeGaussianDistance = (mu1, var1, mu2, var2) => {
    const d = mu1.length;
    if (d !== mu2.length || d === 0) return Infinity;

    const eps = 1e-8;
    let mahalTerm = 0;
    let logDetTerm = 0;

    for (let j = 0; j < d; j++) {
        const v1 = Math.max(var1[j], eps);
        const v2 = Math.max(var2[j], eps);
        const avgV = (v1 + v2) / 2;
        const dm = mu1[j] - mu2[j];

        mahalTerm += (dm * dm) / avgV;
        logDetTerm += Math.log(avgV / Math.sqrt(v1 * v2));
    }

    return (1/8) * mahalTerm + (1/2) * logDetTerm;
};

const buildFreshStructure = () => {
    const legionStructure = Array.from({ length: CONFIG.dims[0] }, (_, group) =>
        Array.from({ length: CONFIG.dims[1] }, (_, section) =>
            Array.from({ length: CONFIG.dims[2] }, (_, layer) =>
                Array.from({ length: CONFIG.dims[3] }, (_, cluster) => {
                    const half = Math.floor(CONFIG.dims[3] / 2);
                    const directoryPath = path.join(CONFIG.stateFolder, 'clusters', `Group${group}`, `Section${section}`, `Layer${layer}`, `G${group}S${section}L${layer}C${cluster}`);

                    return {
                        type: cluster < half ? 'positive' : 'negative',
                        directoryPath,
                        group,
                        section,
                        layer,
                        id: cluster,
                        signalSpeed: 0,
                        memConnections : 0,
                        lastSignal: {},
                        signalHistory : [],
                        probHistory : [],
                        scoreHistory : []
                    };
                })
            )
        )
    );

    fs.mkdirSync(CONFIG.stateFolder, { recursive: true });
    legionStructure.flat(3).forEach(controller => {
        fs.mkdirSync(controller.directoryPath, { recursive: true });
    });

    return legionStructure;
};

const getControllerParams = (group, section, layer) => {
    const reversedGroup = CONFIG.dims[0] - 1 - group;
    const reversedSection = CONFIG.dims[1] - 1 - section;
    const reversedLayer = CONFIG.dims[2] - 1 - layer;

    const groupCacheFactor = 1 + CONFIG.groupCacheBoost * group;
    const sectionCacheFactor = 1 + CONFIG.sectionCacheBoost * section;
    const layerCacheFactor = 1 + CONFIG.layerCacheBoost * layer;
    const cacheFactor = groupCacheFactor * sectionCacheFactor * layerCacheFactor;

    const groupMoneyFactor = 1 + CONFIG.groupPriceBoost * group;
    const sectionMoneyFactor = 1 + CONFIG.sectionPriceBoost * section;
    const layerMoneyFactor = 1 + CONFIG.layerPriceBoost * layer;
    const moneyFactor = groupMoneyFactor * sectionMoneyFactor * layerMoneyFactor;

    const groupPopFactor = 1 + CONFIG.groupPopBoost * reversedGroup;
    const sectionPopFactor = 1 + CONFIG.sectionPopBoost * reversedSection;
    const layerPopFactor = 1 + CONFIG.layerPopBoost * reversedLayer;
    const popFactor = groupPopFactor * sectionPopFactor * layerPopFactor;

    return {
        cacheSize: Math.round(CONFIG.baseCache * cacheFactor),
        atrFactor: Number((CONFIG.baseAtr * moneyFactor).toFixed(3)),
        stopFactor: Number((CONFIG.baseStop * moneyFactor).toFixed(3)),
        minPriceMovement: Number((CONFIG.minPriceMove * moneyFactor).toFixed(3)),
        maxPriceMovement: Number((CONFIG.maxPriceMove * moneyFactor).toFixed(3)),
        pop: Math.round(CONFIG.basePop * popFactor)
    };
};

const saveLegionState = () => {
    db.transaction(() => {
        const controllers = structureMap.flat(3);
        for (const ctrl of controllers) {
            updateControllerStmt.run(
                ctrl.signalSpeed ?? 0,
                ctrl.memConnections ?? 0,
                JSON.stringify(ctrl.lastSignal ?? {}),
                JSON.stringify(ctrl.signalHistory ?? []),
                JSON.stringify(ctrl.probHistory ?? []),
                JSON.stringify(ctrl.scoreHistory ?? []),
                ctrl.group,
                ctrl.section,
                ctrl.layer,
                ctrl.id
            );
        }
        upsertCounterStmt.run(candleCounter);
    })();
};

const initLegion = () => {
    const counterRow = db.prepare('SELECT candle_counter FROM legion_state WHERE singleton = 1').get();
    let loadedCounter = counterRow ? counterRow.candle_counter : 0;
    if (!counterRow) {
        db.prepare('INSERT INTO legion_state (singleton, candle_counter) VALUES (1, 0)').run();
    }

    const expectedCount = CONFIG.dims.reduce((a, b) => a * b, 1);
    const currentCount = db.prepare('SELECT COUNT(*) AS count FROM legion_controllers').get().count;

    if (currentCount === 0) {
        structureMap = buildFreshStructure();

        const insertStmt = db.prepare(`
            INSERT INTO legion_controllers
            (group_id, section_id, layer_id, cluster_id, controller_type, directory_path)
            VALUES (?, ?, ?, ?, ?, ?)
        `);

        db.transaction(() => {
            structureMap.flat(3).forEach(ctrl => {
                insertStmt.run(
                    ctrl.group,
                    ctrl.section,
                    ctrl.layer,
                    ctrl.id,
                    ctrl.type,
                    ctrl.directoryPath
                );
            });
        })();
    } else {
        const rows = db.prepare('SELECT * FROM legion_controllers').all();
        const rowMap = new Map();
        rows.forEach(row => {
            const key = `${row.group_id}-${row.section_id}-${row.layer_id}-${row.cluster_id}`;
            rowMap.set(key, row);
        });

        structureMap = Array.from({ length: CONFIG.dims[0] }, (_, group) =>
            Array.from({ length: CONFIG.dims[1] }, (_, section) =>
                Array.from({ length: CONFIG.dims[2] }, (_, layer) =>
                    Array.from({ length: CONFIG.dims[3] }, (_, cluster) => {
                        const key = `${group}-${section}-${layer}-${cluster}`;
                        const row = rowMap.get(key);

                        if (!row) {
                            const half = Math.floor(CONFIG.dims[3] / 2);
                            const defType = cluster < half ? 'positive' : 'negative';
                            const defDir = path.join(CONFIG.stateFolder, `Group${group}`, `Section${section}`, `Layer${layer}`, `G${group}S${section}L${layer}C${cluster}`);
                            fs.mkdirSync(defDir, { recursive: true });

                            db.prepare(`
                                INSERT INTO legion_controllers
                                (group_id, section_id, layer_id, cluster_id, controller_type, directory_path)
                                VALUES (?, ?, ?, ?, ?, ?)
                            `).run(
                                group, section, layer, cluster,
                                defType, defDir
                            );

                            return {
                                type: defType,
                                directoryPath: defDir,
                                group,
                                section,
                                layer,
                                id: cluster,
                                signalSpeed: 0,
                                memConnections: 0,
                                lastSignal: {},
                                signalHistory: [],
                                probHistory: [],
                                scoreHistory: []
                            };
                        }

                        fs.mkdirSync(row.directory_path, { recursive: true });

                        return {
                            type: row.controller_type,
                            directoryPath: row.directory_path,
                            group: row.group_id,
                            section: row.section_id,
                            layer: row.layer_id,
                            id: row.cluster_id,
                            signalSpeed: row.signal_speed ?? 0,
                            memConnections: row.mem_connections ?? 0,
                            lastSignal: JSON.parse(row.last_signal ?? '{}'),
                            signalHistory: JSON.parse(row.signal_history ?? '[]'),
                            probHistory: JSON.parse(row.prob_history ?? '[]'),
                            scoreHistory: JSON.parse(row.score_history ?? '[]')
                        };
                    })
                )
            )
        );
    }

    return loadedCounter;
};

const processBatch = async () => {
    const currentBatch = candleCounter;
    let progressTracker = 0;
    const results = [];

    const allControllers = structureMap.flat(3);

    console.log('--');
    const controllerStart = performance.now();
    console.log(`> Processing controllers - ${progressTracker}/${allControllers.length} (${((progressTracker / allControllers.length) * 100).toFixed(2)}%)...`);

    for (let index = 0; index < allControllers.length; index += CONFIG.maxWorkers) {
        const chunk = allControllers.slice(index, index + CONFIG.maxWorkers);
        const promises = chunk.map(controller => runWorker(controller));

        let chunkResults;
        try {
            chunkResults = await Promise.all(promises);
        } catch (err) {
            console.error('Error during parallel processing:', err);
            process.exit(1);
        }

        chunkResults.forEach(result => {
            progressTracker++;

            process.stdout.moveCursor(0, -1);
            console.log(`> Processing controllers - ${progressTracker}/${allControllers.length} (${((progressTracker / allControllers.length) * 100).toFixed(2)}%)...`);

            results.push(result);
        });
    }

    const controllersEnd = performance.now();
    console.log(`> Processed all controllers in ${((controllersEnd - controllerStart) / 1000).toFixed(3)} seconds`);
    console.log('--');

    const memoryAnalyzeStart = performance.now();
    console.log(`> Analyzing controller memories...`);

    for (const { controller, signal, duration } of results) {
        controller.lastSignal = signal;

        controller.signalSpeed = duration;
        controller.signalHistory.push(duration);
        if (controller.signalHistory.length > 100) controller.signalHistory.shift();

        controller.probHistory.push(signal.prob);
        if (controller.probHistory.length > 100) controller.probHistory.shift();

        controller.scoreHistory.push(signal.score);
        if (controller.scoreHistory.length > 100) controller.scoreHistory.shift();

        const mb = controller.lastSignal.memoryBroadcast;

        if (mb && typeof mb.totalBroadcast === 'number' && mb.totalBroadcast > 0 && mb.compatibility && Array.isArray(mb.memories)) {
            const isPositive = controller.type === 'positive';
            const compatStr = canonicalJSON(mb.compatibility);

            const compatRow = (isPositive ? getCompatIdPos : getCompatIdNeg).get(compatStr);
            if (!compatRow) {
                mb.vaultAccess = [];
                mb.vaultMemories = 0;
                continue;
            }

            const compat_id = compatRow.id;

            const getCountCore = isPositive ? getCountCorePos : getCountCoreNeg;
            const getCountVol = isPositive ? getCountVolPos : getCountVolNeg;
            const selectAll = isPositive ? selectAllMemoriesPos : selectAllMemoriesNeg;
            const selectTopCore = isPositive ? selectTopCorePos : selectTopCoreNeg;
            const selectTopVol = isPositive ? selectTopVolPos : selectTopVolNeg;

            const countCore = getCountCore.get(compat_id) || 0;
            const countVol = getCountVol.get(compat_id) || 0;
            const totalCount = countCore + countVol;

            let rows;
            if (totalCount === 0) {
                mb.vaultAccess = [];
                mb.vaultMemories = 0;
                continue;
            } else if (totalCount <= CONFIG.maxVaultCandidates) {
                rows = selectAll.all(compat_id, compat_id);
            } else {
                const coreBias = 0.7;
                let limitCore = Math.round(CONFIG.maxVaultCandidates * coreBias);
                let limitVol = CONFIG.maxVaultCandidates - limitCore;

                limitCore = Math.min(limitCore, countCore || 0);
                limitVol = Math.min(limitVol, countVol || 0);

                if (countVol > 0 && limitVol < 100) {
                    const add = 100 - limitVol;
                    limitVol = 100;
                    limitCore = Math.max(0, limitCore - add);
                }

                const coreRows = selectTopCore.all(compat_id, limitCore);
                const volRows = selectTopVol.all(compat_id, limitVol);
                rows = [...coreRows, ...volRows];
            }

            let vaultCandidates = rows.map(row => ({
                source: row.source,
                protoId: row.protoId,
                mean: blobToVector(row.mean),
                variance: blobToVector(row.variance),
                size: row.size,
                accessCount: row.accessCount,
                importance: row.importance,
                contentHash: row.hash,
                lastAccessed: row.lastAccessed || 0
            }));

            vaultCandidates.forEach(cand => {
                const delta = currentBatch - cand.lastAccessed;
                if (delta <= 0) {
                    cand.decayMultiplier = 1;
                    return;
                }
                const decayFactor = cand.source === 'core' ? CONFIG.coreMemoryDecayFactor : CONFIG.volatileMemoryDecayFactor;
                const multiplier = Math.pow(decayFactor, delta);
                cand.decayMultiplier = multiplier;
                cand.size = Math.max(CONFIG.memoryDecayFloor, cand.size * multiplier);
                cand.accessCount = Math.max(CONFIG.memoryDecayFloor, cand.accessCount * multiplier);
                cand.importance = Math.max(CONFIG.memoryDecayFloor, cand.importance * multiplier);
            });

            if (vaultCandidates.length === 0) {
                mb.vaultAccess = [];
                mb.vaultMemories = 0;
                continue;
            }

            let hasQuery = false;
            let queryMean = null;
            let queryVar = null;
            let totalSize = 0;

            if (mb.memories.length > 0) {
                const firstMean = mb.memories[0]?.mean;
                if (firstMean && Array.isArray(firstMean)) {
                    const d = firstMean.length;
                    queryMean = new Array(d).fill(0);
                    const weightedVar = new Array(d).fill(0);

                    for (const mem of mb.memories) {
                        if (!mem.mean || !mem.variance || !Array.isArray(mem.mean)) continue;
                        const s = mem.size ?? 1;
                        totalSize += s;
                        for (let j = 0; j < d; j++) {
                            queryMean[j] += s * mem.mean[j];
                        }
                    }

                    if (totalSize > 0) {
                        for (let j = 0; j < d; j++) queryMean[j] /= totalSize;

                        for (const mem of mb.memories) {
                            if (!mem.mean || !mem.variance) continue;
                            const s = mem.size ?? 1;
                            for (let j = 0; j < d; j++) {
                                const dm = mem.mean[j] - queryMean[j];
                                weightedVar[j] += s * (mem.variance[j] + dm * dm);
                            }
                        }

                        queryVar = weightedVar.map(v => Math.max(v / totalSize, 1e-8));
                        hasQuery = true;
                    }
                }
            }

            if (hasQuery) {
                const distValues = [];
                for (const cand of vaultCandidates) {
                    cand.dist = computeGaussianDistance(queryMean, queryVar, cand.mean, cand.variance);
                    distValues.push(cand.dist);
                }

                distValues.sort((a, b) => a - b);
                const p80Idx = Math.floor(distValues.length * 0.8);
                let temp = distValues[p80Idx] || 1;
                if (temp <= 0) temp = 1;

                for (const cand of vaultCandidates) {
                    cand.similarity = Math.exp(-cand.dist / temp);
                }
            } else {
                for (const cand of vaultCandidates) {
                    cand.similarity = 1;
                }
            }

            let numToSelect = Math.max(1, Math.floor(mb.totalBroadcast * 0.4));
            numToSelect = Math.min(numToSelect, vaultCandidates.length);

            const selectedVault = [];

            vaultCandidates.sort((a, b) => {
                const scoreA = Math.pow(a.similarity, 4) * (a.importance + a.accessCount);
                const scoreB = Math.pow(b.similarity, 4) * (b.importance + b.accessCount);
                return scoreB - scoreA;
            });

            for (let i = 0; i < numToSelect; i++) {
                const selected = vaultCandidates[i];

                const accessStmt = selected.source === 'core'
                    ? (isPositive ? accessMemoryCorePos : accessMemoryCoreNeg)
                    : (isPositive ? accessMemoryVolPos : accessMemoryVolNeg);
                accessStmt.run(currentBatch, selected.protoId);

                selectedVault.push({
                    protoId: selected.protoId,
                    mean: selected.mean,
                    variance: selected.variance,
                    size: Math.max(CONFIG.memoryDecayFloor, selected.size / selected.decayMultiplier),
                    accessCount: Math.max(CONFIG.memoryDecayFloor, selected.accessCount / selected.decayMultiplier),
                    importance: Math.max(CONFIG.memoryDecayFloor, selected.importance / selected.decayMultiplier),
                    contentHash: selected.contentHash,
                    isCore: selected.source === 'core'
                });
            }

            const additionalVault = [];
            const capAdditional = mb.totalBroadcast - selectedVault.length;

            if (capAdditional > 0 && selectedVault.length > 0) {
                const coreSeeds = selectedVault.filter(m => m.isCore).map(m => m.protoId);
                const volSeeds = selectedVault.filter(m => !m.isCore).map(m => m.protoId);

                const types = [
                    {
                        source: 'core',
                        seeds: coreSeeds,
                        thresh: CONFIG.coreHierarchyThreshold,
                        decayF: CONFIG.coreMemoryDecayFactor,
                        selectStmt: isPositive ? selectMemCorePos : selectMemCoreNeg,
                        accessStmt: isPositive ? accessMemoryCorePos : accessMemoryCoreNeg
                    },
                    {
                        source: 'volatile',
                        seeds: volSeeds,
                        thresh: CONFIG.volatileHierarchyThreshold,
                        decayF: CONFIG.volatileMemoryDecayFactor,
                        selectStmt: isPositive ? selectMemVolPos : selectMemVolNeg,
                        accessStmt: isPositive ? accessMemoryVolPos : accessMemoryVolNeg
                    }
                ];

                for (const type of types) {
                    if (type.seeds.length === 0) continue;

                    const pq = new PriorityQueue();
                    const dist = new Map();
                    const visited = new Set();

                    for (const seed of type.seeds) {
                        pq.push({ protoId: seed, accumDist: 0, depth: 0 });
                        dist.set(seed, 0);
                    }

                    const sourceStr = type.source;
                    const polarityStr = isPositive ? 'positive' : 'negative';

                    while (pq.size > 0 && additionalVault.length < capAdditional) {
                        const curr = pq.pop();
                        if (visited.has(curr.protoId)) continue;
                        visited.add(curr.protoId);

                        if (curr.accumDist > 0) {
                            const row = type.selectStmt.get(curr.protoId);
                            if (!row) continue;

                            const delta = currentBatch - (row.lastAccessed || 0);
                            const mult = delta > 0 ? Math.pow(type.decayF, delta) : 1.0;

                            const decayedSize = Math.max(CONFIG.memoryDecayFloor, row.size * mult);
                            const decayedAccess = Math.max(CONFIG.memoryDecayFloor, row.accessCount * mult);
                            const decayedImportance = Math.max(CONFIG.memoryDecayFloor, row.importance * mult);

                            const similarity = Math.exp(-curr.accumDist / type.thresh);
                            const effectiveBoost = CONFIG.performanceBoostFactor * similarity;

                            const finalSize = Math.max(CONFIG.memoryDecayFloor, decayedSize * effectiveBoost);
                            const finalAccess = Math.max(CONFIG.memoryDecayFloor, decayedAccess * effectiveBoost);
                            const finalImportance = Math.max(CONFIG.memoryDecayFloor, decayedImportance * effectiveBoost);

                            additionalVault.push({
                                protoId: curr.protoId,
                                mean: blobToVector(row.mean),
                                variance: blobToVector(row.variance),
                                size: finalSize,
                                accessCount: finalAccess,
                                importance: finalImportance,
                                contentHash: row.hash,
                                isCore: type.source === 'core'
                            });

                            type.accessStmt.run(currentBatch, curr.protoId);
                        }

                        if (curr.depth >= CONFIG.hierarchyTraversalDepth) continue;

                        const neighborRows = getNeighbors.all(
                            sourceStr, polarityStr, curr.protoId,
                            sourceStr, polarityStr, curr.protoId
                        );

                        for (const nr of neighborRows) {
                            const neighId = nr.neighbor_proto;
                            if (visited.has(neighId)) continue;

                            const edgeDist = nr.accum_distance;
                            const newAccum = curr.accumDist + edgeDist;

                            const existing = dist.get(neighId);
                            if (existing === undefined || newAccum < existing) {
                                dist.set(neighId, newAccum);
                                pq.push({ protoId: neighId, accumDist: newAccum, depth: curr.depth + 1 });
                            }
                        }
                    }
                }
            }

            selectedVault.push(...additionalVault);
            mb.vaultAccess = selectedVault;
            mb.vaultMemories = selectedVault.length;
        } else {
            if (mb) {
                mb.vaultAccess = [];
                mb.vaultMemories = 0;
            }
        }
    }

    const performing = [];
    for (const res of results) {
        const ctrl = res.controller;
        const mb = ctrl.lastSignal?.memoryBroadcast;
        if (mb?.vaultMemories > 0) {
            const recent = ctrl.scoreHistory.slice(-10);
            if (recent.length > 0) {
                const avgScore = recent.reduce((a, b) => a + b, 0) / recent.length;
                performing.push({ ctrl, avgScore, mb });
            }
        }
    }

    if (performing.length > 0) {
        performing.sort((a, b) => b.avgScore - a.avgScore);

        memoryDb.transaction(() => {
            for (const res of results) {
                const mb = res.controller?.lastSignal?.memoryBroadcast;
                if (!mb?.vaultAccess?.length) continue;

                const isPositive = res.controller.type === 'positive';
                const avgScore = res.controller.scoreHistory.slice(-10).reduce((a,b)=>a+b,0) / 10 || 0;
                const isTopPerformer = avgScore >= (performing.length ? performing[Math.floor(performing.length * 0.3)]?.avgScore || 0 : 0);

                const boostF = isTopPerformer ? CONFIG.performanceBoostFactor : CONFIG.baseAccessBoost;

                for (const mem of mb.vaultAccess) {
                    const stmt = mem.isCore
                        ? (isPositive ? boostCorePos : boostCoreNeg)
                        : (isPositive ? boostVolatilePos : boostVolatileNeg);

                    stmt.run(boostF, boostF, boostF, currentBatch, mem.protoId);
                }
            }
        })();
    }

    const memoryAnalyzeEnd = performance.now();
    console.log(`> Analyzed controller memories in ${((memoryAnalyzeEnd - memoryAnalyzeStart) / 1000).toFixed(3)} seconds`);
    console.log('--');

    const memoryConsolidateStart = performance.now();

    const { posIdMap, negIdMap } = storeNewMemories(results, currentBatch);

    const consolidationTasks = [];
    for (const compat_id of Object.values(posIdMap)) {
        if (compat_id) consolidationTasks.push({ compat_id, isPositive: true });
    }
    for (const compat_id of Object.values(negIdMap)) {
        if (compat_id) consolidationTasks.push({ compat_id, isPositive: false });
    }

    const allDeltas = [];

    let consolProgress = 0;
    console.log(`> Consolidating vault memories - ${consolProgress}/${consolidationTasks.length} (${((consolProgress / (consolidationTasks.length || 1)) * 100).toFixed(2)}%)...`);

    for (let index = 0; index < consolidationTasks.length; index += CONFIG.maxWorkers) {
        const chunk = consolidationTasks.slice(index, index + CONFIG.maxWorkers);
        const promises = chunk.map(task => 
            runConsolidationWorker(task.compat_id, task.isPositive, currentBatch)
        );

        const chunkDeltas = await Promise.all(promises);
        allDeltas.push(...chunkDeltas);

        consolProgress += chunk.length;
        process.stdout.moveCursor(0, -1);
        console.log(`> Consolidating vault memories - ${consolProgress}/${consolidationTasks.length} (${((consolProgress / (consolidationTasks.length || 1)) * 100).toFixed(2)}%)...`);
    }

    const memoryConsolidateEnd = performance.now();
    console.log(`> Consolidated vault memories in ${((memoryConsolidateEnd- memoryConsolidateStart) / 1000).toFixed(3)} seconds`);
    console.log('--');

    const vaultStateStart = performance.now();
    console.log(`> Updating memory vault state...`);

    applyBulkDeltas(allDeltas);

    const coreCapacity = Math.floor(CONFIG.memoryVaultCapacity * CONFIG.coreCapacityRatio);

    const corePosCount = countCorePos.get();
    const coreNegCount = countCoreNeg.get();
    let coreTotal = corePosCount + coreNegCount;

    let purgedCores = 0;
    if (coreTotal > coreCapacity) {
        const excess = coreTotal - coreCapacity;

        const proportionPos = coreTotal > 0 ? corePosCount / coreTotal : 0.5;
        const purgeCorePosCount = Math.round(excess * proportionPos);
        const purgeCoreNegCount = excess - purgeCorePosCount;

        if (purgeCorePosCount > 0) {
            const { changes } = purgeCorePos.run(purgeCorePosCount);
            purgedCores += changes;
        }
        if (purgeCoreNegCount > 0) {
            const { changes } = purgeCoreNeg.run(purgeCoreNegCount);
            purgedCores += changes;
        }

        coreTotal -= purgedCores;
    }

    let currentTotal = countCorePos.get() + countCoreNeg.get() + countVolPos.get() + countVolNeg.get();

    if (currentTotal > CONFIG.memoryVaultCapacity) {
        const volPos = countVolPos.get();
        const volNeg = countVolNeg.get();
        const volatileTotal = volPos + volNeg;

        let excess = currentTotal - CONFIG.memoryVaultCapacity;
        if (volatileTotal < excess) excess = volatileTotal;

        const purgeVolPosCount = volatileTotal > 0 ? Math.round(excess * (volPos / volatileTotal)) : 0;
        const purgeVolNegCount = excess - purgeVolPosCount;

        let purgedVol = 0;
        if (purgeVolPosCount > 0) {
            const { changes } = purgeVolatilePos.run(purgeVolPosCount);
            purgedVol += changes;
        }
        if (purgeVolNegCount > 0) {
            const { changes } = purgeVolatileNeg.run(purgeVolNegCount);
            purgedVol += changes;
        }

        currentTotal -= purgedVol;
    }

    const vaultStateEnd = performance.now();
    console.log(`> Updated memory vault state in ${((vaultStateEnd- vaultStateStart) / 1000).toFixed(3)} seconds`);
    console.log('--');
};

const runWorker = async (controller) => {
    const params = getControllerParams(controller.group, controller.section, controller.layer);
    const { cacheSize, atrFactor, stopFactor, minPriceMovement, maxPriceMovement, pop } = params;

    const recentCandles = cache.slice(-cacheSize);

    const mainId = `${controller.group}${controller.section}${controller.layer}${controller.id}`;
    const mainCompat = controller.lastSignal?.memoryBroadcast?.compatibility;

    const peerSharedMem = structureMap.flat(3)
        .filter((c) => {
            const peerId = `${c.group}${c.section}${c.layer}${c.id}`;
            const peerCompat = c.lastSignal?.memoryBroadcast?.compatibility;

            return (
                mainCompat &&
                mainId !== peerId &&
                c.type === controller.type &&
                canonicalJSON(mainCompat ?? {}) === canonicalJSON(peerCompat ?? {})
            );
        })
        .map(c => c.lastSignal?.memoryBroadcast ?? {});

    controller.memConnections = peerSharedMem.length;

    return new Promise((resolve, reject) => {
        const worker = new Worker(new URL('./worker.js', import.meta.url), {
            workerData: {
                id: `G${controller.group}S${controller.section}L${controller.layer}C${controller.id}`,
                directoryPath: controller.directoryPath,
                cacheSize,
                pop,
                cache: recentCandles,
                type: controller.type,
                priceObj: {
                    atrFactor,
                    stopFactor,
                    minPriceMovement,
                    maxPriceMovement,
                },
                processCount: CONFIG.baseProcessCount,
                forceMin: CONFIG.forceMin,
                bcR : CONFIG.broadcastRatio,
                injR : CONFIG.injectionRatio,
                sharedMem : peerSharedMem
            },
        });

        worker.on('message', (msg) => {
            worker.terminate();
            if (msg.error) {
                reject(new Error(msg.error));
            } else {
                resolve({
                    controller,
                    signal: msg.signal,
                    duration: msg.duration,
                });
            }
        });

        worker.on('error', reject);
        worker.on('exit', (code) => {
            if (code !== 0) {
                reject(new Error(`Worker exited with code ${code}`));
            }
        });
    });
};

const runConsolidationWorker = async (compat_id, isPositive, currentBatch) => {
    return new Promise((resolve, reject) => {
        const worker = new Worker(new URL('./consolidation_worker.js', import.meta.url), {
            workerData: {
                compat_id,
                isPositive,
                currentBatch,
                config: {
                    volatileConsolidationThreshold: CONFIG.volatileConsolidationThreshold,
                    coreConsolidationThreshold: CONFIG.coreConsolidationThreshold,
                    volatileConsolidationLimit: CONFIG.volatileConsolidationLimit,
                    coreConsolidationLimit: CONFIG.coreConsolidationLimit,
                    consolidationPromoteCount: CONFIG.consolidationPromoteCount,
                    memoryDecayFloor: CONFIG.memoryDecayFloor,
                    volatileMemoryDecayFactor: CONFIG.volatileMemoryDecayFactor,
                    coreMemoryDecayFactor: CONFIG.coreMemoryDecayFactor,
                    maxHierarchyProtos: CONFIG.maxHierarchyProtos,
                    coreMinNeighbors: CONFIG.coreMinNeighbors,
                    coreMaxNeighbors: CONFIG.coreMaxNeighbors,
                    volatileMinNeighbors: CONFIG.volatileMinNeighbors,
                    volatileMaxNeighbors: CONFIG.volatileMaxNeighbors,
                }
            }
        });

        worker.on('message', (msg) => {
            worker.terminate();
            if (msg.error) {
                reject(new Error(msg.error));
            } else {
                resolve(msg.delta);
            }
        });

        worker.on('error', reject);
        worker.on('exit', (code) => {
            if (code !== 0) reject(new Error(`Consolidation worker exited with code ${code}`));
        });
    });
};

const processCandles = async () => {
    const fileStream = fs.createReadStream(CONFIG.file);
    const rd = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity,
    });

    const ipAddress = getLocalIP();

    const rebuildCounter = initLegion();

    spawnDedicatedHttpServer();
    broadcastLegionState('running');

    const maxGroup = CONFIG.dims[0] - 1;
    const maxSection = CONFIG.dims[1] - 1;
    const maxLayer = CONFIG.dims[2] - 1;
    const maxCacheFactor = (1 + CONFIG.groupCacheBoost * maxGroup) * (1 + CONFIG.sectionCacheBoost * maxSection) * (1 + CONFIG.layerCacheBoost * maxLayer);
    const maxCache = Math.round(CONFIG.baseCache * maxCacheFactor);

    for await (const line of rd) {
        if (!line.trim()) continue;

        let candle;
        try {
            candle = JSON.parse(line);
        } catch (e) {
            console.error('Invalid JSON line skipped:', line);
            continue;
        }

        cache.push(candle);
        if (cache.length > maxCache) {
            cache = cache.slice(-maxCache);
        }

        if (cache.length === maxCache) {
            candleCounter++;
            if (candleCounter <= rebuildCounter) continue;

            const finalStart = performance.now();
            console.log(`New candle (${candleCounter}) - http://${ipAddress}:${CONFIG.httpPort}`);

            await processBatch();
            saveLegionState();
            broadcastLegionState('running');

            const finalEnd = performance.now();
            console.log(`> All completed in ${((finalEnd - finalStart) / 1000).toFixed(3)} seconds`);

            console.log('-------------------------------------------------------');

            if (CONFIG.cutoff && candleCounter % CONFIG.cutoff === 0) {
                console.log(`Cutoff reached (${candleCounter}). Candle processing stopped.`);
                console.log(`HTTP state server is still running - http://${ipAddress}:${CONFIG.httpPort}`);

                broadcastLegionState('Stopped - Cuttoff reached');

                fileStream.destroy();
                rd.close();
                return;
            }
        }
    }

    console.log('End of file reached. Processing complete.');
    console.log(`HTTP state server is still running - http://${ipAddress}:${CONFIG.httpPort}`);

    broadcastLegionState(`Stopped - End of file reached`);

    await new Promise(() => {});
};

processCandles().catch(err => {
    console.error('Unexpected error during processing:', err);
});