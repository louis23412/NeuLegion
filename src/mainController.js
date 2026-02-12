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

const CONFIG = {
    cutoff: null,
    baseProcessCount: 1,
    forceMin: true,
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

    volatileMemoryDecayFactor: 0.999,
    coreMemoryDecayFactor: 0.9995,
    memoryDecayFloor: 1,
    volatileConsolidationThreshold: 0.08,
    coreConsolidationThreshold: 0.04,
    consolidationPromoteCount: 25,
    coreCapacityRatio: 0.333,
    performanceBoostFactor: 1.075,
    maxVaultCandidates: 10000,
    memoryVaultCapacity: 5000000,
    volatileConsolidationLimit: 1500,
    coreConsolidationLimit: 600
};

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
        mean TEXT NOT NULL,
        variance TEXT NOT NULL,
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
        mean TEXT NOT NULL,
        variance TEXT NOT NULL,
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
        mean TEXT NOT NULL,
        variance TEXT NOT NULL,
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
        mean TEXT NOT NULL,
        variance TEXT NOT NULL,
        size REAL NOT NULL,
        accessCount REAL NOT NULL,
        importance REAL NOT NULL,
        hash TEXT NOT NULL,
        lastAccessed INTEGER DEFAULT 0,
        usageCount INTEGER NOT NULL DEFAULT 0,
        merged_count INTEGER NOT NULL DEFAULT 1
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
`);

const insertCompatPos = memoryDb.prepare('INSERT INTO compat_positive (compatibility) VALUES (?) ON CONFLICT(compatibility) DO NOTHING');
const insertCompatNeg = memoryDb.prepare('INSERT INTO compat_negative (compatibility) VALUES (?) ON CONFLICT(compatibility) DO NOTHING');

const getCompatIdPos = memoryDb.prepare('SELECT id FROM compat_positive WHERE compatibility = ?');
const getCompatIdNeg = memoryDb.prepare('SELECT id FROM compat_negative WHERE compatibility = ?');

const upsertCorePos = memoryDb.prepare(`
    INSERT INTO core_positive (protoId, compat_id, mean, variance, size, accessCount, importance, hash, usageCount)
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

const upsertCoreNeg = memoryDb.prepare(`
    INSERT INTO core_negative (protoId, compat_id, mean, variance, size, accessCount, importance, hash, usageCount)
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

const updateLastCorePos = memoryDb.prepare(
    'UPDATE core_positive SET lastAccessed = ? WHERE protoId = ?'
);
const updateLastCoreNeg = memoryDb.prepare(
    'UPDATE core_negative SET lastAccessed = ? WHERE protoId = ?'
);
const updateLastVolPos = memoryDb.prepare(
    'UPDATE volatile_positive SET lastAccessed = ? WHERE protoId = ?'
);
const updateLastVolNeg = memoryDb.prepare(
    'UPDATE volatile_negative SET lastAccessed = ? WHERE protoId = ?'
);

const existsInCorePos = memoryDb.prepare('SELECT 1 FROM core_positive WHERE protoId = ?').pluck();
const existsInCoreNeg = memoryDb.prepare('SELECT 1 FROM core_negative WHERE protoId = ?').pluck();

const deleteFromVolatilePos = memoryDb.prepare('DELETE FROM volatile_positive WHERE protoId = ?');
const deleteFromVolatileNeg = memoryDb.prepare('DELETE FROM volatile_negative WHERE protoId = ?');

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
        lastAccessed = ?
    WHERE protoId = ?
`);

const boostCoreNeg = memoryDb.prepare(`
    UPDATE core_negative
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?
    WHERE protoId = ?
`);

const boostVolatilePos = memoryDb.prepare(`
    UPDATE volatile_positive
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?
    WHERE protoId = ?
`);

const boostVolatileNeg = memoryDb.prepare(`
    UPDATE volatile_negative
    SET importance = importance * ?,
        accessCount = accessCount * ?,
        size = size * ?,
        lastAccessed = ?
    WHERE protoId = ?
`);

const updateVolatilePos = memoryDb.prepare(`
    UPDATE volatile_positive
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const updateVolatileNeg = memoryDb.prepare(`
    UPDATE volatile_negative
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const updateCorePos = memoryDb.prepare(`
    UPDATE core_positive
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const updateCoreNeg = memoryDb.prepare(`
    UPDATE core_negative
    SET mean = ?, variance = ?, size = ?, accessCount = ?, importance = ?, hash = ?, lastAccessed = ?, usageCount = ?, merged_count = ?
    WHERE protoId = ?
`);

const deleteFromCorePos = memoryDb.prepare('DELETE FROM core_positive WHERE protoId = ?');
const deleteFromCoreNeg = memoryDb.prepare('DELETE FROM core_negative WHERE protoId = ?');

const promoteToCorePos = memoryDb.prepare(`
    INSERT INTO core_positive
    (protoId, compat_id, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const promoteToCoreNeg = memoryDb.prepare(`
    INSERT INTO core_negative
    (protoId, compat_id, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

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

const computeSymKL = (mu1, var1, mu2, var2) => {
    const d = mu1.length;
    if (d !== mu2.length || d === 0) return Infinity;

    const eps = 1e-8;
    let logTerm12 = 0, traceTerm12 = 0;
    let logTerm21 = 0, traceTerm21 = 0;

    for (let j = 0; j < d; j++) {
        const v1 = Math.max(var1[j], eps);
        const v2 = Math.max(var2[j], eps);
        const dm = mu1[j] - mu2[j];

        logTerm12 += Math.log(v2 / v1);
        traceTerm12 += (v1 + dm * dm) / v2;

        logTerm21 += Math.log(v1 / v2);
        traceTerm21 += (v2 + dm * dm) / v1;
    }

    const kl12 = 0.5 * (logTerm12 + traceTerm12 - d);
    const kl21 = 0.5 * (logTerm21 + traceTerm21 - d);

    return (kl12 + kl21) / 2;
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

const consolidateVolatile = (compat_id, isPositive, currentBatch) => {
    const volTable = isPositive ? 'volatile_positive' : 'volatile_negative';
    const updateStmt = isPositive ? updateVolatilePos : updateVolatileNeg;
    const deleteStmt = isPositive ? deleteFromVolatilePos : deleteFromVolatileNeg;
    const promoteStmt = isPositive ? promoteToCorePos : promoteToCoreNeg;
    const existsCoreStmt = isPositive ? existsInCorePos : existsInCoreNeg;

    const rows = memoryDb.prepare(`
        SELECT protoId, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count
        FROM ${volTable} 
        WHERE compat_id = ?
        ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
        LIMIT ?
    `).all(compat_id, CONFIG.volatileConsolidationLimit);

    if (rows.length < 2) return;

    let mems = rows.map(r => ({
        protoId: r.protoId,
        mean: JSON.parse(r.mean),
        variance: JSON.parse(r.variance),
        size: r.size,
        accessCount: r.accessCount,
        importance: r.importance,
        hash: r.hash,
        lastAccessed: r.lastAccessed,
        usageCount: r.usageCount,
        merged_count: r.merged_count || 1
    }));

    mems.forEach(mem => {
        const delta = currentBatch - mem.lastAccessed;
        if (delta > 0) {
            const multiplier = Math.pow(CONFIG.volatileMemoryDecayFactor, delta);
            mem.size = Math.max(CONFIG.memoryDecayFloor, mem.size * multiplier);
            mem.accessCount = Math.max(CONFIG.memoryDecayFloor, mem.accessCount * multiplier);
            mem.importance = Math.max(CONFIG.memoryDecayFloor, mem.importance * multiplier);
        }
    });

    mems.sort((a, b) => b.importance - a.importance || b.size - a.size);

    let i = 0;
    while (i < mems.length - 1) {
        let j = i + 1;
        while (j < mems.length) {
            const kl = computeSymKL(mems[i].mean, mems[i].variance, mems[j].mean, mems[j].variance);
            if (kl < CONFIG.volatileConsolidationThreshold) {
                const target = mems[i];
                const source = mems[j];
                const s1 = target.size;
                const s2 = source.size;
                const totalSize = s1 + s2;

                const newMean = target.mean.map((m, k) => (s1 * m + s2 * source.mean[k]) / totalSize);
                const newVariance = target.mean.map((_, k) => {
                    const dm1 = target.mean[k] - newMean[k];
                    const dm2 = source.mean[k] - newMean[k];
                    return (s1 * (target.variance[k] + dm1 * dm1) + s2 * (source.variance[k] + dm2 * dm2)) / totalSize;
                }).map(v => Math.max(v, 1e-8));

                target.mean = newMean;
                target.variance = newVariance;
                target.size = totalSize;
                target.accessCount = (s1 * target.accessCount + s2 * source.accessCount) / totalSize;
                target.importance = (s1 * target.importance + s2 * source.importance) / totalSize;
                target.usageCount += source.usageCount;
                target.merged_count += source.merged_count;
                target.lastAccessed = Math.max(target.lastAccessed, source.lastAccessed, currentBatch);
                target.hash = target.hash || source.hash;

                updateStmt.run(
                    JSON.stringify(target.mean),
                    JSON.stringify(target.variance),
                    target.size,
                    target.accessCount,
                    target.importance,
                    target.hash,
                    target.lastAccessed,
                    target.usageCount,
                    target.merged_count,
                    target.protoId
                );
                deleteStmt.run(source.protoId);
                mems.splice(j, 1);
            } else {
                j++;
            }
        }
        i++;
    }

    for (const mem of mems) {
        if (mem.merged_count >= CONFIG.consolidationPromoteCount && !existsCoreStmt.get(mem.protoId)) {
            promoteStmt.run(
                mem.protoId,
                compat_id,
                JSON.stringify(mem.mean),
                JSON.stringify(mem.variance),
                mem.size,
                mem.accessCount,
                mem.importance,
                mem.hash,
                currentBatch,
                mem.usageCount,
                mem.merged_count
            );
            deleteStmt.run(mem.protoId);
        }
    }
};

const consolidateCore = (compat_id, isPositive, currentBatch) => {
    const coreTable = isPositive ? 'core_positive' : 'core_negative';
    const updateStmt = isPositive ? updateCorePos : updateCoreNeg;
    const deleteStmt = isPositive ? deleteFromCorePos : deleteFromCoreNeg;

    const rows = memoryDb.prepare(`
        SELECT protoId, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count
        FROM ${coreTable} 
        WHERE compat_id = ?
        ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
        LIMIT ?
    `).all(compat_id, CONFIG.coreConsolidationLimit);

    if (rows.length < 2) return;

    let mems = rows.map(r => ({
        protoId: r.protoId,
        mean: JSON.parse(r.mean),
        variance: JSON.parse(r.variance),
        size: r.size,
        accessCount: r.accessCount,
        importance: r.importance,
        hash: r.hash,
        lastAccessed: r.lastAccessed,
        usageCount: r.usageCount,
        merged_count: r.merged_count || 1
    }));

    mems.forEach(mem => {
        const delta = currentBatch - mem.lastAccessed;
        if (delta > 0) {
            const multiplier = Math.pow(CONFIG.coreMemoryDecayFactor, delta);
            mem.size = Math.max(CONFIG.memoryDecayFloor, mem.size * multiplier);
            mem.accessCount = Math.max(CONFIG.memoryDecayFloor, mem.accessCount * multiplier);
            mem.importance = Math.max(CONFIG.memoryDecayFloor, mem.importance * multiplier);
        }
    });

    mems.sort((a, b) => b.importance - a.importance || b.size - a.size);

    let i = 0;
    while (i < mems.length - 1) {
        let j = i + 1;
        while (j < mems.length) {
            const kl = computeSymKL(mems[i].mean, mems[i].variance, mems[j].mean, mems[j].variance);
            if (kl < CONFIG.coreConsolidationThreshold) {
                const target = mems[i];
                const source = mems[j];
                const s1 = target.size;
                const s2 = source.size;
                const totalSize = s1 + s2;

                const newMean = target.mean.map((m, k) => (s1 * m + s2 * source.mean[k]) / totalSize);
                const newVariance = target.mean.map((_, k) => {
                    const dm1 = target.mean[k] - newMean[k];
                    const dm2 = source.mean[k] - newMean[k];
                    return (s1 * (target.variance[k] + dm1 * dm1) + s2 * (source.variance[k] + dm2 * dm2)) / totalSize;
                }).map(v => Math.max(v, 1e-8));

                target.mean = newMean;
                target.variance = newVariance;
                target.size = totalSize;
                target.accessCount = (s1 * target.accessCount + s2 * source.accessCount) / totalSize;
                target.importance = (s1 * target.importance + s2 * source.importance) / totalSize;
                target.usageCount += source.usageCount;
                target.merged_count += source.merged_count;
                target.lastAccessed = Math.max(target.lastAccessed, source.lastAccessed, currentBatch);
                target.hash = target.hash || source.hash;

                updateStmt.run(
                    JSON.stringify(target.mean),
                    JSON.stringify(target.variance),
                    target.size,
                    target.accessCount,
                    target.importance,
                    target.hash,
                    target.lastAccessed,
                    target.usageCount,
                    target.merged_count,
                    target.protoId
                );
                deleteStmt.run(source.protoId);
                mems.splice(j, 1);
            } else {
                j++;
            }
        }
        i++;
    }
};

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
            targetArray.push({
                mem: indivMem,
                compatStr,
                isCore: !!indivMem.isCore
            });
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

    for (const { mem, compatStr, isCore } of posMems) {
        const compat_id = posIdMap[compatStr];
        if (!compat_id) continue;

        if (isCore) {
            deleteFromVolatilePos.run(mem.protoId);
            upsertCorePos.run(
                mem.protoId,
                compat_id,
                JSON.stringify(mem.mean),
                JSON.stringify(mem.variance),
                mem.size ?? 0.0,
                mem.accessCount ?? 0.0,
                mem.importance ?? 0.0,
                mem.contentHash ?? ''
            );
            updateLastCorePos.run(currentBatch, mem.protoId);
        } else {
            if (existsInCorePos.get(mem.protoId)) continue;
            upsertVolatilePos.run(
                mem.protoId,
                compat_id,
                JSON.stringify(mem.mean),
                JSON.stringify(mem.variance),
                mem.size ?? 0.0,
                mem.accessCount ?? 0.0,
                mem.importance ?? 0.0,
                mem.contentHash ?? ''
            );
            updateLastVolPos.run(currentBatch, mem.protoId);
        }
    }

    for (const { mem, compatStr, isCore } of negMems) {
        const compat_id = negIdMap[compatStr];
        if (!compat_id) continue;

        if (isCore) {
            deleteFromVolatileNeg.run(mem.protoId);
            upsertCoreNeg.run(
                mem.protoId,
                compat_id,
                JSON.stringify(mem.mean),
                JSON.stringify(mem.variance),
                mem.size ?? 0.0,
                mem.accessCount ?? 0.0,
                mem.importance ?? 0.0,
                mem.contentHash ?? ''
            );
            updateLastCoreNeg.run(currentBatch, mem.protoId);
        } else {
            if (existsInCoreNeg.get(mem.protoId)) continue;
            upsertVolatileNeg.run(
                mem.protoId,
                compat_id,
                JSON.stringify(mem.mean),
                JSON.stringify(mem.variance),
                mem.size ?? 0.0,
                mem.accessCount ?? 0.0,
                mem.importance ?? 0.0,
                mem.contentHash ?? ''
            );
            updateLastVolNeg.run(currentBatch, mem.protoId);
        }
    }

    return { posIdMap, negIdMap };
});

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
    console.log(`New batch(${candleCounter}):`);

    const totalStart = performance.now();
    const currentBatch = candleCounter;
    let progressTracker = 0;
    const results = [];

    const allControllers = structureMap.flat(3);

    console.log(`Processed ${progressTracker}/${allControllers.length} controllers (${((progressTracker / allControllers.length) * 100).toFixed(2)}%)...`);

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
            console.log(`Processed ${progressTracker}/${allControllers.length} controllers (${((progressTracker / allControllers.length) * 100).toFixed(2)}%)...`);

            results.push(result);
        });
    }

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
                mean: JSON.parse(row.mean),
                variance: JSON.parse(row.variance),
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
                const klValues = [];
                for (const cand of vaultCandidates) {
                    cand.symKL = computeSymKL(queryMean, queryVar, cand.mean, cand.variance);
                    klValues.push(cand.symKL);
                }

                klValues.sort((a, b) => a - b);
                const p80Idx = Math.floor(klValues.length * 0.8);
                let temp = klValues[p80Idx] || 1;
                if (temp <= 0) temp = 1;

                for (const cand of vaultCandidates) {
                    cand.similarity = Math.exp(-cand.symKL / temp);
                }
            } else {
                for (const cand of vaultCandidates) {
                    cand.similarity = 1;
                }
            }

            let numToSelect = Math.max(1, Math.floor(mb.totalBroadcast * 0.25));
            numToSelect = Math.min(numToSelect, vaultCandidates.length);

            const selectedVault = [];
            let pool = [...vaultCandidates];

            for (let i = 0; i < numToSelect && pool.length > 0; i++) {
                const weights = pool.map(c => c.similarity * c.importance + 1);
                const sumWeights = weights.reduce((a, b) => a + b, 0);

                let selected;
                if (sumWeights === 0 || !isFinite(sumWeights)) {
                    const idx = Math.floor(Math.random() * pool.length);
                    selected = pool.splice(idx, 1)[0];
                } else {
                    let r = Math.random() * sumWeights;
                    let cum = 0;
                    for (let k = 0; k < weights.length; k++) {
                        cum += weights[k];
                        if (cum >= r) {
                            selected = pool.splice(k, 1)[0];
                            break;
                        }
                    }
                    if (!selected) selected = pool.pop();
                }

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

        const topCount = Math.max(1, Math.ceil(performing.length * 0.3));
        const threshold = performing[topCount - 1].avgScore;

        const boostFactor = CONFIG.performanceBoostFactor;

        memoryDb.transaction(() => {
            for (const p of performing) {
                if (p.avgScore < threshold) continue;

                const isPositive = p.ctrl.type === 'positive';

                for (const mem of p.mb.vaultAccess) {
                    const stmt = mem.isCore
                        ? (isPositive ? boostCorePos : boostCoreNeg)
                        : (isPositive ? boostVolatilePos : boostVolatileNeg);

                    stmt.run(boostFactor, boostFactor, boostFactor, currentBatch, mem.protoId);
                }
            }
        })();
    }

    const { posIdMap, negIdMap } = storeNewMemories(results, currentBatch);

    for (const compat_id of Object.values(posIdMap)) {
        if (compat_id) {
            consolidateVolatile(compat_id, true, currentBatch);
            consolidateCore(compat_id, true, currentBatch);
        }
    }
    for (const compat_id of Object.values(negIdMap)) {
        if (compat_id) {
            consolidateVolatile(compat_id, false, currentBatch);
            consolidateCore(compat_id, false, currentBatch);
        }
    }

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

    const totalEnd = performance.now();
    console.log(`Batch completed in ${((totalEnd - totalStart) / 1000).toFixed(3)} seconds`);
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

const processCandles = async () => {
    const fileStream = fs.createReadStream(CONFIG.file);
    const rd = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity,
    });

    const rebuildCounter = initLegion();

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

            await processBatch();
            saveLegionState();

            console.log('-------------------------------------------------------');

            if (CONFIG.cutoff && candleCounter % CONFIG.cutoff === 0) {
                console.log(`Done! Cutoff = ${candleCounter}`);
                process.exit();
            }
        }
    }

    console.log('End of file reached. Processing complete.');
};

processCandles().catch(err => {
    console.error('Unexpected error during processing:', err);
});