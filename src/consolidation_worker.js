import { parentPort, workerData } from 'node:worker_threads';
import Database from 'better-sqlite3';
import path from 'path';

const { compat_id, isPositive, currentBatch, config } = workerData;

const stateFolder = path.join(import.meta.dirname, '..', 'state');
const dbPath = path.join(stateFolder, 'main', 'memory_vault.db');

const memoryDb = new Database(dbPath);
memoryDb.pragma('journal_mode = WAL');
memoryDb.pragma('synchronous = NORMAL');
memoryDb.pragma('temp_store = MEMORY');
memoryDb.pragma('cache_size = -128000');
memoryDb.pragma('query_only = ON');

const polarity = isPositive ? 'positive' : 'negative';

const blobToVector = (blob) => {
    if (!Buffer.isBuffer(blob) || blob.length === 0 || blob.length % 8 !== 0) return [];
    const f64 = new Float64Array(blob.buffer, blob.byteOffset, blob.length / 8);
    return Array.from(f64); // keeps compatibility with existing number[] code
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

const computeContentHash = (mean) => {
    let hash = 2166136261;
    for (let i = 0; i < mean.length; i++) {
        let iv = Math.floor(mean[i] * 10000 + 0.5);
        hash ^= iv;
        hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
    }
    return ((hash >>> 0) % 0xFFFFFFFF).toString(16).padStart(8, '0');
};

const consolidateVolatile = () => {
    const volTable = isPositive ? 'volatile_positive' : 'volatile_negative';
    const loadStmt = memoryDb.prepare(`
        SELECT protoId, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count
        FROM ${volTable}
        WHERE compat_id = ?
        ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
        LIMIT ?
    `);

    const rows = loadStmt.all(compat_id, config.volatileConsolidationLimit);
    if (rows.length < 2) return { updates: [], deletes: [], promotes: [] };

    let mems = rows.map(r => ({
        protoId: r.protoId,
        mean: blobToVector(r.mean),
        variance: blobToVector(r.variance),
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
            const multiplier = Math.pow(config.volatileMemoryDecayFactor, delta);
            mem.size = Math.max(config.memoryDecayFloor, mem.size * multiplier);
            mem.accessCount = Math.max(config.memoryDecayFloor, mem.accessCount * multiplier);
            mem.importance = Math.max(config.memoryDecayFloor, mem.importance * multiplier);
        }
    });

    mems.sort((a, b) => b.importance - a.importance || b.size - a.size);

    const updates = [];
    const deletes = [];
    const promotes = [];

    let i = 0;
    while (i < mems.length - 1) {
        let j = i + 1;
        while (j < mems.length) {
            const dist = computeGaussianDistance(mems[i].mean, mems[i].variance, mems[j].mean, mems[j].variance);
            if (dist < config.volatileConsolidationThreshold) {
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
                target.hash = computeContentHash(newMean);

                updates.push({
                    protoId: target.protoId,
                    mean: target.mean,
                    variance: target.variance,
                    size: target.size,
                    accessCount: target.accessCount,
                    importance: target.importance,
                    hash: target.hash,
                    lastAccessed: target.lastAccessed,
                    usageCount: target.usageCount,
                    merged_count: target.merged_count
                });

                deletes.push(source.protoId);
                mems.splice(j, 1);
            } else {
                j++;
            }
        }
        i++;
    }

    for (const mem of mems) {
        if (mem.merged_count >= config.consolidationPromoteCount) {
            const promoteHash = computeContentHash(mem.mean);
            promotes.push({
                protoId: mem.protoId,
                mean: mem.mean,
                variance: mem.variance,
                size: mem.size,
                accessCount: mem.accessCount,
                importance: mem.importance,
                hash: promoteHash,
                lastAccessed: currentBatch,
                usageCount: mem.usageCount,
                merged_count: mem.merged_count
            });
            deletes.push(mem.protoId);
        }
    }

    return { updates, deletes, promotes };
}

const consolidateCore = () => {
    const coreTable = isPositive ? 'core_positive' : 'core_negative';
    const loadStmt = memoryDb.prepare(`
        SELECT protoId, mean, variance, size, accessCount, importance, hash, lastAccessed, usageCount, merged_count
        FROM ${coreTable}
        WHERE compat_id = ?
        ORDER BY importance DESC, accessCount DESC, lastAccessed DESC
        LIMIT ?
    `);

    const rows = loadStmt.all(compat_id, config.coreConsolidationLimit);
    if (rows.length < 2) return { updates: [], deletes: [] };

    let mems = rows.map(r => ({
        protoId: r.protoId,
        mean: blobToVector(r.mean),
        variance: blobToVector(r.variance),
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
            const multiplier = Math.pow(config.coreMemoryDecayFactor, delta);
            mem.size = Math.max(config.memoryDecayFloor, mem.size * multiplier);
            mem.accessCount = Math.max(config.memoryDecayFloor, mem.accessCount * multiplier);
            mem.importance = Math.max(config.memoryDecayFloor, mem.importance * multiplier);
        }
    });

    mems.sort((a, b) => b.importance - a.importance || b.size - a.size);

    const updates = [];
    const deletes = [];

    let i = 0;
    while (i < mems.length - 1) {
        let j = i + 1;
        while (j < mems.length) {
            const dist = computeGaussianDistance(mems[i].mean, mems[i].variance, mems[j].mean, mems[j].variance);
            if (dist < config.coreConsolidationThreshold) {
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
                target.hash = computeContentHash(newMean);

                updates.push({
                    protoId: target.protoId,
                    mean: target.mean,
                    variance: target.variance,
                    size: target.size,
                    accessCount: target.accessCount,
                    importance: target.importance,
                    hash: target.hash,
                    lastAccessed: target.lastAccessed,
                    usageCount: target.usageCount,
                    merged_count: target.merged_count
                });

                deletes.push(source.protoId);
                mems.splice(j, 1);
            } else {
                j++;
            }
        }
        i++;
    }

    return { updates, deletes };
}

const buildPrototypeHierarchy = (memType) => {
    const decayFactor = memType === 'core' ? config.coreMemoryDecayFactor : config.volatileMemoryDecayFactor;
    const table = memType === 'core'
        ? (isPositive ? 'core_positive' : 'core_negative')
        : (isPositive ? 'volatile_positive' : 'volatile_negative');

    const loadStmt = memoryDb.prepare(`
        SELECT protoId, mean, variance, size, accessCount, importance, hash, lastAccessed
        FROM ${table}
        WHERE compat_id = ?
    `);

    const rows = loadStmt.all(compat_id);
    if (rows.length < 2) return { deletes: [], inserts: [] };

    let protos = rows.map(r => ({
        protoId: r.protoId,
        mean: blobToVector(r.mean),
        variance: blobToVector(r.variance),
        size: r.size,
        accessCount: r.accessCount,
        importance: r.importance,
        hash: r.hash,
        lastAccessed: r.lastAccessed
    }));

    protos.forEach(p => {
        const delta = currentBatch - p.lastAccessed;
        if (delta > 0) {
            const mult = Math.pow(decayFactor, delta);
            p.size = Math.max(config.memoryDecayFloor, p.size * mult);
            p.accessCount = Math.max(config.memoryDecayFloor, p.accessCount * mult);
            p.importance = Math.max(config.memoryDecayFloor, p.importance * mult);
        }
    });

    protos.sort((a, b) =>
        b.importance - a.importance ||
        b.size - a.size ||
        b.accessCount - a.accessCount ||
        b.lastAccessed - a.lastAccessed
    );

    const limitedProtos = protos.slice(0, config.maxHierarchyProtos);
    if (limitedProtos.length < 2) return { deletes: [], inserts: [] };

    const n = limitedProtos.length;
    const deletes = [];
    const inserts = [];

    for (const p of limitedProtos) {
        deletes.push({ source: memType, polarity, protoId: p.protoId });
    }

    const minNeighbors = memType === 'core' ? config.coreMinNeighbors : config.volatileMinNeighbors;
    const maxNeighbors = memType === 'core' ? config.coreMaxNeighbors : config.volatileMaxNeighbors;

    for (let i = 0; i < n; i++) {
        const proto = limitedProtos[i];
        const fraction = n <= 1 ? 0 : i / (n - 1);
        let numNeighbors = Math.round(minNeighbors + (maxNeighbors - minNeighbors) * (1 - fraction));
        numNeighbors = Math.max(1, numNeighbors);

        const candidates = [];
        for (let j = 0; j < n; j++) {
            if (i === j) continue;
            const other = limitedProtos[j];
            const dist = computeGaussianDistance(proto.mean, proto.variance, other.mean, other.variance);
            candidates.push({ neighborId: other.protoId, dist });
        }

        candidates.sort((a, b) => a.dist - b.dist);

        const actualNum = Math.min(numNeighbors, candidates.length);
        for (let k = 0; k < actualNum; k++) {
            const { neighborId, dist } = candidates[k];
            inserts.push({
                source: memType,
                polarity,
                parent_proto: proto.protoId,
                child_proto: neighborId,
                accum_distance: dist
            });
            inserts.push({
                source: memType,
                polarity,
                parent_proto: neighborId,
                child_proto: proto.protoId,
                accum_distance: dist
            });
        }
    }

    return { deletes, inserts };
}

try {
    const volatileChanges = consolidateVolatile();
    const volatileEdges = buildPrototypeHierarchy('volatile');
    const coreChanges = consolidateCore();
    const coreEdges = buildPrototypeHierarchy('core');

    parentPort.postMessage({
        success: true,
        delta: {
            compat_id,
            isPositive,
            volatile: volatileChanges,
            core: coreChanges,
            edges: {
                deletes: [...volatileEdges.deletes, ...coreEdges.deletes],
                inserts: [...volatileEdges.inserts, ...coreEdges.inserts]
            }
        }
    });
} catch (err) {
    parentPort.postMessage({ error: err.message || String(err) });
}