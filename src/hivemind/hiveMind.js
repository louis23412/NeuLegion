import fs from 'fs';
import path from 'path';
import Database from 'better-sqlite3';

import { isValidNumber } from './utils.js';

class HiveMind {
    #directoryPath; #fileName; 
    #ensembleSize; #inputSize;
    #numLayers; #numHeads; #headDim; #hiddenSize; #feedForwardSize; 
    #contextWindow; #adaptiveWindow; #semanticMaxProtos;
    #maxTrustHistory; #maxPerformanceHistory;
    #learningRate; #learningRateDecay;
    #swarmIntelligenceFactor; ;#gradientResetFrequency;
    #adaptiveLearningRate; #ensembleWeights; #agreementScores;
    #performanceScores; #historicalPerformance; #trustScoresHistory;
    #specializationScores; #specializationWeights;
    #attentionWeightMatrix; #attentionBias;
    #transformers; #gradientAccumulation;
    #attentionMemory; #adaptiveContext; #semanticProtos;
    #semanticLR; #semanticBoost; #effectiveSemanticMax;
    #longTermMaxProtos; #shortTermMaxProtos; #rawMaxProtos;
    #lowDim; #numProjections; #projectionMatrices;
    #semanticMergeEvery; #kernelGamma;
    #maxRetrievedProtos; #numRetrievalCandidates;
    #maxEpisodicConsider; #replaySamples;
    #coreMaxProtos; #coreEpisodicMaxEntries; #coreEpisodic;
    #lshNumTables; #lshHashBits;
    #numLshSets; #lshHyperplanes; #semanticLSHBuckets;
    #priorityMax; #priorityIndices;
    #protoCapacityFactor; #baseProtoCapacity; #memoryFactor; #maxVariancePerDim;
    #tempOverloadFactor; #mergeTrimFactor;
    #trainingStepCount = 0;

    constructor (dp, es, is, id, forceMin = false) {
        this.#directoryPath = dp;
        this.#fileName = `hivemind_state-ID=${id}-es=${es}-is=${is}.db`;

        const loadStatus = this.#loadState(es, is, forceMin);

        if (!loadStatus.status) {
            console.log(`Load state failed! Error: ${loadStatus.error}. Trace: ${loadStatus.trace}`);
            process.exit();
        }
    }

    #loadState (ensembleSize, inputSize, forceMin) {
        const dbPath = path.join(this.#directoryPath, this.#fileName);
        let db;

        try {
            if (!fs.existsSync(dbPath)) {
                this.#scaleAndSetDimensions(ensembleSize, inputSize, forceMin);
                return { status : true , message : 'Started with new state!'};
            }

            db = new Database(dbPath, { readonly: true });

            const getMetadata = db.prepare('SELECT value FROM metadata WHERE key = ?');

            const scalarKeys = [
                'ensembleSize', 'inputSize', 'numLayers', 'numHeads', 'headDim', 'hiddenSize',
                'feedForwardSize', 'contextWindow', 'adaptiveWindow', 'semanticMaxProtos',
                'maxTrustHistory', 'maxPerformanceHistory', 'learningRate', 'learningRateDecay',
                'swarmIntelligenceFactor', 'gradientResetFrequency', 'semanticLR', 'semanticBoost',
                'effectiveSemanticMax', 'longTermMaxProtos', 'shortTermMaxProtos', 'rawMaxProtos',
                'lowDim', 'numProjections', 'semanticMergeEvery',
                'maxRetrievedProtos', 'numRetrievalCandidates', 'maxEpisodicConsider',
                'replaySamples', 'coreMaxProtos', 'coreEpisodicMaxEntries',
                'lshNumTables', 'lshHashBits', 'numLshSets', 'priorityMax', 'trainingStepCount',
                'protoCapacityFactor', 'baseProtoCapacity', 'memoryFactor', 'maxVariancePerDim',
                'tempOverloadFactor', 'mergeTrimFactor', 'kernelGamma'
            ];

            scalarKeys.forEach(key => {
                const row = getMetadata.get(key);
                if (row) {
                    const value = Number(row.value);
                    if (isValidNumber(value)) {
                        switch (key) {
                            case 'ensembleSize':
                                this.#ensembleSize = value;
                                break;
                            case 'inputSize':
                                this.#inputSize = value;
                                break;
                            case 'numLayers':
                                this.#numLayers = value;
                                break;
                            case 'numHeads':
                                this.#numHeads = value;
                                break;
                            case 'headDim':
                                this.#headDim = value;
                                break;
                            case 'hiddenSize':
                                this.#hiddenSize = value;
                                break;
                            case 'feedForwardSize':
                                this.#feedForwardSize = value;
                                break;
                            case 'contextWindow':
                                this.#contextWindow = value;
                                break;
                            case 'adaptiveWindow':
                                this.#adaptiveWindow = value;
                                break;
                            case 'semanticMaxProtos':
                                this.#semanticMaxProtos = value;
                                break;
                            case 'maxTrustHistory':
                                this.#maxTrustHistory = value;
                                break;
                            case 'maxPerformanceHistory':
                                this.#maxPerformanceHistory = value;
                                break;
                            case 'learningRate':
                                this.#learningRate = value;
                                break;
                            case 'learningRateDecay':
                                this.#learningRateDecay = value;
                                break;
                            case 'swarmIntelligenceFactor':
                                this.#swarmIntelligenceFactor = value;
                                break;
                            case 'gradientResetFrequency':
                                this.#gradientResetFrequency = value;
                                break;
                            case 'semanticLR':
                                this.#semanticLR = value;
                                break;
                            case 'semanticBoost':
                                this.#semanticBoost = value;
                                break;
                            case 'effectiveSemanticMax':
                                this.#effectiveSemanticMax = value;
                                break;
                            case 'longTermMaxProtos':
                                this.#longTermMaxProtos = value;
                                break;
                            case 'shortTermMaxProtos':
                                this.#shortTermMaxProtos = value;
                                break;
                            case 'rawMaxProtos':
                                this.#rawMaxProtos = value;
                                break;
                            case 'lowDim':
                                this.#lowDim = value;
                                break;
                            case 'numProjections':
                                this.#numProjections = value;
                                break;
                            case 'semanticMergeEvery':
                                this.#semanticMergeEvery = value;
                                break;
                            case 'maxRetrievedProtos':
                                this.#maxRetrievedProtos = value;
                                break;
                            case 'numRetrievalCandidates':
                                this.#numRetrievalCandidates = value;
                                break;
                            case 'maxEpisodicConsider':
                                this.#maxEpisodicConsider = value;
                                break;
                            case 'replaySamples':
                                this.#replaySamples = value;
                                break;
                            case 'coreMaxProtos':
                                this.#coreMaxProtos = value;
                                break;
                            case 'coreEpisodicMaxEntries':
                                this.#coreEpisodicMaxEntries = value;
                                break;
                            case 'lshNumTables':
                                this.#lshNumTables = value;
                                break;
                            case 'lshHashBits':
                                this.#lshHashBits = value;
                                break;
                            case 'numLshSets':
                                this.#numLshSets = value;
                                break;
                            case 'priorityMax':
                                this.#priorityMax = value;
                                break;
                            case 'trainingStepCount':
                                this.#trainingStepCount = value;
                                break;
                            case 'protoCapacityFactor':
                                this.#protoCapacityFactor = value;
                                break;
                            case 'baseProtoCapacity':
                                this.#baseProtoCapacity = value;
                                break;
                            case 'memoryFactor':
                                this.#memoryFactor = value;
                                break;
                            case 'maxVariancePerDim':
                                this.#maxVariancePerDim = value;
                                break;
                            case 'tempOverloadFactor':
                                this.#tempOverloadFactor = value;
                                break;
                            case 'mergeTrimFactor':
                                this.#mergeTrimFactor = value;
                                break;
                            case 'kernelGamma':
                                this.#kernelGamma = value;
                                break;
                            default:
                                console.warn(`Unexpected metadata key loaded: ${key}`);
                        }
                    }
                }
            });

            const hidden = this.#hiddenSize;
            const low = this.#lowDim;
            const projCount = this.#numProjections;
            const es = this.#ensembleSize;
            const layers = this.#numLayers;
            const ff = this.#feedForwardSize;

            this.#projectionMatrices = Array.from({ length: projCount }, () => {
                const mat = new Array(hidden);
                for (let r = 0; r < hidden; r++) {
                    mat[r] = new Float32Array(low).fill(0);
                }
                return mat;
            });

            this.#lshHyperplanes = Array.from({ length: this.#numLshSets }, () =>
                Array.from({ length: this.#lshNumTables }, () =>
                    Array.from({ length: this.#lshHashBits }, () => new Float32Array(low).fill(0))
                )
            );

            this.#attentionMemory = Array(es).fill().map(() => []);
            this.#adaptiveContext = Array(es).fill().map(() => []);
            this.#semanticProtos = Array(es).fill().map(() => []);
            this.#coreEpisodic = Array(es).fill().map(() => []);
            this.#priorityIndices = Array(es).fill().map(() => []);
            this.#semanticLSHBuckets = Array(es).fill().map(() =>
                Array(this.#numLshSets).fill().map(() =>
                    Array(this.#lshNumTables).fill().map(() => new Map())
                )
            );

            this.#ensembleWeights = Array(es).fill(0);
            this.#performanceScores = Array(es).fill(0);
            this.#agreementScores = Array(es).fill(0);
            this.#specializationScores = Array(es).fill(0);
            this.#adaptiveLearningRate = Array(es).fill(0);
            this.#attentionWeightMatrix = Array(es).fill().map(() => Array(hidden).fill(0));
            this.#attentionBias = Array(es).fill().map(() => Array(hidden).fill(0));
            this.#specializationWeights = Array(es).fill().map(() =>
                Array(hidden).fill().map(() => Array(hidden).fill(0))
            );
            this.#historicalPerformance = Array(es).fill().map(() => []);
            this.#trustScoresHistory = Array(es).fill().map(() => []);

            const zeroMatrix = (rows, cols) => Array(rows).fill().map(() => Array(cols).fill(0));
            const zeroVector = length => Array(length).fill(0);

            this.#transformers = Array(es).fill().map(() => ({
                attentionWeights: Array(layers).fill().map(() => ({
                    Wq: zeroMatrix(hidden, hidden),
                    Wk: zeroMatrix(hidden, hidden),
                    Wv: zeroMatrix(hidden, hidden),
                    Wo: zeroMatrix(hidden, hidden)
                })),
                ffnWeights: Array(layers).fill().map(() => ({
                    gate_proj: zeroMatrix(hidden, ff),
                    up_proj: zeroMatrix(hidden, ff),
                    down_proj: zeroMatrix(ff, hidden)
                })),
                layerNormWeights: Array(layers).fill().map(() => ({
                    gamma1: zeroVector(hidden),
                    gamma2: zeroVector(hidden)
                })),
                outputWeights: zeroMatrix(hidden, 1),
                outputBias: [0]
            }));

            this.#gradientAccumulation = Array(es).fill().map(() => ({
                outputWeights: zeroMatrix(hidden, 1),
                outputBias: [0],
                attentionWeights: Array(layers).fill().map(() => ({
                    Wq: zeroMatrix(hidden, hidden),
                    Wk: zeroMatrix(hidden, hidden),
                    Wv: zeroMatrix(hidden, hidden),
                    Wo: zeroMatrix(hidden, hidden)
                })),
                ffnWeights: Array(layers).fill().map(() => ({
                    gate_proj: zeroMatrix(hidden, ff),
                    up_proj: zeroMatrix(hidden, ff),
                    down_proj: zeroMatrix(ff, hidden)
                })),
                layerNormWeights: Array(layers).fill().map(() => ({
                    gamma1: zeroVector(hidden),
                    gamma2: zeroVector(hidden)
                })),
                attentionBias: zeroVector(hidden),
                attentionWeightMatrix: zeroVector(hidden),
                specializationWeights: zeroMatrix(hidden, hidden)
            }));

            const loadVector = (table, target) => {
                const stmt = db.prepare(`SELECT idx, value FROM ${table} ORDER BY idx`);
                const rows = stmt.all();
                rows.forEach(({ idx, value }) => {
                    if (idx >= 0 && idx < es && isValidNumber(value)) {
                        target[idx] = value;
                    }
                });
            };

            loadVector('ensemble_weights', this.#ensembleWeights);
            loadVector('performance_scores', this.#performanceScores);
            loadVector('agreement_scores', this.#agreementScores);
            loadVector('specialization_scores', this.#specializationScores);
            loadVector('adaptive_learning_rate', this.#adaptiveLearningRate);

            const load1DPerIdx = (table, target) => {
                const stmt = db.prepare(`SELECT idx, row, value FROM ${table}`);
                const rows = stmt.all();
                rows.forEach(({ idx, row, value }) => {
                    if (idx >= 0 && idx < es && row >= 0 && row < hidden && isValidNumber(value)) {
                        target[idx][row] = value;
                    }
                });
            };

            load1DPerIdx('attention_weight_matrix', this.#attentionWeightMatrix);
            load1DPerIdx('attention_bias', this.#attentionBias);

            const load3D = (table, target) => {
                const stmt = db.prepare(`SELECT idx, row, col, value FROM ${table}`);
                const rows = stmt.all();
                rows.forEach(({ idx, row, col, value }) => {
                    if (idx >= 0 && idx < es && row >= 0 && row < hidden && col >= 0 && col < hidden && isValidNumber(value)) {
                        target[idx][row][col] = value;
                    }
                });
            };

            load3D('specialization_weights', this.#specializationWeights);

            const loadHistory = (table, target) => {
                const stmt = db.prepare(`SELECT idx, step, score FROM ${table} ORDER BY idx, step`);
                const rows = stmt.all();
                rows.forEach(({ idx, step, score }) => {
                    if (idx >= 0 && idx < es && Number.isInteger(step) && step >= 0 && isValidNumber(score)) {
                        while (target[idx].length <= step) target[idx].push(0);
                        target[idx][step] = score;
                    }
                });
            };

            loadHistory('historical_performance', this.#historicalPerformance);
            loadHistory('trust_scores_history', this.#trustScoresHistory);

            const projStmt = db.prepare('SELECT proj_idx, row, col, value FROM projection_matrices');
            projStmt.all().forEach(({ proj_idx, row, col, value }) => {
                if (proj_idx >= 0 && proj_idx < projCount && row >= 0 && row < hidden && col >= 0 && col < low && isValidNumber(value)) {
                    this.#projectionMatrices[proj_idx][row][col] = value;
                }
            });

            const lshStmt = db.prepare('SELECT set_idx, table_idx, bit_idx, dim, value FROM lsh_hyperplanes');
            lshStmt.all().forEach(({ set_idx, table_idx, bit_idx, dim, value }) => {
                if (set_idx >= 0 && set_idx < this.#numLshSets &&
                    table_idx >= 0 && table_idx < this.#lshNumTables &&
                    bit_idx >= 0 && bit_idx < this.#lshHashBits &&
                    dim >= 0 && dim < low && isValidNumber(value)) {
                    this.#lshHyperplanes[set_idx][table_idx][bit_idx][dim] = value;
                }
            });

            const loadPrototypeMemory = (type) => {
                const isAttention = type === 'attention';
                const isAdaptive = type === 'adaptive';
                const isSemantic = type === 'semantic';
                const isCore = type === 'core';

                const hasWindow = isAttention || isAdaptive;
                const hasEntry = isCore;
                const hasRep = !isSemantic;

                const prefix = isAttention ? 'attention_memory' :
                               isAdaptive ? 'adaptive_context' :
                               isSemantic ? 'semantic' :
                               'core_episodic';

                const protoStmt = db.prepare(`
                    SELECT idx ${hasWindow ? ', window' : ''} ${hasEntry ? ', entry_idx' : ''}, proto_idx, proto_size, access_count, is_core, importance
                    FROM ${prefix}_protos
                    ORDER BY idx ${hasWindow ? ', window' : ''} ${hasEntry ? ', entry_idx' : ''}, proto_idx
                `);
                const meanStmt = db.prepare(`SELECT idx ${hasWindow ? ', window' : ''} ${hasEntry ? ', entry_idx' : ''}, proto_idx, dim, value FROM ${prefix}_means`);
                const varStmt = db.prepare(`SELECT idx ${hasWindow ? ', window' : ''} ${hasEntry ? ', entry_idx' : ''}, proto_idx, dim, value FROM ${prefix}_variances`);

                const protoMeta = protoStmt.all();
                const means = meanStmt.all();
                const variances = varStmt.all();

                const groups = {};
                protoMeta.forEach(meta => {
                    const pos = hasWindow ? meta.window : (hasEntry ? meta.entry_idx : 0);
                    const key = `${meta.idx}_${pos}`;
                    if (!groups[key]) groups[key] = { meta: [], idx: meta.idx, pos };
                    groups[key].meta.push(meta);
                });

                Object.values(groups).forEach(group => {
                    const { meta, idx, pos } = group;

                    const filteredMeans = means.filter(r => {
                        const rpos = hasWindow ? r.window : (hasEntry ? r.entry_idx : 0);
                        return `${r.idx}_${rpos}` === `${idx}_${pos}`;
                    });
                    const filteredVars = variances.filter(r => {
                        const rpos = hasWindow ? r.window : (hasEntry ? r.entry_idx : 0);
                        return `${r.idx}_${rpos}` === `${idx}_${pos}`;
                    });

                    const meanMap = {};
                    filteredMeans.forEach(r => {
                        if (!meanMap[r.proto_idx]) meanMap[r.proto_idx] = new Float32Array(hidden).fill(0);
                        meanMap[r.proto_idx][r.dim] = r.value;
                    });
                    const varMap = {};
                    filteredVars.forEach(r => {
                        if (!varMap[r.proto_idx]) varMap[r.proto_idx] = new Float32Array(hidden).fill(1e-6);
                        varMap[r.proto_idx][r.dim] = r.value;
                    });

                    const protoList = [];
                    meta.forEach(m => {
                        const mean = meanMap[m.proto_idx] || new Float32Array(hidden).fill(0);
                        const variance = varMap[m.proto_idx] || new Float32Array(hidden).fill(1e-6);
                        protoList[m.proto_idx] = {
                            mean,
                            variance,
                            size: m.proto_size || 0,
                            accessCount: m.access_count || 0,
                            isCore: !!m.is_core,
                            importance: m.importance || 0,
                            projNorms: Array(projCount).fill().map(() => new Float32Array(low).fill(0))
                        };
                    });

                    const denseProtos = protoList.filter(p => p !== undefined);

                    const entry = { protos: denseProtos };
                    if (hasRep) {
                        entry.repMean = new Float32Array(hidden).fill(0);
                        entry.repProj = Array(projCount).fill().map(() => new Float32Array(low).fill(0));
                    }

                    let memoryArray;
                    if (isAttention) memoryArray = this.#attentionMemory[idx];
                    else if (isAdaptive) memoryArray = this.#adaptiveContext[idx];
                    else if (isSemantic) memoryArray = this.#semanticProtos[idx];
                    else if (isCore) memoryArray = this.#coreEpisodic[idx];

                    if (isSemantic) {
                        this.#semanticProtos[idx] = denseProtos;
                    } else {
                        while (memoryArray.length <= pos) memoryArray.push(null);
                        memoryArray[pos] = entry;
                    }
                });

                if (isAttention || isAdaptive) {
                    for (let i = 0; i < es; i++) {
                        const mem = isAttention ? this.#attentionMemory[i] : this.#adaptiveContext[i];
                        while (mem.length > 0 && (!mem[mem.length - 1] || mem[mem.length - 1].protos.length === 0)) {
                            mem.pop();
                        }
                    }
                }
            };

            loadPrototypeMemory('attention');
            loadPrototypeMemory('adaptive');
            loadPrototypeMemory('semantic');
            loadPrototypeMemory('core');

            for (let idx = 0; idx < es; idx++) {
                [this.#attentionMemory[idx], this.#adaptiveContext[idx], this.#coreEpisodic[idx]].forEach(mem => {
                    mem.forEach(entry => {
                        if (entry && entry.protos.length > 0) {
                            entry.protos.forEach(proto => {
                                proto.projNorms = this.#computeProjNorms(proto.mean);
                            });
                            entry.repMean = this.#weightedMean(entry.protos);
                            entry.repProj = this.#computeProjNorms(entry.repMean);
                        }
                    });
                });

                const semProtos = this.#semanticProtos[idx];
                semProtos.forEach(proto => {
                    proto.projNorms = this.#computeProjNorms(proto.mean);
                });

                this.#semanticLSHBuckets[idx] = Array(this.#numLshSets).fill().map(() =>
                    Array(this.#lshNumTables).fill().map(() => new Map())
                );
                semProtos.forEach(proto => {
                    this.#insertProtoToLSH(idx, proto);
                });

                const indexed = semProtos.map((proto, i) => ({ i, util: this.#computeProtoUtility(proto) }));
                indexed.sort((a, b) => b.util - a.util);
                this.#priorityIndices[idx] = indexed.slice(0, this.#priorityMax).map(o => o.i);
            }

            const transWeightStmt = db.prepare('SELECT idx, layer, weight_type, row, col, value FROM transformers');
            transWeightStmt.all().forEach(({ idx, layer, weight_type, row, col, value }) => {
                if (idx < 0 || idx >= es || !isValidNumber(value)) return;
                const trans = this.#transformers[idx];
                if (layer === -1 && weight_type === 'outputWeights') {
                    if (row < hidden && col === 0) trans.outputWeights[row][0] = value;
                } else if (layer >= 0 && layer < layers) {
                    const att = trans.attentionWeights[layer];
                    const ffn = trans.ffnWeights[layer];
                    if (['Wq', 'Wk', 'Wv', 'Wo'].includes(weight_type)) {
                        att[weight_type][row][col] = value;
                    } else if (['gate_proj', 'up_proj'].includes(weight_type)) {
                        ffn[weight_type][row][col] = value;
                    } else if (weight_type === 'down_proj') {
                        ffn.down_proj[row][col] = value;
                    }
                }
            });

            const transBiasStmt = db.prepare('SELECT idx, layer, bias_type, row, value FROM transformer_biases');
            transBiasStmt.all().forEach(({ idx, layer, bias_type, row, value }) => {
                if (idx < 0 || idx >= es || !isValidNumber(value)) return;
                if (bias_type === 'outputBias' && layer === -1 && row === 0) {
                    this.#transformers[idx].outputBias[0] = value;
                }
            });

            const transNormStmt = db.prepare('SELECT idx, layer, norm_type, row, value FROM transformer_layer_norm');
            transNormStmt.all().forEach(({ idx, layer, norm_type, row, value }) => {
                if (idx < 0 || idx >= es || layer < 0 || layer >= layers || row < 0 || row >= hidden || !isValidNumber(value)) return;
                this.#transformers[idx].layerNormWeights[layer][norm_type][row] = value;
            });

            const gradStmt = db.prepare('SELECT idx, layer, weight_type, row, col, value FROM gradient_accumulation');
            gradStmt.all().forEach(({ idx, layer, weight_type, row, col, value }) => {
                if (idx < 0 || idx >= es || !isValidNumber(value)) return;
                const grad = this.#gradientAccumulation[idx];
                if (layer === -1) {
                    if (weight_type === 'outputWeights') grad.outputWeights[row][col] = value;
                    else if (weight_type === 'outputBias') grad.outputBias[row] = value;
                    else if (weight_type === 'attentionBias') grad.attentionBias[row] = value;
                    else if (weight_type === 'attentionWeightMatrix') grad.attentionWeightMatrix[row] = value;
                    else if (weight_type === 'specializationWeights') grad.specializationWeights[row][col] = value;
                } else if (layer >= 0 && layer < layers) {
                    if (['Wq', 'Wk', 'Wv', 'Wo'].includes(weight_type)) {
                        grad.attentionWeights[layer][weight_type][row][col] = value;
                    } else if (['gate_proj', 'up_proj'].includes(weight_type)) {
                        grad.ffnWeights[layer][weight_type][row][col] = value;
                    } else if (weight_type === 'down_proj') {
                        grad.ffnWeights[layer].down_proj[row][col] = value;
                    } else if (['gamma1', 'gamma2'].includes(weight_type)) {
                        grad.layerNormWeights[layer][weight_type][row] = value;
                    }
                }
            });

            this.#normalizeEnsembleWeights();

            return { status: true, message : 'State loaded successfully!' };
        } catch (error) {
            console.log(error.message, error.stack)
            return { status: false, error: error.message, trace: error.stack };
        } finally {
            if (db) db.close();
        }
    }

    #saveState() {
        const dbPath = path.join(this.#directoryPath, this.#fileName);
        let db;

        try {
            db = new Database(dbPath);
            db.pragma('journal_mode = WAL');
            db.pragma('synchronous = NORMAL');

            db.exec('BEGIN TRANSACTION');

            db.exec(`
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                CREATE TABLE IF NOT EXISTS ensemble_weights (
                    idx INTEGER PRIMARY KEY,
                    value REAL
                );
                CREATE TABLE IF NOT EXISTS performance_scores (
                    idx INTEGER PRIMARY KEY,
                    value REAL
                );
                CREATE TABLE IF NOT EXISTS agreement_scores (
                    idx INTEGER PRIMARY KEY,
                    value REAL
                );
                CREATE TABLE IF NOT EXISTS specialization_scores (
                    idx INTEGER PRIMARY KEY,
                    value REAL
                );
                CREATE TABLE IF NOT EXISTS historical_performance (
                    idx INTEGER,
                    step INTEGER,
                    score REAL,
                    PRIMARY KEY (idx, step)
                );
                CREATE TABLE IF NOT EXISTS trust_scores_history (
                    idx INTEGER,
                    step INTEGER,
                    score REAL,
                    PRIMARY KEY (idx, step)
                );
                CREATE TABLE IF NOT EXISTS adaptive_learning_rate (
                    idx INTEGER PRIMARY KEY,
                    value REAL
                );
                CREATE TABLE IF NOT EXISTS attention_weight_matrix (
                    idx INTEGER,
                    row INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, row)
                );
                CREATE TABLE IF NOT EXISTS attention_bias (
                    idx INTEGER,
                    row INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, row)
                );
                CREATE TABLE IF NOT EXISTS specialization_weights (
                    idx INTEGER,
                    row INTEGER,
                    col INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, row, col)
                );

                CREATE TABLE IF NOT EXISTS attention_memory_protos (
                    idx INTEGER,
                    window INTEGER,
                    proto_idx INTEGER,
                    proto_size REAL,
                    access_count REAL,
                    is_core INTEGER DEFAULT 0,
                    importance REAL DEFAULT 0,
                    PRIMARY KEY (idx, window, proto_idx)
                );
                CREATE TABLE IF NOT EXISTS attention_memory_means (
                    idx INTEGER,
                    window INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, window, proto_idx, dim)
                );
                CREATE TABLE IF NOT EXISTS attention_memory_variances (
                    idx INTEGER,
                    window INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, window, proto_idx, dim)
                );

                CREATE TABLE IF NOT EXISTS adaptive_context_protos (
                    idx INTEGER,
                    window INTEGER,
                    proto_idx INTEGER,
                    proto_size REAL,
                    access_count REAL,
                    is_core INTEGER DEFAULT 0,
                    importance REAL DEFAULT 0,
                    PRIMARY KEY (idx, window, proto_idx)
                );
                CREATE TABLE IF NOT EXISTS adaptive_context_means (
                    idx INTEGER,
                    window INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, window, proto_idx, dim)
                );
                CREATE TABLE IF NOT EXISTS adaptive_context_variances (
                    idx INTEGER,
                    window INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, window, proto_idx, dim)
                );

                CREATE TABLE IF NOT EXISTS semantic_protos (
                    idx INTEGER,
                    proto_idx INTEGER,
                    proto_size REAL,
                    access_count REAL,
                    is_core INTEGER DEFAULT 0,
                    importance REAL DEFAULT 0,
                    PRIMARY KEY (idx, proto_idx)
                );
                CREATE TABLE IF NOT EXISTS semantic_means (
                    idx INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, proto_idx, dim)
                );
                CREATE TABLE IF NOT EXISTS semantic_variances (
                    idx INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, proto_idx, dim)
                );

                CREATE TABLE IF NOT EXISTS core_episodic_protos (
                    idx INTEGER,
                    entry_idx INTEGER,
                    proto_idx INTEGER,
                    proto_size REAL,
                    access_count REAL,
                    is_core INTEGER DEFAULT 0,
                    importance REAL DEFAULT 0,
                    PRIMARY KEY (idx, entry_idx, proto_idx)
                );
                CREATE TABLE IF NOT EXISTS core_episodic_means (
                    idx INTEGER,
                    entry_idx INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, entry_idx, proto_idx, dim)
                );
                CREATE TABLE IF NOT EXISTS core_episodic_variances (
                    idx INTEGER,
                    entry_idx INTEGER,
                    proto_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, entry_idx, proto_idx, dim)
                );

                CREATE TABLE IF NOT EXISTS projection_matrices (
                    proj_idx INTEGER,
                    row INTEGER,
                    col INTEGER,
                    value REAL,
                    PRIMARY KEY (proj_idx, row, col)
                );

                CREATE TABLE IF NOT EXISTS lsh_hyperplanes (
                    set_idx INTEGER,
                    table_idx INTEGER,
                    bit_idx INTEGER,
                    dim INTEGER,
                    value REAL,
                    PRIMARY KEY (set_idx, table_idx, bit_idx, dim)
                );

                CREATE TABLE IF NOT EXISTS transformers (
                    idx INTEGER,
                    layer INTEGER,
                    weight_type TEXT,
                    row INTEGER,
                    col INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, layer, weight_type, row, col)
                );
                CREATE TABLE IF NOT EXISTS transformer_biases (
                    idx INTEGER,
                    layer INTEGER,
                    bias_type TEXT,
                    row INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, layer, bias_type, row)
                );
                CREATE TABLE IF NOT EXISTS transformer_layer_norm (
                    idx INTEGER,
                    layer INTEGER,
                    norm_type TEXT,
                    row INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, layer, norm_type, row)
                );
                CREATE TABLE IF NOT EXISTS gradient_accumulation (
                    idx INTEGER,
                    layer INTEGER,
                    weight_type TEXT,
                    row INTEGER,
                    col INTEGER,
                    value REAL,
                    PRIMARY KEY (idx, layer, weight_type, row, col)
                );
            `);

            const tablesToClear = [
                'metadata', 'ensemble_weights', 'performance_scores', 'agreement_scores', 'specialization_scores',
                'historical_performance', 'trust_scores_history', 'adaptive_learning_rate',
                'attention_weight_matrix', 'attention_bias', 'specialization_weights',
                'attention_memory_protos', 'attention_memory_means', 'attention_memory_variances',
                'adaptive_context_protos', 'adaptive_context_means', 'adaptive_context_variances',
                'semantic_protos', 'semantic_means', 'semantic_variances',
                'core_episodic_protos', 'core_episodic_means', 'core_episodic_variances',
                'projection_matrices', 'lsh_hyperplanes',
                'transformers', 'transformer_biases', 'transformer_layer_norm', 'gradient_accumulation'
            ];

            tablesToClear.forEach(table => db.exec(`DELETE FROM ${table};`));

            const insertMetadata = db.prepare('INSERT INTO metadata (key, value) VALUES (?, ?)');

            const scalarFields = {
                ensembleSize: this.#ensembleSize,
                inputSize: this.#inputSize,
                numLayers: this.#numLayers,
                numHeads: this.#numHeads,
                headDim: this.#headDim,
                hiddenSize: this.#hiddenSize,
                feedForwardSize: this.#feedForwardSize,
                contextWindow: this.#contextWindow,
                adaptiveWindow: this.#adaptiveWindow,
                semanticMaxProtos: this.#semanticMaxProtos,
                maxTrustHistory: this.#maxTrustHistory,
                maxPerformanceHistory: this.#maxPerformanceHistory,
                learningRate: this.#learningRate,
                learningRateDecay: this.#learningRateDecay,
                swarmIntelligenceFactor: this.#swarmIntelligenceFactor,
                gradientResetFrequency: this.#gradientResetFrequency,
                semanticLR: this.#semanticLR,
                semanticBoost: this.#semanticBoost,
                effectiveSemanticMax: this.#effectiveSemanticMax,
                longTermMaxProtos: this.#longTermMaxProtos,
                shortTermMaxProtos: this.#shortTermMaxProtos,
                rawMaxProtos: this.#rawMaxProtos,
                lowDim: this.#lowDim,
                numProjections: this.#numProjections,
                semanticMergeEvery: this.#semanticMergeEvery,
                maxRetrievedProtos: this.#maxRetrievedProtos,
                numRetrievalCandidates: this.#numRetrievalCandidates,
                maxEpisodicConsider: this.#maxEpisodicConsider,
                replaySamples: this.#replaySamples,
                coreMaxProtos: this.#coreMaxProtos,
                coreEpisodicMaxEntries: this.#coreEpisodicMaxEntries,
                lshNumTables: this.#lshNumTables,
                lshHashBits: this.#lshHashBits,
                numLshSets: this.#numLshSets,
                priorityMax: this.#priorityMax,
                protoCapacityFactor: this.#protoCapacityFactor,
                baseProtoCapacity: this.#baseProtoCapacity,
                memoryFactor: this.#memoryFactor,
                maxVariancePerDim: this.#maxVariancePerDim,
                tempOverloadFactor: this.#tempOverloadFactor,
                mergeTrimFactor: this.#mergeTrimFactor,
                kernelGamma: this.#kernelGamma,
                trainingStepCount: this.#trainingStepCount ?? 0
            };

            for (const [key, value] of Object.entries(scalarFields)) {
                insertMetadata.run(key, value.toString());
            }

            const batchInsert = (table, columns, data, batchSize = 2000) => {
                if (data.length === 0) return;
                const rowPlaceholder = `(${columns.map(() => '?').join(',')})`;
                let offset = 0;
                while (offset < data.length) {
                    const chunkSize = Math.min(batchSize, data.length - offset);
                    const placeholders = Array(chunkSize).fill(rowPlaceholder).join(',');
                    const sql = `INSERT INTO ${table} (${columns.join(',')}) VALUES ${placeholders}`;
                    const flatParams = data.slice(offset, offset + chunkSize).flat();
                    const stmt = db.prepare(sql);
                    stmt.run(...flatParams);
                    offset += chunkSize;
                }
            };

            const ensembleSize = this.#ensembleSize;

            let rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const value = this.#ensembleWeights[idx];
                if (isValidNumber(value)) rows.push([idx, value]);
            }
            batchInsert('ensemble_weights', ['idx', 'value'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const value = this.#performanceScores[idx];
                if (isValidNumber(value)) rows.push([idx, value]);
            }
            batchInsert('performance_scores', ['idx', 'value'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const value = this.#agreementScores[idx];
                if (isValidNumber(value)) rows.push([idx, value]);
            }
            batchInsert('agreement_scores', ['idx', 'value'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const value = this.#specializationScores[idx];
                if (isValidNumber(value)) rows.push([idx, value]);
            }
            batchInsert('specialization_scores', ['idx', 'value'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const value = this.#adaptiveLearningRate[idx];
                if (isValidNumber(value)) rows.push([idx, value]);
            }
            batchInsert('adaptive_learning_rate', ['idx', 'value'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const history = this.#historicalPerformance[idx];
                for (let step = 0; step < history.length; step++) {
                    const score = history[step];
                    if (isValidNumber(score)) rows.push([idx, step, score]);
                }
            }
            batchInsert('historical_performance', ['idx', 'step', 'score'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const history = this.#trustScoresHistory[idx];
                for (let step = 0; step < history.length; step++) {
                    const score = history[step];
                    if (isValidNumber(score)) rows.push([idx, step, score]);
                }
            }
            batchInsert('trust_scores_history', ['idx', 'step', 'score'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const vec = this.#attentionWeightMatrix[idx];
                for (let row = 0; row < vec.length; row++) {
                    const value = vec[row];
                    if (isValidNumber(value)) rows.push([idx, row, value]);
                }
            }
            batchInsert('attention_weight_matrix', ['idx', 'row', 'value'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const vec = this.#attentionBias[idx];
                for (let row = 0; row < vec.length; row++) {
                    const value = vec[row];
                    if (isValidNumber(value)) rows.push([idx, row, value]);
                }
            }
            batchInsert('attention_bias', ['idx', 'row', 'value'], rows);

            rows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const matrix = this.#specializationWeights[idx];
                for (let r = 0; r < matrix.length; r++) {
                    for (let c = 0; c < matrix[r].length; c++) {
                        const value = matrix[r][c];
                        if (isValidNumber(value)) rows.push([idx, r, c, value]);
                    }
                }
            }
            batchInsert('specialization_weights', ['idx', 'row', 'col', 'value'], rows, 5000);

            rows = [];
            for (let proj_idx = 0; proj_idx < this.#numProjections; proj_idx++) {
                const matrix = this.#projectionMatrices[proj_idx];
                for (let r = 0; r < matrix.length; r++) {
                    for (let c = 0; c < matrix[r].length; c++) {
                        const value = matrix[r][c];
                        if (isValidNumber(value)) rows.push([proj_idx, r, c, value]);
                    }
                }
            }
            batchInsert('projection_matrices', ['proj_idx', 'row', 'col', 'value'], rows);

            rows = [];
            for (let set_idx = 0; set_idx < this.#numLshSets; set_idx++) {
                const setArr = this.#lshHyperplanes[set_idx];
                for (let table_idx = 0; table_idx < setArr.length; table_idx++) {
                    const tableArr = setArr[table_idx];
                    for (let bit_idx = 0; bit_idx < tableArr.length; bit_idx++) {
                        const vec = tableArr[bit_idx];
                        for (let dim = 0; dim < vec.length; dim++) {
                            const value = vec[dim];
                            if (isValidNumber(value)) rows.push([set_idx, table_idx, bit_idx, dim, value]);
                        }
                    }
                }
            }
            batchInsert('lsh_hyperplanes', ['set_idx', 'table_idx', 'bit_idx', 'dim', 'value'], rows);

            let transWeightRows = [];
            let transBiasRows = [];
            let transNormRows = [];

            for (let idx = 0; idx < ensembleSize; idx++) {
                const t = this.#transformers[idx];
                const numLayers = t.attentionWeights.length;

                for (let layer = 0; layer < numLayers; layer++) {
                    const aw = t.attentionWeights[layer];
                    for (const type of ['Wq', 'Wk', 'Wv', 'Wo']) {
                        const m = aw[type];
                        for (let r = 0; r < m.length; r++) {
                            for (let c = 0; c < m[r].length; c++) {
                                const v = m[r][c];
                                if (isValidNumber(v)) transWeightRows.push([idx, layer, type, r, c, v]);
                            }
                        }
                    }
                }

                for (let layer = 0; layer < numLayers; layer++) {
                    const fw = t.ffnWeights[layer];
                    for (const type of ['gate_proj', 'up_proj', 'down_proj']) {
                        const m = fw[type];
                        for (let r = 0; r < m.length; r++) {
                            for (let c = 0; c < m[r].length; c++) {
                                const v = m[r][c];
                                if (isValidNumber(v)) transWeightRows.push([idx, layer, type, r, c, v]);
                            }
                        }
                    }
                }

                for (let layer = 0; layer < numLayers; layer++) {
                    const ln = t.layerNormWeights[layer];
                    for (const type of ['gamma1', 'gamma2']) {
                        const vec = ln[type];
                        for (let r = 0; r < vec.length; r++) {
                            const v = vec[r];
                            if (isValidNumber(v)) transNormRows.push([idx, layer, type, r, v]);
                        }
                    }
                }

                const ow = t.outputWeights;
                for (let r = 0; r < ow.length; r++) {
                    for (let c = 0; c < ow[r].length; c++) {
                        const v = ow[r][c];
                        if (isValidNumber(v)) transWeightRows.push([idx, -1, 'outputWeights', r, c, v]);
                    }
                }

                const ob = t.outputBias;
                for (let r = 0; r < ob.length; r++) {
                    const v = ob[r];
                    if (isValidNumber(v)) transBiasRows.push([idx, -1, 'outputBias', r, v]);
                }
            }

            batchInsert('transformers', ['idx', 'layer', 'weight_type', 'row', 'col', 'value'], transWeightRows, 5000);
            batchInsert('transformer_biases', ['idx', 'layer', 'bias_type', 'row', 'value'], transBiasRows);
            batchInsert('transformer_layer_norm', ['idx', 'layer', 'norm_type', 'row', 'value'], transNormRows);

            let gradRows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const g = this.#gradientAccumulation[idx];

                for (let r = 0; r < g.outputWeights.length; r++) {
                    for (let c = 0; c < g.outputWeights[r].length; c++) {
                        const v = g.outputWeights[r][c];
                        if (isValidNumber(v)) gradRows.push([idx, -1, 'outputWeights', r, c, v]);
                    }
                }
                for (let r = 0; r < g.outputBias.length; r++) {
                    const v = g.outputBias[r];
                    if (isValidNumber(v)) gradRows.push([idx, -1, 'outputBias', r, 0, v]);
                }

                const numLayers = g.attentionWeights.length;

                for (let layer = 0; layer < numLayers; layer++) {
                    const ag = g.attentionWeights[layer];
                    for (const type of ['Wq', 'Wk', 'Wv', 'Wo']) {
                        const m = ag[type];
                        for (let r = 0; r < m.length; r++) {
                            for (let c = 0; c < m[r].length; c++) {
                                const v = m[r][c];
                                if (isValidNumber(v)) gradRows.push([idx, layer, type, r, c, v]);
                            }
                        }
                    }
                }

                for (let layer = 0; layer < numLayers; layer++) {
                    const fg = g.ffnWeights[layer];
                    for (const type of ['gate_proj', 'up_proj', 'down_proj']) {
                        const m = fg[type];
                        for (let r = 0; r < m.length; r++) {
                            for (let c = 0; c < m[r].length; c++) {
                                const v = m[r][c];
                                if (isValidNumber(v)) gradRows.push([idx, layer, type, r, c, v]);
                            }
                        }
                    }
                }

                for (let layer = 0; layer < numLayers; layer++) {
                    const lg = g.layerNormWeights[layer];
                    for (const type of ['gamma1', 'gamma2']) {
                        const vec = lg[type];
                        for (let r = 0; r < vec.length; r++) {
                            const v = vec[r];
                            if (isValidNumber(v)) gradRows.push([idx, layer, type, r, 0, v]);
                        }
                    }
                }

                for (let r = 0; r < g.attentionBias.length; r++) {
                    const v = g.attentionBias[r];
                    if (isValidNumber(v)) gradRows.push([idx, -1, 'attentionBias', r, 0, v]);
                }
                for (let r = 0; r < g.attentionWeightMatrix.length; r++) {
                    const v = g.attentionWeightMatrix[r];
                    if (isValidNumber(v)) gradRows.push([idx, -1, 'attentionWeightMatrix', r, 0, v]);
                }
                for (let r = 0; r < g.specializationWeights.length; r++) {
                    for (let c = 0; c < g.specializationWeights[r].length; c++) {
                        const v = g.specializationWeights[r][c];
                        if (isValidNumber(v)) gradRows.push([idx, -1, 'specializationWeights', r, c, v]);
                    }
                }
            }
            batchInsert('gradient_accumulation', ['idx', 'layer', 'weight_type', 'row', 'col', 'value'], gradRows, 5000);

            let attProtoRows = [], attMeanRows = [], attVarRows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const windows = this.#attentionMemory[idx];
                for (let window = 0; window < windows.length; window++) {
                    const entry = windows[window];
                    if (!entry) continue;

                    for (let pidx = 0; pidx < entry.protos.length; pidx++) {
                        const proto = entry.protos[pidx];
                        attProtoRows.push([idx, window, pidx, proto.size ?? 0, proto.accessCount ?? 0, proto.isCore ? 1 : 0, proto.importance ?? 0]);

                        for (let dim = 0; dim < proto.mean.length; dim++) {
                            const v = proto.mean[dim];
                            if (isValidNumber(v)) attMeanRows.push([idx, window, pidx, dim, v]);
                        }
                        for (let dim = 0; dim < proto.variance.length; dim++) {
                            const v = proto.variance[dim];
                            if (isValidNumber(v)) attVarRows.push([idx, window, pidx, dim, v]);
                        }
                    }
                }
            }
            batchInsert('attention_memory_protos', ['idx', 'window', 'proto_idx', 'proto_size', 'access_count', 'is_core', 'importance'], attProtoRows);
            batchInsert('attention_memory_means', ['idx', 'window', 'proto_idx', 'dim', 'value'], attMeanRows);
            batchInsert('attention_memory_variances', ['idx', 'window', 'proto_idx', 'dim', 'value'], attVarRows);

            let adaptProtoRows = [], adaptMeanRows = [], adaptVarRows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const windows = this.#adaptiveContext[idx];
                for (let window = 0; window < windows.length; window++) {
                    const entry = windows[window];
                    if (!entry) continue;

                    for (let pidx = 0; pidx < entry.protos.length; pidx++) {
                        const proto = entry.protos[pidx];
                        adaptProtoRows.push([idx, window, pidx, proto.size ?? 0, proto.accessCount ?? 0, proto.isCore ? 1 : 0, proto.importance ?? 0]);

                        for (let dim = 0; dim < proto.mean.length; dim++) {
                            const v = proto.mean[dim];
                            if (isValidNumber(v)) adaptMeanRows.push([idx, window, pidx, dim, v]);
                        }
                        for (let dim = 0; dim < proto.variance.length; dim++) {
                            const v = proto.variance[dim];
                            if (isValidNumber(v)) adaptVarRows.push([idx, window, pidx, dim, v]);
                        }
                    }
                }
            }
            batchInsert('adaptive_context_protos', ['idx', 'window', 'proto_idx', 'proto_size', 'access_count', 'is_core', 'importance'], adaptProtoRows);
            batchInsert('adaptive_context_means', ['idx', 'window', 'proto_idx', 'dim', 'value'], adaptMeanRows);
            batchInsert('adaptive_context_variances', ['idx', 'window', 'proto_idx', 'dim', 'value'], adaptVarRows);

            let semProtoRows = [], semMeanRows = [], semVarRows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const protos = this.#semanticProtos[idx];
                for (let pidx = 0; pidx < protos.length; pidx++) {
                    const proto = protos[pidx];
                    semProtoRows.push([idx, pidx, proto.size ?? 0, proto.accessCount ?? 0, proto.isCore ? 1 : 0, proto.importance ?? 0]);

                    for (let dim = 0; dim < proto.mean.length; dim++) {
                        const v = proto.mean[dim];
                        if (isValidNumber(v)) semMeanRows.push([idx, pidx, dim, v]);
                    }
                    for (let dim = 0; dim < proto.variance.length; dim++) {
                        const v = proto.variance[dim];
                        if (isValidNumber(v)) semVarRows.push([idx, pidx, dim, v]);
                    }
                }
            }
            batchInsert('semantic_protos', ['idx', 'proto_idx', 'proto_size', 'access_count', 'is_core', 'importance'], semProtoRows);
            batchInsert('semantic_means', ['idx', 'proto_idx', 'dim', 'value'], semMeanRows);
            batchInsert('semantic_variances', ['idx', 'proto_idx', 'dim', 'value'], semVarRows);

            let coreProtoRows = [], coreMeanRows = [], coreVarRows = [];
            for (let idx = 0; idx < ensembleSize; idx++) {
                const entries = this.#coreEpisodic[idx];
                for (let eidx = 0; eidx < entries.length; eidx++) {
                    const entry = entries[eidx];
                    if (!entry) continue;

                    for (let pidx = 0; pidx < entry.protos.length; pidx++) {
                        const proto = entry.protos[pidx];
                        coreProtoRows.push([idx, eidx, pidx, proto.size ?? 0, proto.accessCount ?? 0, proto.isCore ? 1 : 0, proto.importance ?? 0]);

                        for (let dim = 0; dim < proto.mean.length; dim++) {
                            const v = proto.mean[dim];
                            if (isValidNumber(v)) coreMeanRows.push([idx, eidx, pidx, dim, v]);
                        }
                        for (let dim = 0; dim < proto.variance.length; dim++) {
                            const v = proto.variance[dim];
                            if (isValidNumber(v)) coreVarRows.push([idx, eidx, pidx, dim, v]);
                        }
                    }
                }
            }
            batchInsert('core_episodic_protos', ['idx', 'entry_idx', 'proto_idx', 'proto_size', 'access_count', 'is_core', 'importance'], coreProtoRows);
            batchInsert('core_episodic_means', ['idx', 'entry_idx', 'proto_idx', 'dim', 'value'], coreMeanRows);
            batchInsert('core_episodic_variances', ['idx', 'entry_idx', 'proto_idx', 'dim', 'value'], coreVarRows);

            db.exec('COMMIT');

            return { status: true, message: 'State saved successfully!' };
        } catch (error) {
            if (db) db.exec('ROLLBACK');
            console.log(error.message, error.stack);
            return { status: false, error: error.message, trace: error.stack };
        } finally {
            if (db) db.close();
        }
    }

    #scaleAndSetDimensions (es, is, forceMin) {
        this.#ensembleSize = es;
        this.#inputSize = is;

        const logEs = Math.log10(this.#ensembleSize);
        const normalized = Math.min(1.0, logEs / 3.0);
        const sqrtEs = Math.sqrt(this.#ensembleSize);
        const log2SqrtEs = Math.log2(sqrtEs);

        const numLayersRaw = 6.0 - 4.0 * normalized;
        this.#numLayers = forceMin ? 2 : Math.max(2, Math.round(numLayersRaw));

        const numHeadsRaw = 8.0 - 6.0 * normalized;
        this.#numHeads = forceMin ? 2 : Math.max(2, Math.round(numHeadsRaw));

        const hsExponent = Math.max(2, Math.min(4, 4 - log2SqrtEs));
        const hs = Math.pow(2, Math.floor(hsExponent));
        this.#headDim = forceMin ? 4 : hs * Math.ceil(this.#numHeads / 4);
        this.#hiddenSize = forceMin ? 8 : this.#numHeads * this.#headDim;

        const ffnMultiplier = 8.0 - 4.0 * normalized;
        this.#feedForwardSize = forceMin ? 32 : Math.round(this.#hiddenSize * ffnMultiplier);

        this.#protoCapacityFactor = forceMin ? 0.5 : 1 - 0.5 * normalized;
        this.#memoryFactor = forceMin ? 2.5 : 1 + 1.5 * normalized;

        const rawBase = Math.round(this.#hiddenSize * 0.188 * this.#protoCapacityFactor);
        const minBase = Math.max(4, Math.round(this.#hiddenSize / 32));
        this.#baseProtoCapacity = forceMin ? 4 : Math.max(minBase, rawBase);

        this.#maxVariancePerDim = forceMin ? 20.9 : Number((8 + 25 * this.#protoCapacityFactor + 0.05 * this.#hiddenSize).toFixed(4));

        this.#semanticLR = forceMin ? 0.04 : Number((0.02 + 0.04 * this.#protoCapacityFactor).toFixed(4));
        this.#semanticBoost = forceMin ? 13 : Math.round(5 + 15 * this.#protoCapacityFactor);

        const longTermMultiplier = 0.4 + 0.6 * this.#protoCapacityFactor;
        const shortTermMultiplier = 0.8 + 0.7 * this.#protoCapacityFactor;
        const rawMultiplier = 0.6 + 0.8 * this.#protoCapacityFactor;
        const coreMultiplier = 1.0 + 1.5 * this.#protoCapacityFactor;
        const semanticMultiplier = 2.0 + 4.0 * this.#protoCapacityFactor;
        const coreEpisodicMultiplier = 0.8 + 1.2 * this.#protoCapacityFactor;
        const retrievalMultiplier = semanticMultiplier;
        const candidatesMultiplier = 2 * semanticMultiplier;

        this.#longTermMaxProtos = forceMin ? 3 : Math.round(this.#baseProtoCapacity * longTermMultiplier);
        this.#shortTermMaxProtos = forceMin ? 5 : Math.round(this.#baseProtoCapacity * shortTermMultiplier);
        this.#rawMaxProtos = forceMin ? 4 : Math.round(this.#baseProtoCapacity * rawMultiplier);
        this.#semanticMaxProtos = forceMin ? 16 : Math.round(this.#baseProtoCapacity * semanticMultiplier);
        this.#effectiveSemanticMax = forceMin ? 19 : Math.round(this.#semanticMaxProtos * (1.1 + 0.15 * this.#protoCapacityFactor));

        this.#coreMaxProtos = forceMin ? 7 : Math.round(this.#baseProtoCapacity * coreMultiplier);
        this.#coreEpisodicMaxEntries = forceMin ? 6 : Math.round(this.#baseProtoCapacity * coreEpisodicMultiplier);

        this.#contextWindow = forceMin ? 50 : Math.round(this.#hiddenSize * 2.5 * this.#memoryFactor);
        this.#adaptiveWindow = forceMin ? 13 : Math.max(1, Math.round(this.#contextWindow * 0.25));
        
        this.#maxTrustHistory = forceMin ? 100 : Math.round(this.#contextWindow * 2);
        this.#maxPerformanceHistory = forceMin ? 200 : Math.round(this.#contextWindow * 4);

        const targetProportion = 0.35 + 0.3 * this.#protoCapacityFactor;
        const minLowDim = Math.max(4, Math.round(this.#hiddenSize * 0.18));
        const maxLowDim = Math.round(this.#hiddenSize * 0.78);
        const candidate = Math.round(this.#hiddenSize * targetProportion);
        this.#lowDim = forceMin ? 4 : Math.max(minLowDim, Math.min(maxLowDim, candidate));

        const numProjectionsRaw = 16.0 - 10.0 * normalized;
        this.#numProjections = forceMin ? 6 : Math.max(6, Math.round(numProjectionsRaw));

        const baseLrUnscaled = 0.12 / Math.max(this.#inputSize, 1);
        const sizeScale = Math.pow(this.#hiddenSize, -0.18);
        this.#learningRate = Number(Math.min(0.0025, baseLrUnscaled * sizeScale).toPrecision(6));
        this.#learningRateDecay = Number((this.#learningRate / 10).toPrecision(6));

        this.#swarmIntelligenceFactor = forceMin ? 0.95 : Number((0.05 + 0.90 * normalized).toFixed(6));

        const baseFreq = 10 + Math.ceil(this.#ensembleSize / 8);
        this.#gradientResetFrequency = forceMin ? 30 : baseFreq + Math.round(50 * this.#swarmIntelligenceFactor);

        this.#maxRetrievedProtos = forceMin ? 16 : Math.round(this.#baseProtoCapacity * retrievalMultiplier);
        this.#numRetrievalCandidates = forceMin ? 32 : Math.round(this.#baseProtoCapacity * candidatesMultiplier);
        
        this.#semanticMergeEvery  = forceMin ? 20 : Math.round(8 + 24 * this.#protoCapacityFactor);

        this.#maxEpisodicConsider = forceMin ? 45 : Math.round(this.#contextWindow * (0.4 + 0.2 * this.#memoryFactor));
        this.#replaySamples = forceMin ? 3 : Math.round(this.#baseProtoCapacity * (0.5 + 0.5 * this.#protoCapacityFactor));

        this.#priorityMax = forceMin ? 16 : this.#maxRetrievedProtos;

        this.#numLshSets = forceMin ? 2 : Math.max(1, Math.floor(this.#numProjections / 3));

        const dimScale = Math.max(1, Math.log2(this.#lowDim / 4));
        const capacityScale = Math.max(0.5, this.#protoCapacityFactor);
        const dimFactor = Math.max(1, Math.log2(this.#lowDim / 8));
        const rawTables = Math.round(this.#hiddenSize * (0.04 + 0.12 * this.#protoCapacityFactor) * dimScale);
        const rawBits = Math.round(this.#hiddenSize * (0.06 + 0.18 * this.#protoCapacityFactor) * dimScale);
        const maxTablesCap = Math.round(this.#lowDim * 6 * capacityScale);
        const maxBitsCap = Math.round(this.#lowDim * 3 * capacityScale);

        this.#lshNumTables = forceMin ? 2 : Math.max(Math.round(4 * capacityScale * dimFactor), Math.min(maxTablesCap, rawTables));
        this.#lshHashBits = forceMin ? 6 : Math.max(Math.round(12 * capacityScale * dimFactor), Math.min(maxBitsCap, rawBits));

        this.#tempOverloadFactor = forceMin ? 2.5 : 2.0 + 1.0 * (1 - this.#protoCapacityFactor);
        this.#mergeTrimFactor = forceMin ? 1.75 : 1.5 + 0.5 * (1 - this.#protoCapacityFactor);

        this.#kernelGamma = forceMin ? 32.0 : Number((16 + 32 * this.#protoCapacityFactor).toFixed(1));

        this.#projectionMatrices = Array.from({length: this.#numProjections}, () => this.#generateProjectionMatrix());

        this.#lshHyperplanes = Array.from({length: this.#numLshSets}, () => this.#generateLshHyperplanesLow());

        this.#semanticLSHBuckets = Array(this.#ensembleSize).fill().map(() => 
            Array(this.#numLshSets).fill().map(() => 
                Array(this.#lshNumTables).fill().map(() => new Map())
            )
        );

        this.#performanceScores = Array(this.#ensembleSize).fill(0);
        this.#agreementScores = Array(this.#ensembleSize).fill(0);
        this.#specializationScores = Array(this.#ensembleSize).fill(0);
        this.#trustScoresHistory = Array(this.#ensembleSize).fill().map(() => [0]);
        this.#historicalPerformance = Array(this.#ensembleSize).fill().map(() => [0]);
        this.#adaptiveLearningRate = Array(this.#ensembleSize).fill(this.#learningRate);

        this.#ensembleWeights = Array(this.#ensembleSize).fill().map(() => 
            Math.max(0, 1 / this.#ensembleSize + (Math.random() - 0.5) * 0.1 / this.#ensembleSize)
        );
        this.#normalizeEnsembleWeights();

        this.#attentionMemory = Array(this.#ensembleSize).fill().map(() => []);
        this.#adaptiveContext = Array(this.#ensembleSize).fill().map(() => []);
        this.#semanticProtos = Array(this.#ensembleSize).fill().map(() => []);
        this.#coreEpisodic = Array(this.#ensembleSize).fill().map(() => []);
        this.#priorityIndices = Array(this.#ensembleSize).fill().map(() => []);

        this.#attentionWeightMatrix = Array(this.#ensembleSize).fill().map(() =>
            this.#dynamicInit(this.#hiddenSize, 1, 0, 1).map(row => row[0])
        );

        this.#attentionBias = Array(this.#ensembleSize).fill().map(() =>
            Array(this.#hiddenSize).fill().map(() => (Math.random() - 0.5) * Math.sqrt(4 / this.#hiddenSize))
        );

        this.#specializationWeights = Array(this.#ensembleSize).fill().map(() =>
            Array(this.#hiddenSize).fill().map((_, j) =>
                Array(this.#hiddenSize).fill().map((_, k) => {
                    const baseWeights = this.#dynamicInit(this.#hiddenSize, this.#hiddenSize, 0, 1);
                    const scale = 1 + 0.1 * (j / this.#hiddenSize + k / this.#hiddenSize);
                    return baseWeights[j][k] * scale;
                })
            )
        );

        this.#transformers = this.#setTransformerStructure();

        this.#gradientAccumulation = this.#setGradientStructure();
    }

    #dynamicInit (rows, cols, layerIndex, totalLayers, customK = null) {
        let baseK = customK !== null ? customK : 2.2;
        let fanInScale = Math.min(1.0, 1000 / rows);
        let depthScale = Math.pow(totalLayers, -layerIndex / totalLayers);
        let k = baseK * fanInScale * depthScale;
        k = Math.max(1.5, Math.min(k, 3.0));

        return Array(rows).fill().map(() =>
            Array(cols).fill().map(() => (Math.random() - 0.5) * Math.sqrt(k / rows))
        );
    }

    #setTransformerStructure () {
        return Array(this.#ensembleSize).fill().map(() => ({
            attentionWeights: Array(this.#numLayers).fill().map((_, layerIndex) => ({
                Wq: this.#dynamicInit(this.#hiddenSize, this.#hiddenSize, layerIndex, this.#numLayers),
                Wk: this.#dynamicInit(this.#hiddenSize, this.#hiddenSize, layerIndex, this.#numLayers),
                Wv: this.#dynamicInit(this.#hiddenSize, this.#hiddenSize, layerIndex, this.#numLayers),
                Wo: this.#dynamicInit(this.#hiddenSize, this.#hiddenSize, layerIndex, this.#numLayers)
            })),

            ffnWeights: Array(this.#numLayers).fill().map((_, layerIndex) => ({
                gate_proj: this.#dynamicInit(this.#hiddenSize, this.#feedForwardSize, layerIndex, this.#numLayers),
                up_proj: this.#dynamicInit(this.#hiddenSize, this.#feedForwardSize, layerIndex, this.#numLayers),
                down_proj: this.#dynamicInit(this.#feedForwardSize, this.#hiddenSize, layerIndex, this.#numLayers)
            })),
            
            layerNormWeights: Array(this.#numLayers).fill().map(() => ({
                gamma1: Array(this.#hiddenSize).fill(1.0),
                gamma2: Array(this.#hiddenSize).fill(1.0)
            })),

            outputWeights: this.#dynamicInit(this.#hiddenSize, 1, this.#numLayers, this.#numLayers + 1, 2.1),
            outputBias: Array(1).fill(0)
        }));
    }

    #setGradientStructure () {
        return Array(this.#ensembleSize).fill().map(() => ({
            outputWeights: Array(this.#hiddenSize).fill().map(() => Array(1).fill(0)),
            outputBias: Array(1).fill(0),

            attentionWeights: Array(this.#numLayers).fill().map(() => ({
                Wq: Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                Wk: Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                Wv: Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                Wo: Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0))
            })),

            ffnWeights: Array(this.#numLayers).fill().map(() => ({
                gate_proj: Array(this.#hiddenSize).fill().map(() => Array(this.#feedForwardSize).fill(0)),
                up_proj: Array(this.#hiddenSize).fill().map(() => Array(this.#feedForwardSize).fill(0)),
                down_proj: Array(this.#feedForwardSize).fill().map(() => Array(this.#hiddenSize).fill(0))
            })),

            layerNormWeights: Array(this.#numLayers).fill().map(() => ({
                gamma1: Array(this.#hiddenSize).fill(0),
                gamma2: Array(this.#hiddenSize).fill(0)
            })),

            attentionBias: Array(this.#hiddenSize).fill(0),
            attentionWeightMatrix: Array(this.#hiddenSize).fill(0),
            specializationWeights: Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0))
        }));
    }

    #silu (x) {
        if (!isValidNumber(x)) return 0;
        const sig = 1 / (1 + Math.exp(-x));
        return x * sig;
    }

    #siluDerivative (x) {
        if (!isValidNumber(x)) return 0;
        const sig = 1 / (1 + Math.exp(-x));
        return sig * (1 + x * (1 - sig));
    }

    #sigmoid (x) {
        return isValidNumber(x) ? 1 / (1 + Math.exp(-Math.min(Math.max(x, -100), 100))) : 0;
    }

    #softmax (arr) {
        if (!arr.every(isValidNumber)) return arr.map(() => 1 / arr.length);
        const max = Math.max(...arr);
        const exp = arr.map(x => Math.exp(x - max));
        const sum = exp.reduce((a, b) => a + b, 0) || 1;
        return exp.map(x => x / sum);
    }

    #randomNormal (mean = 0, stdDev = 1) {
        let sum = 0;
        for (let i = 0; i < 12; i += 1) {
            sum += Math.random();
        }
        return mean + stdDev * (sum - 6);
    }

    #generateProjectionMatrix () {
        const invSqrtLow = 1 / Math.sqrt(this.#lowDim);
        const matrix = new Array(this.#hiddenSize);
        for (let d = 0; d < this.#hiddenSize; d++) {
            const row = new Float32Array(this.#lowDim);
            for (let l = 0; l < this.#lowDim; l++) {
                const u = 1 - Math.random();
                const v = Math.random();
                const g = Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
                row[l] = g * invSqrtLow;
            }
            matrix[d] = row;
        }
        return matrix;
    }

    #generateLshHyperplanesLow () {
        return Array.from({length: this.#lshNumTables}, () =>
            Array.from({length: this.#lshHashBits}, () => {
                const vec = new Float32Array(this.#lowDim);
                let normSq = 0;
                for (let d = 0; d < this.#lowDim; d++) {
                    vec[d] = this.#randomNormal(0, 1);
                    normSq += vec[d] * vec[d];
                }
                const norm = Math.sqrt(normSq) || 1;
                for (let d = 0; d < this.#lowDim; d++) {
                    vec[d] /= norm;
                }
                return vec;
            })
        );
    }

    #computeProjNorms (mean) {
        const projs = [];
        for (let np = 0; np < this.#numProjections; np++) {
            const proj = new Float32Array(this.#lowDim).fill(0);
            for (let d = 0; d < this.#hiddenSize; d++) {
                const val = mean[d];
                const row = this.#projectionMatrices[np][d];
                for (let l = 0; l < this.#lowDim; l++) {
                    proj[l] += val * row[l];
                }
            }
            let norm = 0;
            for (let l = 0; l < this.#lowDim; l++) norm += proj[l] * proj[l];
            norm = Math.sqrt(norm);
            if (norm < 1e-8) norm = 1;
            for (let l = 0; l < this.#lowDim; l++) proj[l] /= norm;
            projs.push(proj);
        }
        return projs;
    }

    #projSimilarity (projsA, projsB) {
        if (!projsA || !projsB || projsA.length !== this.#numProjections || projsB.length !== this.#numProjections) {
            return 0;
        }
        let sumSim = 0;
        for (let np = 0; np < this.#numProjections; np++) {
            sumSim += this.#vectorDot(projsA[np], projsB[np]);
        }
        return sumSim / this.#numProjections;
    }

    #computeProtoUtility (proto) {
        if (!proto || !proto.mean || proto.mean.length !== this.#hiddenSize) return 0;

        let varSum = 0;
        for (let j = 0; j < this.#hiddenSize; j++) {
            varSum += proto.variance[j];
        }
        const avgVariance = varSum / this.#hiddenSize;

        const importanceFactor = 1 + 0.2 * Math.sqrt(Math.max(0, proto.importance || 0));
        const effectiveCount = Math.sqrt((proto.size || 1) * (proto.accessCount || 1));
        return effectiveCount * (1 + Math.sqrt(Math.max(avgVariance, 1e-6))) * importanceFactor;
    }

    #rmsNorm (x, gamma, eps = 1e-6) {
        if (!Array.isArray(x) || x.length !== this.#hiddenSize ||
            !Array.isArray(gamma) || gamma.length !== this.#hiddenSize) {
            return Array(this.#hiddenSize).fill(0);
        }

        let sq_sum = 0;
        for (let i = 0; i < this.#hiddenSize; i++) {
            const val = x[i];
            sq_sum += val * val;
        }
        const rms = Math.sqrt(sq_sum / this.#hiddenSize + eps);

        const output = Array(this.#hiddenSize);
        for (let i = 0; i < this.#hiddenSize; i++) {
            output[i] = x[i] * gamma[i] / rms;
        }
        return output;
    }

    #normalizeSemantic (transformerIdx, sem) {
        const maxMeanRMS = 1.8 + 2.2 * this.#protoCapacityFactor;
        const maxSize = Math.round(this.#baseProtoCapacity * (20 + 80 * this.#protoCapacityFactor));
        const maxAccess = Math.round(this.#baseProtoCapacity * (40 + 160 * this.#protoCapacityFactor));
        const maxImportance = Math.round(100 + 300 * this.#protoCapacityFactor);

        for (const proto of sem) {
            const rms = Math.sqrt(this.#vectorDot(proto.mean, proto.mean) / this.#hiddenSize + 1e-8);
            if (rms > maxMeanRMS) {
                const scale = maxMeanRMS / rms;
                this.#vectorScale(proto.mean, scale, proto.mean);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    proto.variance[j] *= scale ** 2;
                    proto.variance[j] = Math.min(proto.variance[j], this.#maxVariancePerDim);
                }
                proto.projNorms = this.#computeProjNorms(proto.mean);
                this.#updateProtoInLSH(transformerIdx, proto);
            }

            if (proto.size > maxSize) proto.size = maxSize;
            if (proto.accessCount > maxAccess) proto.accessCount = maxAccess;
            if (proto.importance > maxImportance) proto.importance = maxImportance;
        }
    }

    #applyRoPE (matrix, startPos = 0) {
        if (!Array.isArray(matrix) || matrix.length === 0 || !matrix[0] || matrix[0].length !== this.#hiddenSize) return;

        const head_dim = this.#hiddenSize / this.#numHeads;
        if (head_dim % 2 !== 0) return;

        const base = 10000.0;
        const seqLen = matrix.length;

        for (let pos = 0; pos < seqLen; pos++) {
            const absPos = startPos + pos;
            for (let h = 0; h < this.#numHeads; h++) {
                const offset = h * head_dim;
                for (let i = 0; i < head_dim / 2; i++) {
                    const exp = -2.0 * i / head_dim;
                    const theta = absPos * Math.pow(base, exp);
                    const cos = Math.cos(theta);
                    const sin = Math.sin(theta);
                    const idx1 = offset + 2 * i;
                    const idx2 = offset + 2 * i + 1;

                    const x = matrix[pos][idx1];
                    const y = matrix[pos][idx2];

                    matrix[pos][idx1] = x * cos - y * sin;
                    matrix[pos][idx2] = x * sin + y * cos;
                }
            }
        }
    }

    #weightedMean (protos) {
        if (!Array.isArray(protos) || protos.length === 0) {
            return new Float32Array(this.#hiddenSize);
        }
        let totalSize = 0;
        for (const p of protos) {
            totalSize += p.size || 0;
        }
        if (totalSize === 0) {
            return new Float32Array(this.#hiddenSize);
        }
        const rep = new Float32Array(this.#hiddenSize);
        for (const p of protos) {
            const weight = (p.size || 0) / totalSize;
            this.#vectorAdd(rep, p.mean, 1, weight, rep);
        }
        return rep;
    }

    #maxPairwiseKernel (protosA, protosB) {
        if (protosA.length === 0 || protosB.length === 0) return 0;
        let maxSim = 0;
        for (const a of protosA) {
            for (const b of protosB) {
                const sim = this.#kernelSimilarity(a, b);
                if (sim > maxSim) maxSim = sim;
            }
        }
        return maxSim;
    }

    #vectorDot (a, b) {
        if (a.length !== b.length) return 0;
        let sum = 0;
        for (let i = 0; i < a.length; i++) {
            sum += a[i] * b[i];
        }
        return sum;
    }

    #vectorNorm (a) {
        return Math.sqrt(this.#vectorDot(a, a));
    }

    #vectorAdd (a, b, coeffA = 1, coeffB = 1, target = null) {
        const len = a.length;
        if (len !== b.length) return new Float32Array(a);
        const result = target || new Float32Array(len);
        for (let i = 0; i < len; i++) {
            result[i] = a[i] * coeffA + b[i] * coeffB;
        }
        return result;
    }

    #vectorScale (vec, scalar, target = null) {
        const len = vec.length;
        const result = target || new Float32Array(len);
        for (let i = 0; i < len; i++) {
            result[i] = vec[i] * scalar;
        }
        return result;
    }

    #vectorSub (a, b, target = null) {
        return this.#vectorAdd(a, b, 1, -1, target);
    }

    #cosineSimilarity (a, b) {
        const dot = this.#vectorDot(a, b);
        const normA = this.#vectorNorm(a);
        const normB = this.#vectorNorm(b);
        return dot / (normA * normB + 1e-8);
    }

    #sampleDirichlet (count) {
        if (count < 1) return [];
        const gammas = [];
        for (let i = 0; i < count; i++) {
            let g = 0;
            while (g <= 0) g = -Math.log(Math.random() || 1e-10);
            gammas.push(g);
        }
        const sum = gammas.reduce((a, b) => a + b, 0);
        return gammas.map(g => g / sum);
    }

    #detectSuddenDrop (transformerIdx) {
        const hist = this.#historicalPerformance[transformerIdx];

        const minHistDrop = Math.max(6, Math.round(this.#contextWindow * 0.06));
        if (hist.length < minHistDrop) return 1.0;

        const current = this.#performanceScores[transformerIdx] ?? 0.5;

        const recentFraction = 0.06 + 0.04 * this.#memoryFactor;
        const numRecentPoints = Math.max(5, Math.round(this.#maxPerformanceHistory * recentFraction));

        let prevSum = 0;
        let count = 0;
        const startIdx = Math.max(0, hist.length - numRecentPoints - 2);
        for (let i = startIdx; i < hist.length - 2; i++) {
            if (i >= 0 && i < hist.length) {
                prevSum += hist[i];
                count++;
            }
        }
        const prevMean = count > 0 ? prevSum / count : current;

        const drop = prevMean - current;

        const severeDropThresh = 0.10 + 0.08 * (1 - this.#protoCapacityFactor);
        const moderateDropThresh = 0.05 + 0.05 * (1 - this.#protoCapacityFactor);
        const adaptiveAllowance = 0.06 + 0.06 * (1 - this.#protoCapacityFactor);

        if (drop > severeDropThresh + adaptiveAllowance * (1 - prevMean)) return 3.0;
        if (drop > moderateDropThresh) return 1.8;
        return 1.0;
    }

    #computeLSHHashesLow (projNorm, hyperplanesSet) {
        const hashes = new Array(this.#lshNumTables);
        for (let t = 0; t < this.#lshNumTables; t++) {
            let hash = 0n;
            for (let b = 0; b < this.#lshHashBits; b++) {
                let dot = 0;
                const hyp = hyperplanesSet[t][b];
                for (let d = 0; d < this.#lowDim; d++) {
                    dot += projNorm[d] * hyp[d];
                }
                if (dot > 0) {
                    hash |= 1n << BigInt(b);
                }
            }
            hashes[t] = hash;
        }
        return hashes;
    }

    #insertProtoToLSH (transformerIdx, proto) {
        if (!proto || !proto.mean || !proto.projNorms || proto.projNorms.length < this.#numLshSets) return;

        const buckets = this.#semanticLSHBuckets[transformerIdx];
        const hashesAll = [];

        for (let s = 0; s < this.#numLshSets; s++) {
            const proj = proto.projNorms[s];
            const hashes = this.#computeLSHHashesLow(proj, this.#lshHyperplanes[s]);
            hashesAll.push(hashes);

            const setBuckets = buckets[s];
            for (let t = 0; t < this.#lshNumTables; t++) {
                const key = hashes[t];
                let bucket = setBuckets[t].get(key);
                if (!bucket) {
                    bucket = new Set();
                    setBuckets[t].set(key, bucket);
                }
                bucket.add(proto);
            }
        }
        proto.lshHashes = hashesAll;
    }

    #removeProtoFromLSH (transformerIdx, proto) {
        if (!proto || !proto.lshHashes) return;

        const buckets = this.#semanticLSHBuckets[transformerIdx];
        for (let s = 0; s < this.#numLshSets; s++) {
            const hashes = proto.lshHashes[s];
            if (!hashes) continue;
            const setBuckets = buckets[s];
            for (let t = 0; t < this.#lshNumTables; t++) {
                const key = hashes[t];
                const bucket = setBuckets[t].get(key);
                if (bucket) {
                    bucket.delete(proto);
                    if (bucket.size === 0) {
                        setBuckets[t].delete(key);
                    }
                }
            }
        }
        proto.lshHashes = undefined;
    }

    #updateProtoInLSH (transformerIdx, proto) {
        this.#removeProtoFromLSH(transformerIdx, proto);
        this.#insertProtoToLSH(transformerIdx, proto);
    }

    #getAvgProtoVariance (transformerIdx) {
        const sem = this.#semanticProtos[transformerIdx];
        if (sem.length === 0) return 0.0;

        let totalVarSum = 0;
        for (const proto of sem) {
            let protoVarSum = 0;
            for (let j = 0; j < this.#hiddenSize; j++) {
                protoVarSum += proto.variance[j];
            }
            totalVarSum += protoVarSum;
        }
        return totalVarSum / (sem.length * this.#hiddenSize);
    }

    #isStagnating (transformerIdx) {
        const history = this.#historicalPerformance[transformerIdx];

        const minHistStag = Math.max(5, Math.round(this.#contextWindow * 0.05));
        if (history.length < minHistStag) return false;

        const recent = history.slice(-minHistStag);
        const currentPerf = this.#performanceScores[transformerIdx] ?? 0.5;
        const meanRecent = recent.reduce((a, b) => a + b, 0) / recent.length;
        const variance = recent.reduce((a, v) => a + (v - meanRecent) ** 2, 0) / recent.length;
        const trend = currentPerf - recent[0];

        const lowVarianceThreshold = 0.004 + 0.008 * (1 - this.#protoCapacityFactor);
        const noProgressThreshold = 0.005 + 0.01 * (1 - this.#protoCapacityFactor);

        const lowVariance = variance < lowVarianceThreshold;
        const noProgress = trend < noProgressThreshold;

        const avgProtoVar = this.#getAvgProtoVariance(transformerIdx);
        const protoVarThreshold = this.#maxVariancePerDim * 0.12;
        const lowProtoVariance = avgProtoVar < protoVarThreshold;

        return lowVariance || noProgress || lowProtoVariance;
    }

    #replayOldMemory (transformerIdx) {
        const attMem = this.#attentionMemory[transformerIdx];

        const minMemLengthForReplay = Math.max(8, Math.round(this.#contextWindow * 0.08));
        if (attMem.length < minMemLengthForReplay) return;

        const perf = this.#performanceScores[transformerIdx] ?? 0.5;
        const stagnationFactor = this.#isStagnating(transformerIdx) ? 5.0 : 1.0;
        const dropBoost = this.#detectSuddenDrop(transformerIdx);
        let totalBoost = stagnationFactor * dropBoost * (1 + 2 * (1 - perf));

        const avgProtoVar = this.#getAvgProtoVariance(transformerIdx);
        const lowProtoVariance = avgProtoVar < this.#maxVariancePerDim * 0.12;
        if (lowProtoVariance) {
            totalBoost *= 2.0;
        }

        let numSamples = Math.round(this.#replaySamples * (1 + 5 * (1 - perf)) * totalBoost);
        const minSamples = Math.max(4, Math.round(this.#baseProtoCapacity * 0.166));
        numSamples = Math.max(numSamples, minSamples);

        const sampledProtos = [];
        const divisor = Math.max(numSamples + 4, Math.round(numSamples * 1.5));
        const step = Math.max(1, Math.floor(attMem.length / divisor));

        const boostBase = Math.round(8 + 24 * this.#protoCapacityFactor);

        for (let i = 1; i <= numSamples; i++) {
            const idx = attMem.length - i * step - 1;
            if (idx < 0) break;
            const entry = attMem[idx];
            if (!entry || entry.protos.length === 0) continue;

            for (const p of entry.protos) {
                const newMean = new Float32Array(p.mean);
                const newVariance = new Float32Array(p.variance);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newVariance[j] = Math.min(newVariance[j], this.#maxVariancePerDim);
                }
                sampledProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: p.size * 0.9,
                    accessCount: p.accessCount * 2.5 + p.size * boostBase,
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: p.isCore,
                    importance: (p.importance || 0) * 0.95
                });
            }
        }

        const coreEp = this.#coreEpisodic[transformerIdx];
        if (coreEp.length > 0) {
            let coreNumSamples = Math.round(numSamples * 1.5 * totalBoost);
            const minCoreSamples = Math.round(this.#coreMaxProtos * 0.2);
            coreNumSamples = Math.max(coreNumSamples, minCoreSamples);

            let noiseFactor = 0.15 + 0.6 * (1 - perf) * totalBoost;
            if (lowProtoVariance) noiseFactor += 1.2;

            for (let i = 0; i < coreNumSamples; i++) {
                const entry = coreEp[Math.floor(Math.random() * coreEp.length)];
                if (!entry || entry.protos.length === 0) continue;

                const p = entry.protos[Math.floor(Math.random() * entry.protos.length)];

                const newMean = new Float32Array(this.#hiddenSize);
                const newVariance = new Float32Array(this.#hiddenSize);

                for (let j = 0; j < this.#hiddenSize; j++) {
                    const std = Math.sqrt(Math.max(p.variance[j], 1e-6)) * noiseFactor;
                    newMean[j] = this.#randomNormal(p.mean[j], std);
                    newVariance[j] = Math.min(p.variance[j] * (1 + 0.4 * noiseFactor), this.#maxVariancePerDim);
                }

                sampledProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: p.size * 1.0,
                    accessCount: p.accessCount * 2.0 + p.size * Math.round(20 + 60 * this.#protoCapacityFactor),
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: true,
                    importance: (p.importance || 0) + 40
                });
            }
        }

        if (dropBoost > 2.0) {
            const faithfulNum = Math.round(this.#replaySamples * 0.7 * dropBoost);
            const faithStep = Math.max(1, Math.floor(attMem.length / (faithfulNum + 5)));

            for (let i = 1; i <= faithfulNum; i++) {
                const idx = attMem.length - i * faithStep - 1;
                if (idx < 0) break;
                const entry = attMem[idx];
                if (!entry || entry.protos.length === 0) continue;

                for (const p of entry.protos) {
                    sampledProtos.push({
                        mean: new Float32Array(p.mean),
                        variance: new Float32Array(p.variance),
                        size: p.size * (1.3 + 0.7 * (dropBoost - 1)),
                        accessCount: p.accessCount * 4.0 + p.size * boostBase * (3 + 2 * this.#protoCapacityFactor),
                        projNorms: this.#computeProjNorms(p.mean),
                        isCore: p.isCore,
                        importance: (p.importance || 0) * 1.4 + Math.round(40 + 120 * this.#protoCapacityFactor) * (dropBoost - 1)
                    });
                }
            }

            if (coreEp.length > 0) {
                const coreFaithNum = Math.round(faithfulNum * 1.8);
                for (let i = 0; i < coreFaithNum; i++) {
                    const entry = coreEp[Math.floor(Math.random() * coreEp.length)];
                    if (!entry || entry.protos.length === 0) continue;
                    const p = entry.protos[Math.floor(Math.random() * entry.protos.length)];

                    sampledProtos.push({
                        mean: new Float32Array(p.mean),
                        variance: new Float32Array(p.variance),
                        size: p.size * (1.8 + 1.0 * (dropBoost - 1)),
                        accessCount: p.accessCount * 6.0 + p.size * boostBase * (6 + 4 * this.#protoCapacityFactor),
                        projNorms: this.#computeProjNorms(p.mean),
                        isCore: true,
                        importance: (p.importance || 0) + Math.round(80 + 240 * this.#protoCapacityFactor) * dropBoost
                    });
                }
            }
        }

        if (sampledProtos.length > 0) {
            const effectiveMax = Math.round(this.#effectiveSemanticMax * this.#tempOverloadFactor);
            if (sampledProtos.length > effectiveMax) {
                sampledProtos.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
                sampledProtos.length = effectiveMax;
            }
            this.#updateSemanticProtos(transformerIdx, sampledProtos);
        }
    }

    #generativeReplay (transformerIdx) {
        const sem = this.#semanticProtos[transformerIdx];
        if (sem.length === 0) return;

        const pcf = this.#protoCapacityFactor;
        const perf = this.#performanceScores[transformerIdx] ?? 0.5;
        const agreement = this.#agreementScores[transformerIdx] ?? 0.5;
        const overconfidence = perf * agreement;
        const forgetPressure = (1 - perf) * 0.8 + (1 - agreement) * 0.2;
        let effectivePressure = forgetPressure + 0.8 * overconfidence;

        const stagnation = this.#isStagnating(transformerIdx);
        const dropBoost = this.#detectSuddenDrop(transformerIdx);
        let totalBoost = stagnation ? 4.0 : 1.0 * dropBoost * (1 + 1.5 * (1 - perf));
        let extraNoiseStag = stagnation ? 2.5 : 0.0;

        const avgProtoVar = this.#getAvgProtoVariance(transformerIdx);
        const lowProtoVariance = avgProtoVar < this.#maxVariancePerDim * 0.12;
        if (lowProtoVariance) {
            totalBoost *= 2.5;
            extraNoiseStag += 5.0;
            effectivePressure += 1.5;
        }

        const generatedProtos = [];

        const cores = sem.filter(p => p.isCore);
        if (cores.length > 0) {
            const coreReplayPressure = effectivePressure + (1 - perf) * 0.8;
            const numCoreSamples = Math.round((this.#coreMaxProtos / 4 + this.#coreMaxProtos * coreReplayPressure) * totalBoost);
            let coreNoiseFactor = 0.10 + 0.45 * effectivePressure + extraNoiseStag * 1.2;
            if (lowProtoVariance) coreNoiseFactor += 1.8;

            for (let s = 0; s < numCoreSamples; s++) {
                const coreP = cores[Math.floor(Math.random() * cores.length)];
                const newMean = new Float32Array(this.#hiddenSize);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    const std = Math.sqrt(Math.max(coreP.variance[j], 1e-6)) * coreNoiseFactor;
                    newMean[j] = this.#randomNormal(coreP.mean[j], std);
                }
                const newVariance = new Float32Array(this.#hiddenSize);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newVariance[j] = Math.min(Math.max(coreP.variance[j] * (1 + 0.35 * coreNoiseFactor), 1e-6), this.#maxVariancePerDim);
                }

                generatedProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: Math.max(1, coreP.size * 0.5 * (0.7 + 0.3 * perf)),
                    accessCount: coreP.accessCount * 1.0 + Math.round(8 + 24 * this.#protoCapacityFactor),
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: (coreP.importance || 0) * 0.95 + Math.round(6 + 18 * this.#protoCapacityFactor)
                });
            }
        }

        if (lowProtoVariance || stagnation) {
            const numDiversity = Math.round(this.#baseProtoCapacity * (2 + 8 * effectivePressure) * totalBoost);
            const highVar = this.#maxVariancePerDim * (0.75 + 0.25 * Math.random());

            for (let s = 0; s < numDiversity; s++) {
                const baseP = cores.length > 0 && Math.random() < 0.8 
                    ? cores[Math.floor(Math.random() * cores.length)] 
                    : (sem.length > 0 ? sem[Math.floor(Math.random() * sem.length)] : null);

                const newMean = new Float32Array(this.#hiddenSize);
                const divNoise = 5.0 + 15.0 * effectivePressure + (lowProtoVariance ? 15.0 : 0.0);

                if (baseP) {
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        const std = Math.sqrt(Math.max(baseP.variance[j], 1e-6)) * divNoise;
                        newMean[j] = this.#randomNormal(baseP.mean[j], std);
                    }
                } else {
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        newMean[j] = this.#randomNormal(0, divNoise);
                    }
                }

                const newVariance = new Float32Array(this.#hiddenSize).fill(highVar);

                generatedProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: Math.max(1, 20 * (0.5 + 0.5 * perf)),
                    accessCount: Math.round(25 + 50 * this.#protoCapacityFactor) + Math.round(40 + 80 * this.#protoCapacityFactor) * (1 - perf),
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: Math.round(20 + 40 * this.#protoCapacityFactor) + Math.round(30 + 70 * this.#protoCapacityFactor) * (1 - perf)
                });
            }
        }

        const baseNum = Math.round((this.#longTermMaxProtos + this.#semanticMaxProtos / 3 * effectivePressure) * totalBoost);
        let candidates = [];
        if (baseNum > 0) {
            const enriched = sem.map(p => ({
                proto: p,
                access: p.accessCount / (p.size + 1),
                utility: this.#computeProtoUtility(p)
            }));

            enriched.sort((a, b) => a.access - b.access || b.utility - a.utility);

            const numCandidates = Math.min(Math.round(this.#baseProtoCapacity * 12 * (1 + 3.5 * effectivePressure)), enriched.length);
            candidates = enriched.slice(0, numCandidates);

            const noiseFactor = 1.2 + 2.8 * effectivePressure + extraNoiseStag * 2.5;
            const samplesPerProto = Math.round((4 + 12 * effectivePressure) * pcf);

            let generatedCount = 0;
            for (const item of candidates) {
                if (generatedCount >= baseNum * samplesPerProto) break;
                const p = item.proto;

                for (let s = 0; s < samplesPerProto && generatedCount < baseNum * samplesPerProto; s++) {
                    const newMean = new Float32Array(this.#hiddenSize);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        const std = Math.sqrt(Math.max(p.variance[j], 1e-6)) * noiseFactor;
                        newMean[j] = this.#randomNormal(p.mean[j], std);
                    }

                    const newVariance = new Float32Array(this.#hiddenSize);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        newVariance[j] = Math.min(Math.max(p.variance[j] * (1 + 0.7 * noiseFactor), 1e-6), this.#maxVariancePerDim);
                    }

                    generatedProtos.push({
                        mean: newMean,
                        variance: newVariance,
                        size: Math.max(1, p.size * 0.25 * (0.6 + 0.4 * perf)),
                        accessCount: Math.round(5 + 10 * this.#protoCapacityFactor) + p.accessCount * 0.5,
                        projNorms: this.#computeProjNorms(newMean),
                        isCore: false,
                        importance: (p.importance || 0) * 0.6 + Math.round(2 + 6 * this.#protoCapacityFactor)
                    });
                    generatedCount++;
                }
            }
        }

        const numInterp = Math.round(baseNum * 1.5 * (1 + 1.8 * effectivePressure) * totalBoost);
        if (numInterp > 0 && candidates.length > 1) {
            const shuffled = candidates.slice();
            for (let i = shuffled.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
            }

            let interpCount = 0;
            for (let c = 0; c + 1 < shuffled.length && interpCount < numInterp; c += 2) {
                const p1 = shuffled[c].proto;
                const p2 = shuffled[c + 1].proto;

                let alpha = 0.25 + Math.random() * 0.5;
                const extrapProb = stagnation ? 0.95 : 0.3;
                if (Math.random() < extrapProb) {
                    alpha = Math.random() < 0.5 ? -0.4 + Math.random() * 0.5 : 1.0 + Math.random() * 0.8;
                }

                const newMean = new Float32Array(this.#hiddenSize);
                const newVariance = new Float32Array(this.#hiddenSize);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newMean[j] = alpha * p1.mean[j] + (1 - alpha) * p2.mean[j];
                    const diff = p1.mean[j] - p2.mean[j];
                    newVariance[j] = Math.max(p1.variance[j], p2.variance[j]) + alpha * (1 - alpha) * diff * diff * 3.0;
                    if (alpha < 0 || alpha > 1) {
                        newVariance[j] += Math.abs(alpha - 0.5) * 2 * diff * diff * 5.0;
                    }
                    newVariance[j] = Math.min(Math.max(newVariance[j], 1e-6), this.#maxVariancePerDim);
                }

                generatedProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: Math.max(1, (p1.size + p2.size) * 0.2 * (0.6 + 0.4 * perf)),
                    accessCount: Math.round(5 + 10 * this.#protoCapacityFactor) + (p1.accessCount + p2.accessCount) * 0.4,
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: ((p1.importance || 0) + (p2.importance || 0)) * 0.5 + Math.round(3 + 9 * this.#protoCapacityFactor)
                });
                interpCount++;
            }
        }

        const numCrossover = Math.round(baseNum * (0.6 + 1.2 * effectivePressure) * totalBoost);
        if (numCrossover > 0 && candidates.length > 1) {
            const shuffled = candidates.slice();
            for (let i = shuffled.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
            }

            let crossoverCount = 0;
            for (let c = 0; c + 1 < shuffled.length && crossoverCount < numCrossover; c += 2) {
                const p1 = shuffled[c].proto;
                const p2 = shuffled[c + 1].proto;

                const newMean = new Float32Array(this.#hiddenSize);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newMean[j] = Math.random() < 0.5 ? p1.mean[j] : p2.mean[j];
                }

                const mutationNoise = (0.3 + 0.8 * effectivePressure + extraNoiseStag) * 0.8;
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newMean[j] += this.#randomNormal(0, mutationNoise);
                }

                const newVariance = new Float32Array(this.#hiddenSize);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newVariance[j] = Math.max(p1.variance[j], p2.variance[j]) * (1 + 0.6 * effectivePressure);
                    newVariance[j] = Math.min(newVariance[j], this.#maxVariancePerDim);
                }

                generatedProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: Math.max(1, (p1.size + p2.size) * 0.18 * (0.6 + 0.4 * perf)),
                    accessCount: Math.round(6 + 12 * this.#protoCapacityFactor) + (p1.accessCount + p2.accessCount) * 0.35,
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: ((p1.importance || 0) + (p2.importance || 0)) * 0.45 + Math.round(5 + 10 * this.#protoCapacityFactor)
                });
                crossoverCount++;
            }
        }

        const minClusterSize = Math.max(3, Math.round(4 * this.#protoCapacityFactor));
        const mixPressure = effectivePressure + (stagnation ? 1 : 0) + (dropBoost - 1);
        if (mixPressure > 0.2 && sem.length >= minClusterSize) {
            const numMixSamples = Math.round(this.#baseProtoCapacity * (3 + 9 * mixPressure) * totalBoost);
            for (let s = 0; s < numMixSamples; s++) {
                let m = Math.round((minClusterSize + Math.random() * minClusterSize) * this.#protoCapacityFactor);
                if (stagnation) m += Math.round(2 * pcf);
                m = Math.max(minClusterSize, m);
                if (sem.length < m) continue;

                const shuffled = sem.slice();
                for (let i = shuffled.length - 1; i > 0; i--) {
                    const j = Math.floor(Math.random() * (i + 1));
                    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
                }
                const selected = shuffled.slice(0, m);

                const alphas = this.#sampleDirichlet(m);

                const newMean = new Float32Array(this.#hiddenSize);
                const newVariance = new Float32Array(this.#hiddenSize);

                for (let k = 0; k < m; k++) {
                    const alpha = alphas[k];
                    const p = selected[k];
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        newMean[j] += alpha * p.mean[j];
                        const diff = p.mean[j] - newMean[j];
                        newVariance[j] += alpha * (p.variance[j] + diff * diff * 1.5);
                    }
                }

                const mixNoise = (0.4 + 1.2 * effectivePressure + extraNoiseStag * 2) * 3.0;
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newVariance[j] += mixNoise;
                    newVariance[j] = Math.min(newVariance[j], this.#maxVariancePerDim);
                }

                generatedProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: Math.max(1, 20 * (0.5 + 0.5 * perf)),
                    accessCount: Math.round(12 + 24 * this.#protoCapacityFactor),
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: Math.round(10 + 20 * this.#protoCapacityFactor)
                });
            }
        }

        const needExploration = stagnation || perf < 0.7;
        if (needExploration) {
            const numRandom = Math.round(this.#baseProtoCapacity * (0.417 + 1.667 * (1 - perf)) * totalBoost);
            const randomVar = Math.min(this.#maxVariancePerDim, 8.0 + 16.0 * (1 - perf));
            for (let s = 0; s < numRandom; s++) {
                const newMean = new Float32Array(this.#hiddenSize);
                for (let j = 0; j < this.#hiddenSize; j++) {
                    newMean[j] = this.#randomNormal(0, 2.0);
                }
                const newVariance = new Float32Array(this.#hiddenSize).fill(randomVar);

                generatedProtos.push({
                    mean: newMean,
                    variance: newVariance,
                    size: Math.max(1, 5 * (0.5 + 0.5 * perf)),
                    accessCount: Math.round(3 + 9 * this.#protoCapacityFactor),
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: Math.round(3 + 9 * this.#protoCapacityFactor) * (1 - perf)
                });
            }
        }

        if (generatedProtos.length > 0) {
            const effectiveMax = Math.round(this.#effectiveSemanticMax * this.#tempOverloadFactor);
            if (generatedProtos.length > effectiveMax) {
                generatedProtos.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
                generatedProtos.length = effectiveMax;
            }
            this.#updateSemanticProtos(transformerIdx, generatedProtos);
        }
    }

    #poolMultiPrototype (entry, maxAdditionalProtos, thresholdFactor) {
        if (
            !Array.isArray(entry) ||
            entry.length === 0 ||
            !entry[0] ||
            !Array.isArray(entry[0]) ||
            entry[0].length !== this.#hiddenSize
        ) {
            return [];
        }
        const seqLen = entry.length;
        const hidden = this.#hiddenSize;

        const total = new Float32Array(hidden);
        for (let i = 0; i < seqLen; i++) {
            this.#vectorAdd(total, entry[i], 1, 1, total);
        }
        const globalMean = this.#vectorScale(total, 1 / seqLen);

        const globalVariance = new Float32Array(hidden);
        for (let i = 0; i < seqLen; i++) {
            const diff = this.#vectorSub(entry[i], globalMean);
            for (let j = 0; j < hidden; j++) {
                globalVariance[j] += diff[j] * diff[j];
            }
        }
        for (let j = 0; j < hidden; j++) globalVariance[j] /= seqLen;

        let avgVarSum = 0;
        for (let j = 0; j < hidden; j++) avgVarSum += globalVariance[j];
        const avgVar = avgVarSum / hidden;
        const thresholdSq = avgVar * hidden * thresholdFactor;

        const protos = [];

        const initialMean = new Float32Array(globalMean);
        const initialVariance = new Float32Array(hidden);
        for (let j = 0; j < hidden; j++) {
            initialVariance[j] = Math.max(globalVariance[j], 1e-6);
        }
        const initialProto = {
            mean: initialMean,
            variance: initialVariance,
            size: seqLen,
            accessCount: seqLen,
            projNorms: this.#computeProjNorms(initialMean),
            isCore: false,
            importance: 0
        };
        protos.push(initialProto);

        if (seqLen <= 1) {
            return protos;
        }

        for (let i = 0; i < seqLen; i++) {
            const point = entry[i];
            let minDistSq = Infinity;
            let bestProtoIdx = 0;
            for (let c = 0; c < protos.length; c++) {
                const diff = this.#vectorSub(point, protos[c].mean);
                const d = this.#vectorDot(diff, diff);
                if (d < minDistSq) {
                    minDistSq = d;
                    bestProtoIdx = c;
                }
            }

            if (protos.length < 1 + maxAdditionalProtos && minDistSq > thresholdSq) {
                const newMean = new Float32Array(point);
                const newVariance = new Float32Array(hidden).fill(1e-6);
                const newProto = {
                    mean: newMean,
                    variance: newVariance,
                    size: 1,
                    accessCount: 1,
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: 0
                };
                protos.push(newProto);
            } else {
                const proto = protos[bestProtoIdx];
                proto.size++;
                proto.accessCount += 1;
                const oldMean = new Float32Array(proto.mean);
                const delta = this.#vectorSub(point, oldMean);
                const size = proto.size;
                this.#vectorAdd(proto.mean, delta, 1, 1 / size, proto.mean);
                const newDiff = this.#vectorSub(point, proto.mean);
                for (let j = 0; j < hidden; j++) {
                    proto.variance[j] += delta[j] * newDiff[j];
                }
                proto.projNorms = this.#computeProjNorms(proto.mean);
            }
        }

        for (const proto of protos) {
            if (proto.size > 1) {
                for (let j = 0; j < hidden; j++) {
                    proto.variance[j] /= (proto.size - 1);
                    proto.variance[j] = Math.max(proto.variance[j], 1e-6);
                }
            }
        }
        return protos;
    }

    #updateSemanticProtos (transformerIdx, newProtos) {
        if (!Array.isArray(newProtos) || newProtos.length === 0) return;

        let sem = this.#semanticProtos[transformerIdx];
        const baseLR = this.#semanticLR;
        const maxP = this.#effectiveSemanticMax;
        const boost = this.#semanticBoost;
        const hidden = this.#hiddenSize;

        const perf = this.#performanceScores[transformerIdx] ?? 0.5;
        const agreement = this.#agreementScores[transformerIdx] ?? 0.5;
        const overload = sem.length / maxP;
        const mergePressure = Math.max(0, overload - 0.15) + (perf > 0.7 ? (perf - 0.7) * 3.0 : 0);
        let dynamicThreshold = (0.85 + 0.15 * this.#protoCapacityFactor) - 0.50 * mergePressure + 0.30 * (1 - agreement);
        const stagnation = this.#isStagnating(transformerIdx);
        if (stagnation) dynamicThreshold += 0.20;
        dynamicThreshold = Math.max(0.50, Math.min(0.98, dynamicThreshold));

        const accessDecay = 0.95 + 0.04 * perf;
        const sizeDecay = 0.98 + 0.01 * perf;
        const noiseThreshold = 1 + 12 * (1 - perf);
        const baseNoiseScale = 0.05 + 0.15 * (1 - perf);
        const effectiveBoost = boost * (1 + 2.5 * (1 - perf) + 1.2 * (1 - agreement));

        sem.forEach(p => {
            if (p.isCore) return;
            const importance = p.importance || 0;
            const decayPower = 1 / (1 + 0.04 * importance);
            p.accessCount *= Math.pow(accessDecay, decayPower);
            p.size *= Math.pow(sizeDecay, decayPower);

            if (p.accessCount < noiseThreshold) {
                let varianceSum = 0;
                for (let j = 0; j < hidden; j++) {
                    varianceSum += p.variance[j];
                }
                const avgVar = varianceSum / hidden;
                let noiseScale = baseNoiseScale * Math.sqrt(Math.max(avgVar, 1e-6));
                if (importance > 60) noiseScale = 0;
                else if (importance > 0) noiseScale /= (1 + importance / 10);
                if (noiseScale > 0) {
                    for (let j = 0; j < hidden; j++) {
                        p.mean[j] += (Math.random() - 0.5) * 2 * noiseScale * 1.5;
                    }
                    p.projNorms = this.#computeProjNorms(p.mean);
                    this.#updateProtoInLSH(transformerIdx, p);
                }
            }
        });

        for (const p of newProtos) {
            if (!p || !p.mean) continue;

            if (!p.projNorms || p.projNorms.length !== this.#numProjections) p.projNorms = this.#computeProjNorms(p.mean);

            p.isCore = p.isCore || false;
            if (typeof p.importance !== 'number') p.importance = 0;

            for (let j = 0; j < hidden; j++) {
                p.variance[j] = Math.min(Math.max(p.variance[j], 1e-6), this.#maxVariancePerDim);
            }

            if (sem.length === 0) {
                const newMean = new Float32Array(p.mean);
                const newVar = new Float32Array(hidden);
                for (let j = 0; j < hidden; j++) {
                    newVar[j] = p.variance[j];
                }
                const addedProto = {
                    mean: newMean,
                    variance: newVar,
                    size: p.size * effectiveBoost * 1.5,
                    accessCount: p.size * 2.0,
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: 15
                };
                sem.push(addedProto);
                this.#insertProtoToLSH(transformerIdx, addedProto);
                continue;
            }

            const n = sem.length;
            const topM = Math.min(Math.round(this.#effectiveSemanticMax * 1.3), Math.max(Math.round(this.#effectiveSemanticMax * 0.4), Math.round(n * 0.6)));
            const approxScores = new Float32Array(n);

            for (let i = 0; i < n; i++) {
                if (!sem[i].projNorms || sem[i].projNorms.length !== this.#numProjections) sem[i].projNorms = this.#computeProjNorms(sem[i].mean);
                approxScores[i] = this.#projSimilarity(p.projNorms, sem[i].projNorms);
            }

            const indices = Array.from({length: n}, (_, i) => i);
            indices.sort((a, b) => approxScores[b] - approxScores[a]);

            const topCandidates = indices.slice(0, topM);

            let bestSim = -1;
            let bestIdx = -1;
            for (const idx of topCandidates) {
                const sim = this.#kernelSimilarity(p, sem[idx]);
                if (sim > bestSim) {
                    bestSim = sim;
                    bestIdx = idx;
                }
            }

            let noveltyFactor = Math.max(0, (dynamicThreshold - bestSim) / dynamicThreshold);

            let merged = false;
            if (bestSim >= dynamicThreshold && bestIdx !== -1) {
                const proto = sem[bestIdx];
                const oldMean = new Float32Array(proto.mean);
                const importance = proto.importance || 0;

                let dynamicLR = baseLR / (1 + 0.8 * importance);
                let sizeBoost = 1.0;
                let accessBoost = 1.8;

                if (proto.isCore) {
                    dynamicLR *= 0.01 + 0.10 * perf + 0.08 * agreement;
                    sizeBoost = 0.03 + 0.15 * perf;
                    accessBoost = 0.3 + 0.5 * perf;
                }

                proto.size += p.size * sizeBoost;
                proto.accessCount += p.size * accessBoost;

                const diff = this.#vectorSub(p.mean, oldMean);
                const diffSqSum = this.#vectorDot(diff, diff);

                this.#vectorAdd(oldMean, p.mean, 1 - dynamicLR, dynamicLR, proto.mean);

                for (let j = 0; j < hidden; j++) {
                    const d = diff[j];
                    proto.variance[j] = (1 - dynamicLR) * proto.variance[j] + dynamicLR * (p.variance[j] + d * d * 1.5);
                    proto.variance[j] = Math.min(Math.max(proto.variance[j], 1e-6), this.#maxVariancePerDim);
                }
                proto.importance = importance + 3.0 * (diffSqSum * dynamicLR * dynamicLR) / hidden + (proto.isCore ? Math.round(15 + 35 * this.#protoCapacityFactor) : 0);
                proto.projNorms = this.#computeProjNorms(proto.mean);
                this.#updateProtoInLSH(transformerIdx, proto);
                merged = true;
            }

            if (!merged) {
                const lowerRepelThresh = Math.max(0.4, dynamicThreshold * 0.65 - 0.25 * mergePressure);
                if (bestSim > lowerRepelThresh && bestIdx !== -1) {
                    const bestProto = sem[bestIdx];
                    const diff = this.#vectorSub(p.mean, bestProto.mean);
                    const diffNorm = this.#vectorNorm(diff);
                    if (diffNorm > 1e-8) {
                        const direction = this.#vectorScale(diff, 1 / diffNorm);
                        let closeness = (bestSim - lowerRepelThresh) / (dynamicThreshold - lowerRepelThresh);
                        closeness = Math.min(1, Math.max(0, closeness));
                        let strength = closeness * (0.8 + 1.5 * noveltyFactor);
                        if (stagnation) strength *= 1.8;
                        const avgProtoVar = this.#getAvgProtoVariance(transformerIdx);
                        strength *= (0.5 + 0.8 * Math.sqrt(avgProtoVar / this.#maxVariancePerDim));
                        const moveAmount = strength * Math.sqrt(this.#maxVariancePerDim * 0.08);
                        this.#vectorAdd(p.mean, direction, 1, moveAmount, p.mean);

                        for (let j = 0; j < hidden; j++) {
                            const proj = direction[j] * direction[j];
                            p.variance[j] += proj * moveAmount * moveAmount * 4.0;
                            p.variance[j] = Math.min(p.variance[j], this.#maxVariancePerDim);
                        }
                        p.projNorms = this.#computeProjNorms(p.mean);
                        noveltyFactor += closeness * 0.4;
                        noveltyFactor = Math.min(1.2, noveltyFactor);
                    }
                }

                const newMean = new Float32Array(p.mean);
                const newVar = new Float32Array(hidden);
                for (let j = 0; j < hidden; j++) {
                    newVar[j] = p.variance[j];
                }

                let extraNoveltyImportance = 0;
                if (bestIdx !== -1 && sem[bestIdx].isCore && bestSim > dynamicThreshold * 0.7) {
                    extraNoveltyImportance = Math.round(20 + 80 * this.#protoCapacityFactor) * (bestSim - dynamicThreshold * 0.7) / (1 - dynamicThreshold * 0.7);
                }

                const addedProto = {
                    mean: newMean,
                    variance: newVar,
                    size: p.size * effectiveBoost * (1 + 1.5 * noveltyFactor),
                    accessCount: p.size * 2.0 * (1 + 0.8 * noveltyFactor),
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: Math.round(20 + 60 * this.#protoCapacityFactor) * noveltyFactor + extraNoveltyImportance
                };
                sem.push(addedProto);
                this.#insertProtoToLSH(transformerIdx, addedProto);
            }
        }

        if (sem.length > maxP * this.#tempOverloadFactor) {
            sem.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
            const oldLen = sem.length;
            const trimLen = Math.round(maxP * this.#mergeTrimFactor);
            const toRemove = sem.slice(trimLen, oldLen);
            for (const proto of toRemove) {
                this.#removeProtoFromLSH(transformerIdx, proto);
            }
            sem.length = trimLen;

            const overloadFactor = sem.length / maxP;
            if (overloadFactor > 1.0 && sem.length > 0) {
                const accessValues = sem.map(p => p.accessCount || 0);
                accessValues.sort((a, b) => a - b);

                const pruneFraction = 0.05 + 0.10 * (overloadFactor - 1.0);
                const pruneIdx = Math.floor(accessValues.length * pruneFraction);
                let dynamicThreshold = pruneIdx < accessValues.length ? accessValues[pruneIdx] : 0;

                dynamicThreshold = Math.max(dynamicThreshold, 2.0);

                const toRemovePrune = [];
                for (const proto of sem) {
                    if (!proto.isCore && (proto.accessCount || 0) < dynamicThreshold) {
                        toRemovePrune.push(proto);
                    }
                }
                for (const proto of toRemovePrune) {
                    this.#removeProtoFromLSH(transformerIdx, proto);
                }
                sem = sem.filter(p => p.isCore || (p.accessCount || 0) >= dynamicThreshold);
            }
        }

        const minAvgVar = 0.08 + 0.50 * (1 - perf) + 0.40 * (1 - agreement);
        const inflationFactor = 3.0 + 5.0 * (1 - perf);
        const noiseBase = 0.06 + 0.20 * (1 - perf);
        for (const proto of sem) {
            let varSum = 0;
            for (let j = 0; j < hidden; j++) {
                varSum += proto.variance[j];
            }
            const avgVar = varSum / hidden;
            if (avgVar < minAvgVar && !proto.isCore) {
                const deficit = minAvgVar - avgVar;
                const noiseScale = noiseBase + 0.6 * deficit;
                for (let j = 0; j < hidden; j++) {
                    proto.mean[j] += this.#randomNormal(0, noiseScale * 0.8);
                    proto.variance[j] *= inflationFactor;
                    proto.variance[j] = Math.min(proto.variance[j], this.#maxVariancePerDim);
                }
                proto.projNorms = this.#computeProjNorms(proto.mean);
                this.#updateProtoInLSH(transformerIdx, proto);
            }
        }

        const numPriority = Math.min(Math.round(this.#priorityMax * this.#tempOverloadFactor), sem.length);
        if (numPriority > 0) {
            const indexed = sem.map((proto, i) => ({i, util: this.#computeProtoUtility(proto)}));
            indexed.sort((a, b) => b.util - a.util);
            this.#priorityIndices[transformerIdx] = indexed.slice(0, numPriority).map(o => o.i);
        } else {
            this.#priorityIndices[transformerIdx] = [];
        }

        this.#normalizeSemantic(transformerIdx, sem);
        this.#semanticProtos[transformerIdx] = sem;
    }

    #kernelSimilarity (protoA, protoB) {
        if (!protoA.mean || !protoB.mean || protoA.mean.length !== this.#hiddenSize ||
            !protoA.variance || !protoB.variance || protoA.variance.length !== this.#hiddenSize) {
            return 0;
        }

        let mahalSum = 0;
        let sumLnPooled = 0;
        let sumLnA = 0;
        let sumLnB = 0;
        const eps = 1e-6;

        for (let j = 0; j < this.#hiddenSize; j++) {
            const diff = protoA.mean[j] - protoB.mean[j];
            const varA = Math.max(protoA.variance[j], eps);
            const varB = Math.max(protoB.variance[j], eps);
            const pooled = (varA + varB) / 2 + eps;

            mahalSum += diff * diff / pooled;
            sumLnPooled += Math.log(pooled);
            sumLnA += Math.log(varA);
            sumLnB += Math.log(varB);
        }

        const avgMahal = mahalSum / this.#hiddenSize;
        const quadraticDivisor = 6 + 4 * this.#protoCapacityFactor;
        const quadratic = avgMahal / quadraticDivisor;

        const avgLnPooled = sumLnPooled / this.#hiddenSize;
        const avgLnA = sumLnA / this.#hiddenSize;
        const avgLnB = sumLnB / this.#hiddenSize;
        const detTerm = 0.5 * (avgLnPooled - 0.5 * (avgLnA + avgLnB));

        let D = quadratic + detTerm;
        D = Math.max(0, D);

        return Math.exp(-this.#kernelGamma * D);
    }

    #retrieveTopRelevantProtos (transformerIdx, currentProtos, maxRetrieve = this.#maxRetrievedProtos) {
        if (currentProtos.length === 0) return [];

        currentProtos.forEach(cp => {
            if (!cp.projNorms || cp.projNorms.length !== this.#numProjections) {
                cp.projNorms = this.#computeProjNorms(cp.mean);
            }
        });

        const queryMean = this.#weightedMean(currentProtos);
        const totalSizeQuery = currentProtos.reduce((s, p) => s + (p.size || 0), 0) || 1;
        const queryVariance = new Float32Array(this.#hiddenSize);
        for (const cp of currentProtos) {
            const weight = (cp.size || 1) / totalSizeQuery;
            for (let j = 0; j < this.#hiddenSize; j++) {
                const diff = cp.mean[j] - queryMean[j];
                queryVariance[j] += weight * (cp.variance[j] + diff * diff);
            }
        }
        for (let j = 0; j < this.#hiddenSize; j++) {
            queryVariance[j] = Math.max(queryVariance[j], 1e-6);
        }
        const queryProjs = this.#computeProjNorms(queryMean);

        const avgProj = Array.from({length: this.#numProjections}, () => new Float32Array(this.#lowDim).fill(0));
        const numCurrent = currentProtos.length;
        for (const cp of currentProtos) {
            for (let np = 0; np < this.#numProjections; np++) {
                const a = avgProj[np];
                const c = cp.projNorms[np];
                for (let l = 0; l < this.#lowDim; l++) {
                    a[l] += c[l] / numCurrent;
                }
            }
        }

        const attMem = this.#attentionMemory[transformerIdx];
        const episodicToConsider = attMem.length > this.#maxEpisodicConsider 
            ? attMem.slice(-this.#maxEpisodicConsider) 
            : attMem;
        const episodicLen = episodicToConsider.length;

        const adaptMem = this.#adaptiveContext[transformerIdx];
        const adaptLen = adaptMem.length;

        const numEntries = episodicLen + adaptLen;

        const perf = this.#performanceScores[transformerIdx] ?? 0.5;
        const agreement = this.#agreementScores[transformerIdx] ?? 0.5;
        const overconfidence = perf * agreement;

        let explorationRate = 0.35 + 0.65 * (1 - perf) + 0.45 * (1 - agreement) + 0.8 * overconfidence;
        const stagnation = this.#isStagnating(transformerIdx);
        const dropBoost = this.#detectSuddenDrop(transformerIdx);
        if (stagnation) {
            explorationRate = Math.min(1.0, explorationRate + 0.7);
        }

        const avgProtoVar = this.#getAvgProtoVariance(transformerIdx);
        const lowProtoVariance = avgProtoVar < this.#maxVariancePerDim * 0.12;
        if (lowProtoVariance) {
            explorationRate = Math.min(1.0, explorationRate + 0.8);
        }

        let candProtos = [];
        let candIsSem = [];

        const basePerEntry = Math.max(this.#longTermMaxProtos, this.#shortTermMaxProtos / 2);
        const maxPerMemoryEntry = Math.round(basePerEntry * (1 + explorationRate));

        if (numEntries > 0) {
            const entryScores = new Float32Array(numEntries);

            for (let e = 0; e < episodicLen; e++) {
                const entry = episodicToConsider[e];
                if (entry && entry.repProj && entry.repProj.length === this.#numProjections) {
                    entryScores[e] = this.#projSimilarity(avgProj, entry.repProj);
                }
            }

            for (let a = 0; a < adaptLen; a++) {
                const entry = adaptMem[a];
                if (entry && entry.repProj && entry.repProj.length === this.#numProjections) {
                    entryScores[episodicLen + a] = this.#projSimilarity(avgProj, entry.repProj);
                }
            }

            for (let e = 0; e < episodicLen; e++) {
                const age = episodicLen - 1 - e;
                entryScores[e] += 0.8 * Math.exp(-0.01 * age);
            }
            for (let a = 0; a < adaptLen; a++) {
                const age = adaptLen - 1 - a;
                entryScores[episodicLen + a] += 0.7 * Math.exp(-0.01 * age);
            }

            const entryIndices = Array.from({length: numEntries}, (_, i) => i);
            entryIndices.sort((a, b) => entryScores[b] - entryScores[a]);

            const numEpCand = Math.min(Math.round(this.#numRetrievalCandidates * 1.2 * (1 + 2.5 * explorationRate)), numEntries);

            for (let ii = 0; ii < numEpCand && ii < entryIndices.length; ii++) {
                const eIdx = entryIndices[ii];
                let protos = [];
                if (eIdx < episodicLen) {
                    const entry = episodicToConsider[eIdx];
                    if (entry && entry.protos && entry.protos.length > 0) protos = entry.protos;
                } else {
                    const aIdx = eIdx - episodicLen;
                    const entry = adaptMem[aIdx];
                    if (entry && entry.protos && entry.protos.length > 0) protos = entry.protos;
                }

                if (protos.length > 0) {
                    const sortedProtos = protos.slice();
                    sortedProtos.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
                    const takeNum = Math.min(Math.round(maxPerMemoryEntry * 2), sortedProtos.length);
                    for (let pp = 0; pp < takeNum; pp++) {
                        const p = sortedProtos[pp];
                        if (!p.projNorms || p.projNorms.length !== this.#numProjections) {
                            p.projNorms = this.#computeProjNorms(p.mean);
                        }
                        candProtos.push(p);
                        candIsSem.push(false);
                    }
                }
            }
        }

        const coreEp = this.#coreEpisodic[transformerIdx];
        if (coreEp.length > 0) {
            for (const entry of coreEp) {
                if (entry.protos && entry.protos.length > 0) {
                    const sortedProtos = entry.protos.slice();
                    sortedProtos.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
                    const takeNum = Math.min(maxPerMemoryEntry * 8, sortedProtos.length);
                    for (let pp = 0; pp < takeNum; pp++) {
                        const p = sortedProtos[pp];
                        if (!p.projNorms || p.projNorms.length !== this.#numProjections) {
                            p.projNorms = this.#computeProjNorms(p.mean);
                        }
                        candProtos.push(p);
                        candIsSem.push(false);
                    }
                }
            }
        }

        let protectedProtos = [];
        const semProtos = this.#semanticProtos[transformerIdx];
        semProtos.forEach(p => { if (p.isCore) protectedProtos.push(p); });
        coreEp.forEach(entry => { if (entry.protos) protectedProtos.push(...entry.protos); });

        if (protectedProtos.length > 0) {
            protectedProtos.forEach(p => {
                if (!p.projNorms || p.projNorms.length !== this.#numProjections) {
                    p.projNorms = this.#computeProjNorms(p.mean);
                }
            });

            const protectedSims = protectedProtos.map(p => this.#kernelSimilarity(
                { mean: queryMean, variance: queryVariance },
                p
            ));

            const protectedIndices = Array.from({length: protectedProtos.length}, (_, i) => i);
            protectedIndices.sort((a, b) => protectedSims[b] - protectedSims[a]);

            const numProtected = Math.min(Math.round(this.#maxRetrievedProtos * 0.35 * (1 + explorationRate)), protectedProtos.length);
            for (let k = 0; k < numProtected; k++) {
                const p = protectedProtos[protectedIndices[k]];
                candProtos.push(p);
                candIsSem.push(true);
            }
        }

        const numSem = semProtos.length;
        let semCandProtos = [];
        if (numSem > 0) {
            semProtos.forEach(p => {
                if (!p.projNorms || p.projNorms.length !== this.#numProjections) {
                    p.projNorms = this.#computeProjNorms(p.mean);
                }
            });

            const semCandidates = new Set();

            const hashesPerSet = new Array(this.#numLshSets);
            for (let s = 0; s < this.#numLshSets; s++) {
                hashesPerSet[s] = this.#computeLSHHashesLow(queryProjs[s], this.#lshHyperplanes[s]);
            }

            const baseProbes = Math.round(this.#numRetrievalCandidates * explorationRate * 0.8);
            const scaledAdditive = Math.round(this.#baseProtoCapacity * (4 + 4 * this.#memoryFactor));

            let numProbes = baseProbes + scaledAdditive;

            const stagnationAdd = Math.round(this.#baseProtoCapacity * (8 + 16 * this.#memoryFactor) * (stagnation ? 1 : 0));
            const dropAdd = Math.round(this.#baseProtoCapacity * (7 + 14 * this.#memoryFactor) * (dropBoost > 1.5 ? 1 : 0));
            const lowPerfAdd = Math.round(this.#baseProtoCapacity * (6 + 12 * this.#memoryFactor) * (perf < 0.6 ? 1 : 0));
            let lowVarAdd = 0;
            if (lowProtoVariance) lowVarAdd = Math.round(this.#baseProtoCapacity * (24 + 48 * this.#memoryFactor));

            numProbes += stagnationAdd + dropAdd + lowPerfAdd + lowVarAdd;

            const maxCandidateCap = Math.round(this.#effectiveSemanticMax * (6 + 4 * this.#memoryFactor));

            for (let s = 0; s < this.#numLshSets; s++) {
                const hashes = hashesPerSet[s];
                const tableArray = this.#semanticLSHBuckets[transformerIdx][s];
                for (let t = 0; t < this.#lshNumTables; t++) {
                    let key = hashes[t];
                    let bucket = tableArray[t].get(key);
                    if (bucket) for (const proto of bucket) semCandidates.add(proto);

                    for (let b = 0; b < this.#lshHashBits; b++) {
                        const flipped = key ^ (1n << BigInt(b));
                        bucket = tableArray[t].get(flipped);
                        if (bucket) for (const proto of bucket) semCandidates.add(proto);
                    }

                    const flipsPerLevel = [2, 3, 4, Math.round(this.#lshHashBits * 0.08), Math.round(this.#lshHashBits * 0.15)];
                    const probesPerLevel = Math.round(numProbes / flipsPerLevel.length);
                    for (let level = 0; level < flipsPerLevel.length && semCandidates.size < maxCandidateCap; level++) {
                        const flipBits = flipsPerLevel[level];
                        for (let pr = 0; pr < probesPerLevel && semCandidates.size < maxCandidateCap; pr++) {
                            let flippedHash = key;
                            for (let f = 0; f < flipBits; f++) {
                                const bit = Math.floor(Math.random() * this.#lshHashBits);
                                flippedHash ^= (1n << BigInt(bit));
                            }
                            bucket = tableArray[t].get(flippedHash);
                            if (bucket) for (const proto of bucket) semCandidates.add(proto);
                        }
                    }
                }
            }

            semCandProtos = Array.from(semCandidates);

            let desiredSem = Math.round(this.#numRetrievalCandidates * 0.7 * (1 + 4.0 * explorationRate));
            if (stagnation) desiredSem = Math.round(desiredSem * 2.2);
            if (dropBoost > 1.5) desiredSem = Math.round(desiredSem * 1.8);
            if (lowProtoVariance) desiredSem = Math.round(desiredSem * 3.0);

            const lowThresh = Math.round(desiredSem * 0.6);
            if (semCandProtos.length < lowThresh) {
                let extraProbes = Math.round(this.#baseProtoCapacity * (8 + 24 * explorationRate) * (stagnation ? 2 : 1) * (dropBoost > 1.5 ? 1.8 : 1));
                if (lowProtoVariance) extraProbes *= 4;

                const farFlipFrac = 0.09 + 0.15 * explorationRate + 0.10 * (stagnation ? 1 : 0);

                for (let ex = 0; ex < extraProbes; ex++) {
                    if (semCandProtos.length >= maxCandidateCap) break;
                    const ss = Math.floor(Math.random() * this.#numLshSets);
                    const tt = Math.floor(Math.random() * this.#lshNumTables);
                    let baseKey = hashesPerSet[ss][tt];
                    let numFlips = Math.max(4, Math.round(this.#lshHashBits * farFlipFrac));
                    let flipped = baseKey;
                    for (let ff = 0; ff < numFlips; ff++) {
                        const bit = Math.floor(Math.random() * this.#lshHashBits);
                        flipped ^= (1n << BigInt(bit));
                    }
                    const bucket = this.#semanticLSHBuckets[transformerIdx][ss][tt].get(flipped);
                    if (bucket) for (const proto of bucket) semCandidates.add(proto);
                }
                semCandProtos = Array.from(semCandidates);
            }

            if (semCandProtos.length < desiredSem * 0.3) {
                const projScores = new Float32Array(numSem);
                for (let i = 0; i < numSem; i++) {
                    projScores[i] = this.#projSimilarity(queryProjs, semProtos[i].projNorms);
                }
                const projIndices = Array.from({length: numSem}, (_, i) => i);
                projIndices.sort((a, b) => projScores[b] - projScores[a]);
                const need = Math.min(numSem, Math.round(desiredSem * (3 + 2 * explorationRate) - semCandProtos.length));
                for (let k = 0; k < need; k++) {
                    const proto = semProtos[projIndices[k]];
                    if (!semCandidates.has(proto)) {
                        semCandProtos.push(proto);
                    }
                }
            }

            if (semCandProtos.length < desiredSem * 0.2) {
                const lowAccess = semProtos.map((p, i) => ({p, acc: p.accessCount || 0}));
                lowAccess.sort((a, b) => a.acc - b.acc);
                const need = Math.min(lowAccess.length, Math.round(desiredSem * (1.5 + explorationRate) - semCandProtos.length));
                for (let k = 0; k < need; k++) {
                    const proto = lowAccess[k].p;
                    if (!semCandidates.has(proto)) {
                        semCandProtos.push(proto);
                    }
                }
            }

            let targetTotal = Math.round(desiredSem * (2.5 + explorationRate));
            if (stagnation || explorationRate > 0.6) {
                targetTotal = Math.round(targetTotal * 2.0);
            }
            while (semCandProtos.length < targetTotal && semCandProtos.length < numSem * 0.95) {
                const randProto = semProtos[Math.floor(Math.random() * numSem)];
                if (!semCandidates.has(randProto)) {
                    semCandProtos.push(randProto);
                }
            }

            semProtos.forEach(p => {
                if (p.isCore && !semCandidates.has(p)) semCandProtos.push(p);
            });
            const priorityProtos = this.#priorityIndices[transformerIdx].map(i => semProtos[i]);
            priorityProtos.forEach(p => {
                if (!semCandidates.has(p)) semCandProtos.push(p);
            });
        }

        candProtos.push(...semCandProtos);
        candIsSem.push(...semCandProtos.map(() => true));

        const totalCandLimit = Math.round(this.#numRetrievalCandidates * (1 + 5.0 * explorationRate));
        if (candProtos.length > totalCandLimit) {
            const approxScores = candProtos.map(p => this.#projSimilarity(queryProjs, p.projNorms));
            const sortedIdx = Array.from({length: candProtos.length}, (_, i) => i);
            sortedIdx.sort((a, b) => approxScores[b] - approxScores[a]);
            const newCandProtos = [];
            const newCandIsSem = [];
            for (let t = 0; t < totalCandLimit; t++) {
                const oldi = sortedIdx[t];
                newCandProtos.push(candProtos[oldi]);
                newCandIsSem.push(candIsSem[oldi]);
            }
            candProtos = newCandProtos;
            candIsSem = newCandIsSem;
        }

        const numCandProtos = candProtos.length;
        if (numCandProtos === 0) return [];

        const baseThresh = 0.55;
        let threshAdjust = 0.60 * explorationRate;

        const semScale = this.#effectiveSemanticMax;
        if (numSem < semScale * 1.5) threshAdjust += 0.55;
        else if (numSem < semScale * 3) threshAdjust += 0.35;

        if (stagnation) threshAdjust += 0.45;

        let threshold = Math.max(-0.6, baseThresh - threshAdjust);

        const candidatesForExact = [];
        for (let i = 0; i < numCandProtos; i++) {
            const proto = candProtos[i];
            const approxSim = this.#projSimilarity(queryProjs, proto.projNorms);
            if (approxSim > threshold || proto.isCore) {
                candidatesForExact.push({i, approxSim});
            }
        }
        candidatesForExact.sort((a, b) => b.approxSim - a.approxSim);

        let maxExactCompute = Math.round(this.#effectiveSemanticMax * (8 + 6 * explorationRate) * this.#memoryFactor);
        if (stagnation) maxExactCompute = Math.round(maxExactCompute * 1.8);

        candidatesForExact.length = Math.min(maxExactCompute, candidatesForExact.length);

        const protoScores = new Float32Array(numCandProtos).fill(-100);

        for (const {i} of candidatesForExact) {
            const proto = candProtos[i];
            const sim = this.#kernelSimilarity(
                { mean: queryMean, variance: queryVariance },
                proto
            );

            let score = sim * 1.4;

            score += candIsSem[i] ? 4.0 : 0;

            const coreBoostFactor = 20 + 30 * this.#protoCapacityFactor;
            if (proto.isCore) score += coreBoostFactor;

            score += 0.4 * Math.log(1 + proto.size / 8);

            const accessBonus = explorationRate / Math.sqrt(proto.accessCount + 1);
            score += accessBonus * 12.0;

            let varSum = 0;
            for (let j = 0; j < this.#hiddenSize; j++) {
                varSum += proto.variance[j];
            }
            const avgVar = varSum / this.#hiddenSize;
            const varBoost = Math.tanh(Math.sqrt(avgVar + 1e-6) * 2.5);
            score += explorationRate * 3.5 * varBoost;

            protoScores[i] = score;
        }

        const protoIndices = Array.from({length: numCandProtos}, (_, i) => i);
        protoIndices.sort((a, b) => protoScores[b] - protoScores[a]);

        const diversityAdjust = Math.tanh(numCandProtos / 80.0);
        const baseThreshDiv = 0.45;
        let DIVERSITY_THRESHOLD = baseThreshDiv - 0.55 * explorationRate - 0.4 * diversityAdjust;
        if (stagnation) DIVERSITY_THRESHOLD -= 0.45;
        if (dropBoost > 1.5) DIVERSITY_THRESHOLD -= 0.25;
        DIVERSITY_THRESHOLD = Math.max(0.01, Math.min(0.75, DIVERSITY_THRESHOLD));

        let effectiveMaxRetrieve = Math.round(maxRetrieve * (1 + 3.0 * explorationRate + 2.0 * this.#memoryFactor));
        if (stagnation) effectiveMaxRetrieve = Math.round(effectiveMaxRetrieve * 1.6);

        const topNum = Math.min(effectiveMaxRetrieve, numCandProtos);

        const selectedProtoIndices = [];
        let pi = 0;
        while (selectedProtoIndices.length < topNum && pi < numCandProtos) {
            const candIdx = protoIndices[pi];
            const pProjs = candProtos[candIdx].projNorms;

            let maxSimToSelected = -1;
            for (const selIdx of selectedProtoIndices) {
                const sProjs = candProtos[selIdx].projNorms;
                const sim = this.#projSimilarity(pProjs, sProjs);
                if (sim > maxSimToSelected) maxSimToSelected = sim;
            }

            let effectiveThresh = candProtos[candIdx].isCore ? DIVERSITY_THRESHOLD + 0.5 : DIVERSITY_THRESHOLD;

            const diversityBypassMin = Math.max(20, Math.round(this.#maxRetrievedProtos * 0.2 * this.#memoryFactor));
            if (maxSimToSelected < effectiveThresh || selectedProtoIndices.length < diversityBypassMin) {
                selectedProtoIndices.push(candIdx);
                candProtos[candIdx].accessCount += 6.0;
            }
            pi++;
        }

        const selectedProtos = selectedProtoIndices.map(idx => candProtos[idx]);

        return selectedProtos;
    }

    #consolidateSemanticProtos (transformerIdx) {
        let sem = this.#semanticProtos[transformerIdx];

        const minForConsolidate = Math.max(5, Math.round(this.#baseProtoCapacity * 0.2));
        if (sem.length < minForConsolidate) {
            return;
        }

        const n = sem.length;
        const hidden = this.#hiddenSize;

        const perf = this.#performanceScores[transformerIdx] ?? 0.5;
        const agreement = this.#agreementScores[transformerIdx] ?? 0.5;
        const overload = sem.length / this.#effectiveSemanticMax;
        const mergePressure = Math.max(0, overload - 0.15) + (perf > 0.7 ? (perf - 0.7) * 3.0 : 0);
        let mergeKernelThresh = (0.73 + 0.15 * this.#protoCapacityFactor) - 0.40 * mergePressure - 0.25 * (1 - agreement);
        mergeKernelThresh = Math.max(0.40, Math.min(0.85, mergeKernelThresh));

        const stagnation = this.#isStagnating(transformerIdx);
        const drop = this.#detectSuddenDrop(transformerIdx) > 1.2;

        if (stagnation) mergeKernelThresh += 0.22;
        if (drop) mergeKernelThresh += 0.18;
        mergeKernelThresh = Math.min(0.92, mergeKernelThresh);

        const topPreCandidates = Math.min(Math.round(this.#effectiveSemanticMax * 1.05), Math.round(n * 0.6));

        for (let i = 0; i < n; i++) {
            if (!sem[i].projNorms || sem[i].projNorms.length !== this.#numProjections) sem[i].projNorms = this.#computeProjNorms(sem[i].mean);
        }

        const pairs = [];
        for (let i = 0; i < n; i++) {
            if (sem[i].isCore) continue;

            const piProjs = sem[i].projNorms;

            const approxScores = new Float32Array(n);
            approxScores.fill(-10);

            for (let j = 0; j < n; j++) {
                if (j === i) continue;
                approxScores[j] = this.#projSimilarity(piProjs, sem[j].projNorms);
            }

            const indices = Array.from({length: n}, (_, k) => k);
            indices.sort((a, b) => approxScores[b] - approxScores[a]);

            const topIdx = indices.slice(0, topPreCandidates);

            for (const j of topIdx) {
                if (i >= j) continue;
                if (sem[j].isCore) continue;

                const sim = this.#kernelSimilarity(sem[i], sem[j]);
                if (sim > mergeKernelThresh) {
                    pairs.push([i, j, sim]);
                }
            }
        }

        if (pairs.length > 0) {
            pairs.sort((a, b) => b[2] - a[2]);

            const merged = new Set();
            for (const pair of pairs) {
                let i = pair[0];
                let j = pair[1];
                if (merged.has(i) || merged.has(j)) continue;

                let keepIdx = sem[i].size >= sem[j].size ? i : j;
                let removeIdx = keepIdx === i ? j : i;

                if (this.#computeProtoUtility(sem[removeIdx]) > this.#computeProtoUtility(sem[keepIdx])) {
                    [keepIdx, removeIdx] = [removeIdx, keepIdx];
                }

                const keep = sem[keepIdx];
                const remove = sem[removeIdx];
                const oldMean = new Float32Array(keep.mean);
                const keepSize = keep.size;
                const removeSize = remove.size;
                const totalSize = keepSize + removeSize;

                const newMean = this.#vectorAdd(oldMean, remove.mean, keepSize / totalSize, removeSize / totalSize);
                keep.mean.set(newMean);

                keep.size = totalSize;
                keep.accessCount += remove.accessCount * 1.8;
                keep.importance = (keep.importance || 0) + (remove.importance || 0) * 0.95;

                let deltaSqSum = 0;
                const deltaVec = this.#vectorSub(newMean, oldMean);
                deltaSqSum = this.#vectorDot(deltaVec, deltaVec);

                const diff1 = this.#vectorSub(oldMean, newMean);
                const diff2 = this.#vectorSub(remove.mean, newMean);
                for (let k = 0; k < hidden; k++) {
                    keep.variance[k] = (keepSize * (keep.variance[k] + diff1[k] * diff1[k]) +
                                       removeSize * (remove.variance[k] + diff2[k] * diff2[k])) / totalSize * 1.4;
                    keep.variance[k] = Math.max(keep.variance[k], 1e-6);
                }
                keep.importance += 3.0 * deltaSqSum / hidden;
                keep.projNorms = this.#computeProjNorms(keep.mean);
                this.#updateProtoInLSH(transformerIdx, keep);

                this.#removeProtoFromLSH(transformerIdx, remove);

                merged.add(removeIdx);
            }

            const sortedMerged = Array.from(merged).sort((a, b) => b - a);
            for (const idx of sortedMerged) {
                sem.splice(idx, 1);
            }
        }

        let varThresh = 1.0 + 4.0 * (1 - perf) * this.#memoryFactor + 2.0 * (1 - agreement) * this.#protoCapacityFactor;
        const baseMinSize = Math.round(this.#baseProtoCapacity * 0.25);
        let minSizeForSplit = baseMinSize - Math.round((6 + 4 * (1 - this.#protoCapacityFactor)) * perf);
        minSizeForSplit = Math.max(4, minSizeForSplit);
        if (stagnation || drop) {
            varThresh -= 5.0 * this.#memoryFactor;
            minSizeForSplit = Math.max(3, minSizeForSplit - Math.round((6 + 4 * this.#protoCapacityFactor) * this.#memoryFactor));
        }

        if (sem.length < this.#effectiveSemanticMax * 1.4) {
            for (let i = 0; i < sem.length; i++) {
                const p = sem[i];
                if (p.size < minSizeForSplit || p.isCore) continue;

                let varSum = 0;
                for (let j = 0; j < hidden; j++) {
                    varSum += p.variance[j];
                }
                const avgVar = varSum / hidden;
                if (avgVar <= varThresh) continue;

                let maxVar = 0;
                let splitDim = 0;
                for (let j = 0; j < hidden; j++) {
                    if (p.variance[j] > maxVar) {
                        maxVar = p.variance[j];
                        splitDim = j;
                    }
                }

                const baseScale = Math.sqrt(Math.max(maxVar, 1e-6)) * 1.5;
                const perfScaleAdjust = 1 + 6.0 * (1 - perf) + 3.0 * (1 - agreement);
                let splitScale = baseScale * perfScaleAdjust;
                if (stagnation || drop) splitScale *= 3.0;

                const originalMean = new Float32Array(p.mean);
                const oldImportance = p.importance || 0;
                const oldSize = p.size;
                const oldAccess = p.accessCount;
                const halfSize = Math.floor(oldSize / 2);
                const size1 = oldSize - halfSize;
                const size2 = halfSize;

                p.mean[splitDim] -= splitScale;
                p.size = size1;
                p.accessCount = oldAccess * (size1 / oldSize);
                p.variance[splitDim] = Math.min(p.variance[splitDim] + splitScale ** 2 * 3.0, this.#maxVariancePerDim);
                p.importance = oldImportance * 0.5;
                p.projNorms = this.#computeProjNorms(p.mean);
                this.#updateProtoInLSH(transformerIdx, p);

                const newMean = new Float32Array(originalMean);
                newMean[splitDim] += splitScale;

                const newVariance = new Float32Array(p.variance);
                newVariance[splitDim] = Math.min(newVariance[splitDim] + splitScale ** 2 * 3.0, this.#maxVariancePerDim);

                const newProto = {
                    mean: newMean,
                    variance: newVariance,
                    size: size2,
                    accessCount: oldAccess * (size2 / oldSize),
                    projNorms: this.#computeProjNorms(newMean),
                    isCore: false,
                    importance: oldImportance * 0.5
                };

                sem.push(newProto);
                this.#insertProtoToLSH(transformerIdx, newProto);

                if (sem.length >= this.#effectiveSemanticMax * this.#mergeTrimFactor) break;
            }
        }

        if (sem.length > this.#effectiveSemanticMax * this.#tempOverloadFactor) {
            sem.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
            const oldLen = sem.length;
            const trimLen = Math.round(this.#effectiveSemanticMax * this.#mergeTrimFactor);
            const toRemove = sem.slice(trimLen, oldLen);
            for (const proto of toRemove) {
                this.#removeProtoFromLSH(transformerIdx, proto);
            }
            sem.length = trimLen;

            const overloadFactor = sem.length / this.#effectiveSemanticMax;
            if (overloadFactor > 1.0 && sem.length > 0) {
                const accessValues = sem.map(p => p.accessCount || 0);
                accessValues.sort((a, b) => a - b);

                const pruneFraction = 0.05 + 0.10 * (overloadFactor - 1.0);
                const pruneIdx = Math.floor(accessValues.length * pruneFraction);
                let dynamicThreshold = pruneIdx < accessValues.length ? accessValues[pruneIdx] : 0;

                dynamicThreshold = Math.max(dynamicThreshold, 2.0);

                const toRemovePrune = [];
                for (const proto of sem) {
                    if (!proto.isCore && (proto.accessCount || 0) < dynamicThreshold) {
                        toRemovePrune.push(proto);
                    }
                }
                for (const proto of toRemovePrune) {
                    this.#removeProtoFromLSH(transformerIdx, proto);
                }
                sem = sem.filter(p => p.isCore || (p.accessCount || 0) >= dynamicThreshold);
            }
        }

        if (sem.length > 0) {
            sem.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));

            let numCores = Math.min(this.#coreMaxProtos * 2, sem.length);
            if (stagnation || drop) {
                numCores = Math.min(Math.round(this.#coreMaxProtos * 4), sem.length);
            }
            sem.forEach(p => p.isCore = false);
            if (numCores > 0) {
                const candidateLimit = Math.min(sem.length, numCores * (6 + 4 * this.#protoCapacityFactor));
                const candidates = sem.slice(0, candidateLimit);

                const coreProtos = [];
                coreProtos.push(candidates[0]);
                candidates[0].isCore = true;

                for (let c = 1; c < numCores; c++) {
                    let bestIdx = -1;
                    let maxMinDist = -1;
                    for (let i = 0; i < candidates.length; i++) {
                        const cand = candidates[i];
                        if (coreProtos.includes(cand)) continue;

                        let maxSimToCore = -Infinity;
                        for (const coreP of coreProtos) {
                            const sim = this.#projSimilarity(cand.projNorms, coreP.projNorms);
                            if (sim > maxSimToCore) maxSimToCore = sim;
                        }
                        const dist = 1 - maxSimToCore;

                        if (dist > maxMinDist || (dist === maxMinDist && Math.random() < 0.5)) {
                            maxMinDist = dist;
                            bestIdx = i;
                        }
                    }
                    if (bestIdx !== -1) {
                        const selected = candidates[bestIdx];
                        coreProtos.push(selected);
                        selected.isCore = true;
                    } else {
                        break;
                    }
                }

                coreProtos.forEach(p => {
                    const logBonus = Math.round(10 + 20 * this.#protoCapacityFactor) * Math.log(1 + p.size);
                    const cappedLogBonus = Math.min(logBonus, Math.round(40 + 80 * this.#protoCapacityFactor));
                    p.importance = (p.importance || 0) + Math.round(30 + 70 * this.#protoCapacityFactor) + cappedLogBonus;
                });
            }

            if (sem.length > this.#effectiveSemanticMax) {
                const oldLen = sem.length;
                const toRemove = sem.slice(this.#effectiveSemanticMax, oldLen);
                for (const proto of toRemove) {
                    this.#removeProtoFromLSH(transformerIdx, proto);
                }
                sem.length = this.#effectiveSemanticMax;
            }
        }

        const numPriority = Math.min(Math.round(this.#priorityMax * this.#tempOverloadFactor), sem.length);
        if (numPriority > 0) {
            const indexed = sem.map((proto, i) => ({i, util: this.#computeProtoUtility(proto)}));
            indexed.sort((a, b) => b.util - a.util);
            this.#priorityIndices[transformerIdx] = indexed.slice(0, numPriority).map(o => o.i);
        } else {
            this.#priorityIndices[transformerIdx] = [];
        }

        this.#normalizeSemantic(transformerIdx, sem);
        this.#semanticProtos[transformerIdx] = sem;
    }

    #computeMemoryScoreFromProtos (protos, attentionScores, transformerIdx, entryIndex, memoryList, ignoreRecency = false) {
        if (!Array.isArray(protos) || protos.length === 0) return 0;

        const hidden = this.#hiddenSize;
        const totalSize = protos.reduce((sum, p) => sum + p.size, 0) || 1;
        const rep = this.#weightedMean(protos);

        let sqSumApprox = 0;
        let pooledVarSum = 0;
        for (let j = 0; j < hidden; j++) {
            let weightedSq = 0;
            let varJ = 0;
            for (const p of protos) {
                const m = p.mean[j];
                const v = Math.max(p.variance[j], 1e-6);
                const diff = m - rep[j];
                varJ += p.size * (v + diff * diff);
                weightedSq += p.size * m * m;
            }
            pooledVarSum += varJ / totalSize;
            sqSumApprox += weightedSq;
        }
        const magnitudeScore = Math.sqrt(sqSumApprox);
        const varianceScore = Math.tanh(Math.sqrt(Math.max(0, pooledVarSum / hidden)));

        let clusterDiversityScore = 0;
        if (protos.length > 1) {
            let clusterEnt = 0;
            for (const p of protos) {
                const prob = p.size / totalSize;
                if (prob > 1e-6) clusterEnt -= prob * Math.log(prob + 1e-12);
            }
            clusterDiversityScore = clusterEnt / Math.log(protos.length + 1);
        }

        let activeDims = 0;
        for (let j = 0; j < hidden; j++) {
            let maxAbs = 0;
            for (const p of protos) {
                maxAbs = Math.max(maxAbs, Math.abs(p.mean[j]));
            }
            if (maxAbs > 1e-3) activeDims++;
        }
        const sparsity = activeDims / hidden;
        const sparsityScore = 1 - Math.abs(sparsity - 0.5) * 2;

        let attentionSharpness = 0;
        if (attentionScores) {
            let totalEntropy = 0, queryCount = 0;
            for (let h = 0; h < attentionScores.length; h++) {
                for (let i = 0; i < attentionScores[h].length; i++) {
                    let entropy = 0, sumP = 0;
                    const headRow = attentionScores[h][i];
                    for (let jj = 0; jj < headRow.length; jj++) {
                        const prob = headRow[jj];
                        if (prob > 0) {
                            entropy -= prob * Math.log(prob + 1e-12);
                            sumP += prob;
                        }
                    }
                    if (sumP > 0.1) {
                        totalEntropy += entropy;
                        queryCount++;
                    }
                }
            }
            const avgEntropy = queryCount > 0 ? totalEntropy / queryCount : Math.log(this.#inputSize);
            attentionSharpness = Math.exp(-avgEntropy / Math.log(this.#inputSize));
        }

        let totalAccess = 0;
        for (const p of protos) totalAccess += p.accessCount;

        const accessScore = Math.tanh(totalAccess / (protos.length * 50.0));

        const perf = Math.max(0, Math.min(1, this.#performanceScores[transformerIdx] || 0.5));
        const specScore = Math.min(Math.max(this.#specializationScores[transformerIdx] || 0.5, 0), 1);
        const confidenceBoost = 0.25 * perf * specScore;

        let uniqueness = 1.0;
        if (!ignoreRecency && memoryList.length > 1) {
            const centroidRep = new Float32Array(hidden);
            let totalCentSize = 0;
            const maxSampled = Math.min(Math.round(this.#contextWindow * 0.15), memoryList.length);
            const indicesToUse = [];
            if (memoryList.length > maxSampled) {
                for (let i = 0; i < maxSampled; i++) {
                    indicesToUse.push(Math.floor(i * memoryList.length / maxSampled));
                }
            } else {
                for (let i = 0; i < memoryList.length; i++) indicesToUse.push(i);
            }
            const filtered = indicesToUse.filter(idx => idx !== entryIndex);
            for (const idx of filtered) {
                const mProtos = memoryList[idx].protos;
                for (const pr of mProtos) {
                    for (let j = 0; j < hidden; j++) {
                        centroidRep[j] += pr.mean[j] * pr.size;
                    }
                    totalCentSize += pr.size;
                }
            }
            if (totalCentSize > 0) {
                for (let j = 0; j < hidden; j++) centroidRep[j] /= totalCentSize;
                uniqueness = 1 - this.#cosineSimilarity(rep, centroidRep);
                uniqueness = Math.max(0, uniqueness);
            }
        }

        let baseScore =
            0.25 * varianceScore +
            0.15 * clusterDiversityScore +
            0.15 * sparsityScore +
            0.15 * Math.tanh(magnitudeScore / Math.sqrt(totalSize * hidden)) +
            0.10 * attentionSharpness +
            0.15 * accessScore;

        baseScore = baseScore * (1 + confidenceBoost) * Math.pow(uniqueness + 0.5, 1.5);

        let diversityFactor = 1.0;
        if (!ignoreRecency && memoryList.length > 5) {
            const minRecentForDiversity = Math.max(Math.round(3 * this.#memoryFactor), Math.round(memoryList.length * 0.15));
            const recentCount = Math.max(minRecentForDiversity, Math.floor(memoryList.length * 0.2));
            let distSum = 0;
            let rCount = 0;
            for (let r = 1; r <= recentCount; r++) {
                const recentIndex = memoryList.length - r;
                if (recentIndex < 0 || recentIndex === entryIndex) continue;
                const recentProtos = memoryList[recentIndex].protos;
                const overlapSim = this.#maxPairwiseKernel(protos, recentProtos);
                distSum += 1 - overlapSim;
                rCount++;
            }
            if (rCount > 0) diversityFactor = 1 + (distSum / rCount);
        }

        const age = ignoreRecency ? 0 : memoryList.length - 1 - entryIndex;
        const recencyFactor = Math.exp(-0.03 * age) * (1 + 1.0 / (1 + age / 5));

        return baseScore * diversityFactor * recencyFactor;
    }

    #pruneMemory (transformerIdx, contextWindow, latestAttentionScores) {
        const memory = this.#attentionMemory[transformerIdx];
        if (memory.length <= contextWindow) return;

        const numToKeep = contextWindow;
        const baseRecent = Math.round(this.#baseProtoCapacity * 0.4);
        const scaledRecent = Math.round(baseRecent * this.#memoryFactor);
        const numForcedRecent = Math.min(scaledRecent, Math.floor(numToKeep * 0.6));
        const latestIndex = memory.length - 1;
        let forcedStart = Math.max(0, latestIndex - numForcedRecent + 1);

        const numEntries = memory.length;
        const scores = new Float32Array(numEntries);
        for (let i = 0; i < numEntries; i++) {
            const attScores = (i === latestIndex) ? latestAttentionScores : null;
            scores[i] = this.#computeMemoryScoreFromProtos(memory[i].protos, attScores, transformerIdx, i, memory);
        }

        const olderIndices = [];
        for (let i = 0; i < forcedStart; i++) {
            olderIndices.push(i);
        }
        olderIndices.sort((a, b) => scores[b] - scores[a]);

        const actualForcedCount = latestIndex - forcedStart + 1;
        const numFromOlder = Math.max(0, numToKeep - actualForcedCount);

        const selectedIndices = [];
        for (let i = forcedStart; i <= latestIndex; i++) {
            selectedIndices.push(i);
        }
        for (let k = 0; k < numFromOlder && k < olderIndices.length; k++) {
            selectedIndices.push(olderIndices[k]);
        }

        selectedIndices.sort((a, b) => a - b);
        this.#attentionMemory[transformerIdx] = selectedIndices.map(idx => memory[idx]);

        const perf = this.#performanceScores[transformerIdx] ?? 0.5;
        const allIndices = Array.from({length: memory.length}, (_, i) => i);
        const discardedIndices = allIndices.filter(idx => !selectedIndices.includes(idx));

        if (discardedIndices.length > 0) {
            const discardWithScore = discardedIndices.map(idx => ({
                idx,
                score: this.#computeMemoryScoreFromProtos(memory[idx].protos, null, transformerIdx, idx, memory, true)
            }));
            discardWithScore.sort((a, b) => b.score - a.score);

            const numPromote = Math.min(Math.round(this.#baseProtoCapacity * (0.25 + 0.75 * (1 - perf))), discardWithScore.length);

            const promotedEntries = [];
            for (let k = 0; k < numPromote; k++) {
                const { idx } = discardWithScore[k];
                const entry = memory[idx];
                const newProtos = entry.protos.map(p => ({
                    mean: new Float32Array(p.mean),
                    variance: new Float32Array(p.variance),
                    size: p.size * 1.5,
                    accessCount: p.accessCount + p.size * Math.round(6 + 18 * this.#protoCapacityFactor),
                    projNorms: p.projNorms ? p.projNorms.map(arr => new Float32Array(arr)) : this.#computeProjNorms(p.mean),
                    isCore: true,
                    importance: (p.importance || 0) + Math.round(15 + 35 * this.#protoCapacityFactor)
                }));
                const repMean = entry.repMean ? new Float32Array(entry.repMean) : this.#weightedMean(entry.protos);
                promotedEntries.push({
                    protos: newProtos,
                    repMean,
                    repProj: this.#computeProjNorms(repMean)
                });
            }

            this.#coreEpisodic[transformerIdx].push(...promotedEntries);

            if (this.#coreEpisodic[transformerIdx].length > this.#coreEpisodicMaxEntries) {
                const coreWithScore = this.#coreEpisodic[transformerIdx].map(entry => ({
                    entry,
                    score: this.#computeMemoryScoreFromProtos(entry.protos, null, transformerIdx, -1, [], true)
                }));
                coreWithScore.sort((a, b) => b.score - a.score);
                this.#coreEpisodic[transformerIdx] = coreWithScore.slice(0, this.#coreEpisodicMaxEntries).map(item => item.entry);
            }
        }
    }

    #updateMemoryBanks (finalOutput, attentionScores, transformerIdx, training, layerNum) {
        const specScore = this.#specializationScores[transformerIdx] || 0.5;

        const perf = Math.max(0, Math.min(1, this.#performanceScores[transformerIdx] || 0.5));
        const threshMultiplier = 0.75 + 0.5 * perf;
        const maxAddMultiplier = 1.0 + 1.0 * (1 - perf);
        const mergeThreshold = 0.7 + 0.2 * perf * this.#protoCapacityFactor;

        const numLayersEffective = Math.max(1, this.#numLayers - 1);
        const layerFraction = layerNum / numLayersEffective;
        const layerDepthFactor = 1 - layerFraction;
        const earlyBias = 1.5;
        const lateFloor = 0.4;
        const noiseMultiplier = earlyBias * layerDepthFactor + lateFloor * (1 - layerDepthFactor);

        let outputToAdaptive = finalOutput;
        if (training) {
            const confidence = perf * (0.7 + 0.3 * specScore);
            const baseMaxNoiseScale = 0.03 + 0.3 * (1 - confidence);
            const maxNoiseScale = baseMaxNoiseScale * noiseMultiplier;

            outputToAdaptive = finalOutput.map(tokenVec => {
                return tokenVec.map((val, j) => val + (Math.random() - 0.5) * maxNoiseScale * 
                    (1 + (this.#specializationWeights[transformerIdx][j % this.#hiddenSize][j % this.#hiddenSize] || 1)));
            });
        } else {
            const baseScale = 0.01 + 0.04 * (1 - perf);
            const scaledBase = baseScale * noiseMultiplier;
            const timePhase = this.#attentionMemory[transformerIdx].length * 0.05;

            outputToAdaptive = finalOutput.map(tokenVec =>
                tokenVec.map((val, j) => val + scaledBase * Math.sin(j * 0.12 + timePhase))
            );
        }

        const dynamicShortThreshFactor = 4.0 * threshMultiplier;
        const dynamicShortMaxAdd = Math.round(this.#shortTermMaxProtos * maxAddMultiplier);
        const shortProtos = this.#poolMultiPrototype(outputToAdaptive, dynamicShortMaxAdd, dynamicShortThreshFactor);

        if (shortProtos.length > Math.round(this.#shortTermMaxProtos * this.#tempOverloadFactor)) {
            shortProtos.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
            shortProtos.length = Math.round(this.#shortTermMaxProtos * this.#tempOverloadFactor);
        }

        if (shortProtos.length > 0) {
            const shortRepMean = this.#weightedMean(shortProtos);
            const shortRepProj = this.#computeProjNorms(shortRepMean);
            const shortTotalSize = shortProtos.reduce((s, p) => s + p.size, 0) || 1;

            const adaptMem = this.#adaptiveContext[transformerIdx];
            if (adaptMem.length > 0) {
                const last = adaptMem[adaptMem.length - 1];
                if (!last.repMean) last.repMean = this.#weightedMean(last.protos);
                const lastTotalSize = last.protos.reduce((s, p) => s + p.size, 0) || 1;
                const sim = this.#projSimilarity(last.repProj, shortRepProj);

                if (sim > mergeThreshold) {
                    const combinedTotal = lastTotalSize + shortTotalSize;
                    const combinedMean = new Float32Array(this.#hiddenSize);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        combinedMean[j] = (last.repMean[j] * lastTotalSize + shortRepMean[j] * shortTotalSize) / combinedTotal;
                    }
                    last.repMean = combinedMean;
                    last.repProj = this.#computeProjNorms(combinedMean);

                    for (const p of shortProtos) {
                        last.protos.push({
                            mean: new Float32Array(p.mean),
                            variance: new Float32Array(p.variance),
                            size: p.size,
                            accessCount: p.accessCount + 1,
                            projNorms: p.projNorms.map(arr => new Float32Array(arr))
                        });
                    }

                    const maxPerShortEntry = Math.round(this.#shortTermMaxProtos * this.#mergeTrimFactor);
                    if (last.protos.length > maxPerShortEntry) {
                        last.protos.sort((a, b) => (b.size * b.accessCount) - (a.size * b.accessCount));
                        last.protos = last.protos.slice(0, maxPerShortEntry);
                        last.repMean = this.#weightedMean(last.protos);
                        last.repProj = this.#computeProjNorms(last.repMean);
                    }
                } else {
                    adaptMem.push({
                        protos: shortProtos,
                        repMean: shortRepMean,
                        repProj: shortRepProj
                    });
                }
            } else {
                adaptMem.push({
                    protos: shortProtos,
                    repMean: shortRepMean,
                    repProj: shortRepProj
                });
            }

            if (adaptMem.length > this.#adaptiveWindow) {
                adaptMem.shift();
            }
        }

        if (layerNum + 1 === this.#numLayers) {
            const dynamicLongThreshFactor = 4.0 * threshMultiplier;
            const dynamicLongMaxAdd = Math.round(this.#longTermMaxProtos * maxAddMultiplier);
            const longProtos = this.#poolMultiPrototype(finalOutput, dynamicLongMaxAdd, dynamicLongThreshFactor);

            if (longProtos.length > Math.round(this.#longTermMaxProtos * this.#tempOverloadFactor)) {
                longProtos.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
                longProtos.length = Math.round(this.#longTermMaxProtos * this.#tempOverloadFactor);
            }

            if (longProtos.length > 0) {
                const longRepMean = this.#weightedMean(longProtos);
                const longRepProj = this.#computeProjNorms(longRepMean);
                const longTotalSize = longProtos.reduce((s, p) => s + p.size, 0) || 1;

                const attMem = this.#attentionMemory[transformerIdx];
                if (attMem.length > 0) {
                    const last = attMem[attMem.length - 1];
                    if (!last.repMean) last.repMean = this.#weightedMean(last.protos);
                    const lastTotalSize = last.protos.reduce((s, p) => s + p.size, 0) || 1;
                    const sim = this.#projSimilarity(last.repProj, longRepProj);

                    if (sim > mergeThreshold) {
                        const combinedTotal = lastTotalSize + longTotalSize;
                        const combinedMean = new Float32Array(this.#hiddenSize);
                        for (let j = 0; j < this.#hiddenSize; j++) {
                            combinedMean[j] = (last.repMean[j] * lastTotalSize + longRepMean[j] * longTotalSize) / combinedTotal;
                        }
                        last.repMean = combinedMean;
                        last.repProj = this.#computeProjNorms(combinedMean);

                        for (const p of longProtos) {
                            last.protos.push({
                                mean: new Float32Array(p.mean),
                                variance: new Float32Array(p.variance),
                                size: p.size,
                                accessCount: p.accessCount + 1,
                                projNorms: p.projNorms.map(arr => new Float32Array(arr))
                            });
                        }

                        const maxPerLongEntry = Math.round(this.#longTermMaxProtos * this.#mergeTrimFactor);
                        if (last.protos.length > maxPerLongEntry) {
                            last.protos.sort((a, b) => (b.size * b.accessCount) - (a.size * b.accessCount));
                            last.protos = last.protos.slice(0, maxPerLongEntry);
                            last.repMean = this.#weightedMean(last.protos);
                            last.repProj = this.#computeProjNorms(last.repMean);
                        }
                    } else {
                        attMem.push({
                            protos: longProtos,
                            repMean: longRepMean,
                            repProj: longRepProj
                        });
                    }
                } else {
                    attMem.push({
                        protos: longProtos,
                        repMean: longRepMean,
                        repProj: longRepProj
                    });
                }

                this.#pruneMemory(transformerIdx, this.#contextWindow, attentionScores);
            }

            const episodicDecay = 0.95 + 0.04 * perf;

            const attMem = this.#attentionMemory[transformerIdx];
            for (let i = 0; i < attMem.length - 1; i++) {
                attMem[i].protos.forEach(p => p.accessCount *= episodicDecay);
            }

            const adaptMem = this.#adaptiveContext[transformerIdx];
            for (let i = 0; i < adaptMem.length - 1; i++) {
                adaptMem[i].protos.forEach(p => p.accessCount *= episodicDecay);
            }

            const coreEp = this.#coreEpisodic[transformerIdx];
            for (const entry of coreEp) {
                entry.protos.forEach(p => p.accessCount *= 0.99);
            }

            if (longProtos.length > 0) {
                this.#updateSemanticProtos(transformerIdx, longProtos);
            }

            this.#generativeReplay(transformerIdx);

            if (this.#trainingStepCount % this.#semanticMergeEvery === 0 && training) {
                this.#consolidateSemanticProtos(transformerIdx);
                this.#replayOldMemory(transformerIdx);
            }

            const dropBoost = this.#detectSuddenDrop(transformerIdx);
            if (dropBoost > 1.5 && Math.random() < 0.4) {
                this.#replayOldMemory(transformerIdx);
                this.#generativeReplay(transformerIdx);
            }
        }
    }

    #multiHeadAttention (x, layerNum, layer, transformerIdx, training = true) {
        if (
            !Array.isArray(x) ||
            x.length !== this.#inputSize ||
            !x.every(row => Array.isArray(row) && row.length === this.#hiddenSize) ||
            !layer || !layer.Wq || !layer.Wk || !layer.Wv || !layer.Wo
        ) {
            return {
                output: Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                preWoOutput: Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                Q: Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                K: Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                V: Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0)),
                scores: Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(this.#inputSize).fill(0))),
                probs: Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(this.#inputSize).fill(0)))
            };
        }

        const headSize = this.#hiddenSize / this.#numHeads;

        const Q = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        const K = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        const V = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));

        const specScore = this.#specializationScores[transformerIdx] || 0.5;

        for (let i = 0; i < this.#inputSize; i++) {
            for (let j = 0; j < this.#hiddenSize; j++) {
                let qSum = 0;
                let kSum = 0;
                let vSum = 0;
                for (let kk = 0; kk < this.#hiddenSize; kk++) {
                    const specWeight = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][kk % this.#hiddenSize][j], 0.5), 1.5);
                    const xVal = x[i][kk];
                    qSum += xVal * layer.Wq[kk][j] * specWeight;
                    kSum += xVal * layer.Wk[kk][j] * specWeight;
                    vSum += xVal * layer.Wv[kk][j] * specWeight;
                }
                Q[i][j] = qSum;
                K[i][j] = kSum;
                V[i][j] = vSum;
            }
        }

        this.#applyRoPE(Q);
        this.#applyRoPE(K);

        const attentionScores = Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(this.#inputSize).fill(0)));
        const attentionProbs = Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(this.#inputSize).fill(0)));

        for (let h = 0; h < this.#numHeads; h++) {
            const offset = h * headSize;
            for (let i = 0; i < this.#inputSize; i++) {
                for (let j = 0; j < this.#inputSize; j++) {
                    let sum = 0;
                    for (let kk = 0; kk < headSize; kk++) {
                        sum += Q[i][offset + kk] * K[j][offset + kk];
                    }
                    attentionScores[h][i][j] = sum / Math.sqrt(headSize);
                }
                attentionProbs[h][i] = this.#softmax(attentionScores[h][i]);
            }
        }

        const preWoOutput = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        for (let h = 0; h < this.#numHeads; h++) {
            const offset = h * headSize;
            for (let i = 0; i < this.#inputSize; i++) {
                for (let j = 0; j < this.#inputSize; j++) {
                    const p = attentionProbs[h][i][j];
                    for (let kk = 0; kk < headSize; kk++) {
                        preWoOutput[i][offset + kk] += p * V[j][offset + kk];
                    }
                }
            }
        }

        const finalOutput = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        for (let i = 0; i < this.#inputSize; i++) {
            for (let j = 0; j < this.#hiddenSize; j++) {
                let sum = 0;
                for (let kk = 0; kk < this.#hiddenSize; kk++) {
                    const specWeight = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][kk % this.#hiddenSize][j], 0.5), 1.5);
                    sum += preWoOutput[i][kk] * layer.Wo[kk][j] * specWeight;
                }
                finalOutput[i][j] = sum;
            }
        }

        this.#updateMemoryBanks(finalOutput, attentionScores, transformerIdx, training, layerNum);

        return {
            output: finalOutput,
            preWoOutput,
            Q,
            K,
            V,
            scores: attentionScores,
            probs: attentionProbs
        };
    }

    #feedForward (x, layer, transformerIdx) {
        if (
            !Array.isArray(x) || x.length !== this.#hiddenSize ||
            !layer || !layer.gate_proj || !layer.up_proj || !layer.down_proj
        ) {
            return Array(this.#hiddenSize).fill(0);
        }

        const specScore = this.#specializationScores[transformerIdx] || 0.5;
        const gate = new Float32Array(this.#feedForwardSize);
        const up = new Float32Array(this.#feedForwardSize);

        for (let j = 0; j < this.#feedForwardSize; j++) {
            let gSum = 0;
            let uSum = 0;
            for (let i = 0; i < this.#hiddenSize; i++) {
                const specWeight = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][i % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5);
                const xVal = x[i];
                gSum += xVal * layer.gate_proj[i][j] * specWeight;
                uSum += xVal * layer.up_proj[i][j] * specWeight;
            }
            gate[j] = gSum;
            up[j] = uSum;
        }

        const activated = new Float32Array(this.#feedForwardSize);
        for (let j = 0; j < this.#feedForwardSize; j++) {
            activated[j] = this.#silu(gate[j]) * up[j];
        }

        const output = new Float32Array(this.#hiddenSize);
        for (let j = 0; j < this.#hiddenSize; j++) {
            let sum = 0;
            for (let i = 0; i < this.#feedForwardSize; i++) {
                const specWeight = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][j % this.#hiddenSize][i % this.#hiddenSize], 0.5), 1.5);
                sum += activated[i] * layer.down_proj[i][j] * specWeight;
            }
            output[j] = sum;
        }

        return Array.from(output);
    }

    #contextAwareAttention (inputs, transformerIdx) {
        if (
            !Array.isArray(inputs) ||
            inputs.length !== this.#inputSize
        ) {
            return Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        }

        const perf = Math.max(0, Math.min(1, this.#performanceScores[transformerIdx] || 0.5));
        const specScore = this.#specializationScores[transformerIdx] || 0.5;

        const inputProjection = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        for (let i = 0; i < this.#inputSize; i++) {
            for (let j = 0; j < this.#hiddenSize; j++) {
                const specWeight = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][i % this.#hiddenSize][j], 0.5), 1.5);
                inputProjection[i][j] = inputs[i] * specWeight * (1 + this.#swarmIntelligenceFactor);
            }
        }

        const rawComponent = inputProjection.map(row => row.slice());

        const threshMultiplier = 0.75 + 0.5 * perf;
        const dynamicRawThreshFactor = 4.0 * threshMultiplier;
        const maxAddMultiplier = 1.0 + 1.0 * (1 - perf);
        const dynamicRawMaxAdd = Math.round(this.#rawMaxProtos * maxAddMultiplier);

        const currentProtos = this.#poolMultiPrototype(rawComponent, dynamicRawMaxAdd, dynamicRawThreshFactor);

        if (currentProtos.length > Math.round(this.#rawMaxProtos * this.#tempOverloadFactor)) {
            currentProtos.sort((a, b) => this.#computeProtoUtility(b) - this.#computeProtoUtility(a));
            currentProtos.length = Math.round(this.#rawMaxProtos * this.#tempOverloadFactor);
        }

        if (currentProtos.length === 0) {
            return rawComponent;
        }

        const longTermMemoryLength = this.#attentionMemory[transformerIdx].length;
        const shortTermMemoryLength = this.#adaptiveContext[transformerIdx].length;
        const fillFactor = (longTermMemoryLength / this.#contextWindow + shortTermMemoryLength / this.#adaptiveWindow) / 2;

        const dynamicRetrieve = Math.round(this.#maxRetrievedProtos * (1 + fillFactor));

        const selectedProtos = this.#retrieveTopRelevantProtos(transformerIdx, currentProtos, dynamicRetrieve);

        if (selectedProtos.length === 0) {
            return rawComponent;
        }

        const protoMeans = selectedProtos.map(p => p.mean.slice());
        const protoSizes = selectedProtos.map(p => p.size);

        const pastLen = protoMeans.length;

        const transformer = this.#transformers[transformerIdx];
        let { avgWq, avgWk, avgWv, avgWo } = transformer.cachedAvgWeights || this.#cacheAverageWeights(transformerIdx);

        const headSize = this.#hiddenSize / this.#numHeads;

        const K = Array(pastLen).fill().map(() => Array(this.#hiddenSize).fill(0));
        const V = Array(pastLen).fill().map(() => Array(this.#hiddenSize).fill(0));

        for (let pj = 0; pj < pastLen; pj++) {
            const mean = protoMeans[pj];
            for (let j = 0; j < this.#hiddenSize; j++) {
                let sumK = 0;
                let sumV = 0;
                for (let kk = 0; kk < this.#hiddenSize; kk++) {
                    const specW = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][kk][j], 0.5), 1.5);
                    sumK += mean[kk] * avgWk[kk][j] * specW;
                    sumV += mean[kk] * avgWv[kk][j] * specW;
                }
                K[pj][j] = sumK;
                V[pj][j] = sumV;
            }
        }

        this.#applyRoPE(K, 0);

        let globalDiversity = 0;
        if (pastLen > 8) {
            const numSamples = Math.min(16, pastLen);
            const sampleIndices = Array.from({length: numSamples}, () => Math.floor(Math.random() * pastLen));
            const centroid = Array(this.#hiddenSize).fill(0);
            for (const si of sampleIndices) {
                for (let j = 0; j < this.#hiddenSize; j++) {
                    centroid[j] += protoMeans[si][j];
                }
            }
            for (let j = 0; j < this.#hiddenSize; j++) centroid[j] /= numSamples;

            for (const si of sampleIndices) {
                globalDiversity += 1 - this.#cosineSimilarity(protoMeans[si], centroid);
            }
            globalDiversity /= numSamples;
        }
        const diversityScore = Math.tanh(globalDiversity * 6);

        let augmented = rawComponent.map(row => row.slice());
        const baseHops = Math.round(2 + (2 + 3 * this.#memoryFactor) * fillFactor);
        const maxHops = Math.max(baseHops, Math.round(selectedProtos.length * 0.5 * this.#memoryFactor));

        for (let hop = 0; hop < maxHops; hop++) {
            const Q = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));

            for (let seqi = 0; seqi < this.#inputSize; seqi++) {
                for (let j = 0; j < this.#hiddenSize; j++) {
                    let sum = 0;
                    for (let kk = 0; kk < this.#hiddenSize; kk++) {
                        const specW = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][kk][j], 0.5), 1.5);
                        sum += augmented[seqi][kk] * avgWq[kk][j] * specW;
                    }
                    Q[seqi][j] = sum;
                }
            }

            this.#applyRoPE(Q, pastLen);

            let totalEntropy = 0;
            let queryCount = 0;

            const preWoOutput = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));

            for (let h = 0; h < this.#numHeads; h++) {
                const offset = h * headSize;
                for (let i = 0; i < this.#inputSize; i++) {
                    let maxScore = -Infinity;
                    const scores = Array(pastLen).fill(0);
                    for (let pj = 0; pj < pastLen; pj++) {
                        let sum = 0;
                        for (let kk = 0; kk < headSize; kk++) {
                            sum += Q[i][offset + kk] * K[pj][offset + kk];
                        }
                        scores[pj] = sum / Math.sqrt(headSize);
                        if (scores[pj] > maxScore) maxScore = scores[pj];
                    }

                    const expScores = scores.map(s => Math.exp(s - maxScore));
                    const sumExp = expScores.reduce((a, b) => a + b, 0) || 1;

                    let headEntropy = 0;
                    let sumP = 0;
                    for (let pj = 0; pj < pastLen; pj++) {
                        const p = expScores[pj] / sumExp;
                        if (p > 1e-6) {
                            headEntropy -= p * Math.log(p);
                            sumP += p;
                        }
                        if (p > 0) {
                            for (let kk = 0; kk < headSize; kk++) {
                                preWoOutput[i][offset + kk] += p * V[pj][offset + kk] * protoSizes[pj];
                            }
                        }
                    }

                    if (sumP > 0.1) {
                        totalEntropy += headEntropy;
                        queryCount++;
                    }
                }
            }

            const logPast = Math.log(pastLen + 1);
            const avgEntropy = queryCount > 0 ? totalEntropy / queryCount : logPast;
            const attentionSharpness = Math.exp(-avgEntropy / logPast);

            const gate = (3.0 + this.#memoryFactor) * attentionSharpness + (1.5 + this.#memoryFactor) * fillFactor +
                (1.5 + 0.5 * this.#protoCapacityFactor) * perf + (1.5 + this.#memoryFactor) * diversityScore;
            let retrievedRatio = 1 / (1 + Math.exp(-gate));

            const retrieved = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
            for (let i = 0; i < this.#inputSize; i++) {
                for (let j = 0; j < this.#hiddenSize; j++) {
                    let sum = 0;
                    for (let kk = 0; kk < this.#hiddenSize; kk++) {
                        const specW = Math.min(Math.max(1 + specScore * this.#specializationWeights[transformerIdx][kk][j], 0.5), 1.5);
                        sum += preWoOutput[i][kk] * avgWo[kk][j] * specW;
                    }
                    retrieved[i][j] = sum;
                }
            }

            for (let i = 0; i < this.#inputSize; i++) {
                for (let j = 0; j < this.#hiddenSize; j++) {
                    augmented[i][j] = augmented[i][j] * (1 - retrievedRatio) + retrieved[i][j] * retrievedRatio;
                }
            }

            if (hop >= 2 && attentionSharpness > 0.65 + 0.25 * perf) {
                break;
            }
            if (hop >= 3 && retrievedRatio < 0.05) {
                break;
            }
            if (hop >= 5 && retrievedRatio < 0.1) {
                break;
            }
        }

        return augmented;
    }

    #cacheAverageWeights (transformerIdx) {
        const transformer = this.#transformers[transformerIdx];
        const numLayers = this.#numLayers;

        const avgWq = Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        const avgWk = Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        const avgWv = Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0));
        const avgWo = Array(this.#hiddenSize).fill().map(() => Array(this.#hiddenSize).fill(0));

        const scale = 1.0 / numLayers;
        for (let l = 0; l < numLayers; l++) {
            const att = transformer.attentionWeights[l];
            for (let ii = 0; ii < this.#hiddenSize; ii++) {
                for (let jj = 0; jj < this.#hiddenSize; jj++) {
                    avgWq[ii][jj] += att.Wq[ii][jj] * scale;
                    avgWk[ii][jj] += att.Wk[ii][jj] * scale;
                    avgWv[ii][jj] += att.Wv[ii][jj] * scale;
                    avgWo[ii][jj] += att.Wo[ii][jj] * scale;
                }
            }
        }

        transformer.cachedAvgWeights = { avgWq, avgWk, avgWv, avgWo };
        return transformer.cachedAvgWeights;
    }

    #processTransformer (inputs, idx, computeIntermediates = false, training = true, isLast = false) {
        const transformer = this.#transformers[idx];

        let x = this.#contextAwareAttention(inputs, idx);

        const layerOutputs = computeIntermediates ? [x] : [];
        const activations = computeIntermediates ? [] : [];
        const attentionIntermediates = computeIntermediates ? [] : [];

        for (let layer = 0; layer < this.#numLayers; layer++) {
            const normX = x.map(row => this.#rmsNorm(row, transformer.layerNormWeights[layer].gamma1));

            const attentionResult = this.#multiHeadAttention(normX, layer, transformer.attentionWeights[layer], idx, training);
            const attentionOutput = attentionResult.output;

            const attentionResidual = x.map((row, i) => row.map((val, j) => val + attentionOutput[i][j]));

            const normAttention = attentionResidual.map(row => this.#rmsNorm(row, transformer.layerNormWeights[layer].gamma2));

            const ffnOutputs = normAttention.map(row => this.#feedForward(row, transformer.ffnWeights[layer], idx));

            x = attentionResidual.map((row, i) => row.map((val, j) => val + ffnOutputs[i][j]));

            if (computeIntermediates) {
                layerOutputs.push(x);
                activations.push({ normX, attentionOutput, normAttention });
                attentionIntermediates.push({
                    Q: attentionResult.Q,
                    K: attentionResult.K,
                    V: attentionResult.V,
                    attentionScores: attentionResult.scores,
                    attentionProbs: attentionResult.probs,
                    preWoOutput: attentionResult.preWoOutput
                });
            }
        }

        let finalHidden = Array(this.#hiddenSize).fill(0);
        let validCount = 0;
        for (let pos = 0; pos < this.#inputSize; pos++) {
            let posHidden = x[pos];
            if (this.#numLayers > 0) {
                const lastLayerIdx = this.#numLayers - 1;
                posHidden = this.#rmsNorm(posHidden, transformer.layerNormWeights[lastLayerIdx].gamma2);
            }
            for (let j = 0; j < this.#hiddenSize; j++) {
                if (isValidNumber(posHidden[j])) {
                    finalHidden[j] += posHidden[j];
                }
            }
            validCount++;
        }
        if (validCount > 0) {
            finalHidden = finalHidden.map(v => v / validCount);
        }

        let output = Array(1).fill(0);
        for (let i = 0; i < this.#hiddenSize; i++) {
            output[0] += isValidNumber(finalHidden[i]) && isValidNumber(transformer.outputWeights[i][0])
                ? finalHidden[i] * transformer.outputWeights[i][0]
                : 0;
        }
        output[0] = isValidNumber(output[0]) && isValidNumber(transformer.outputBias[0])
            ? output[0] + transformer.outputBias[0]
            : output[0];

        if (isLast) this.#computeAttentionWeights(inputs);

        if (computeIntermediates) {
            return { output: output[0], layerOutputs, activations, attentionIntermediates };
        }
        return output[0];
    }

    #updateHiveState (inputs, target, intermediates, training, shouldUpdateMetrics, shouldReturn) {
        let probability;
        let outputs = [];
        const layerOutputs = [];
        const activations = [];
        const attentionIntermediates = [];

        if (intermediates) {
            this.#transformers.forEach((_, idx, arr) => {
                const result = this.#processTransformer(inputs, idx, true, training, idx === arr.length - 1);
                outputs[idx] = result.output;
                layerOutputs[idx] = result.layerOutputs;
                activations[idx] = result.activations;
                attentionIntermediates[idx] = result.attentionIntermediates;
            });
        } else {
            outputs = this.#transformers.map((_, idx, arr) => 
                this.#processTransformer(inputs, idx, false, training, idx === arr.length - 1)
            );
        }

        const finalOutput = this.#computeWeightedSum(outputs);
        probability = this.#sigmoid(finalOutput);

        if (shouldUpdateMetrics) this.#updateMetrics(inputs, outputs, target, probability);

        if (intermediates && shouldReturn) { 
            return { outputs, layerOutputs, activations, attentionIntermediates, probability }; 
        }

        if (!intermediates && shouldReturn) {
            return { outputs, probability };
        }
    }

    #hiveMemorySharing () {
        if (this.#ensembleSize < 2) return;

        const compositeScores = this.#performanceScores.map((perf, i) => {
            const agree = this.#agreementScores[i] ?? 0.5;
            const spec = this.#specializationScores[i] ?? 0.5;
            return 0.45 * perf + 0.25 * agree + 0.3 * spec;
        });

        const rankedIndices = Array.from({length: this.#ensembleSize}, (_, i) => i);
        rankedIndices.sort((a, b) => compositeScores[b] - compositeScores[a]);

        const minShare = Math.max(2, Math.round(this.#ensembleSize * 0.05));
        const numShare = Math.max(minShare, Math.round(this.#ensembleSize * (0.2 + 0.4 * this.#swarmIntelligenceFactor)));
        const donors = rankedIndices.slice(0, numShare);
        const receivers = rankedIndices.slice(-numShare);

        const hidden = this.#hiddenSize;
        const maxTransferPerReceiver = Math.round(this.#baseProtoCapacity * (0.5 + 2 * this.#swarmIntelligenceFactor));

        for (const recIdx of receivers) {
            const recPerf = this.#performanceScores[recIdx] ?? 0.5;
            const recAgree = this.#agreementScores[recIdx] ?? 0.5;
            const noiseScale = 0.04 + 0.08 * (1 - recPerf) + 0.04 * (1 - recAgree);

            let transferredProtos = [];

            for (const donIdx of donors) {
                let donProtos = this.#semanticProtos[donIdx];
                if (donProtos.length === 0) continue;

                const sorted = donProtos.slice().sort((a, b) => {
                    const utilA = this.#computeProtoUtility(a);
                    const utilB = this.#computeProtoUtility(b);
                    const impA = (a.importance || 0);
                    const impB = (b.importance || 0);
                    return (utilB + 15 * impB) - (utilA + 15 * impA);
                });

                const minFromDonor = Math.max(2, Math.round((2 + 3 * this.#protoCapacityFactor) * this.#memoryFactor));
                let numFromDonor = Math.max(minFromDonor, Math.round(sorted.length * (0.10 + 0.20 * this.#swarmIntelligenceFactor)));
                const topFraction = 0.6 + 0.3 * this.#protoCapacityFactor;
                const numTop = Math.ceil(numFromDonor * topFraction);
                const numRandom = numFromDonor - numTop;

                for (const p of sorted.slice(0, numTop)) {
                    const newMean = new Float32Array(p.mean);
                    for (let j = 0; j < hidden; j++) {
                        newMean[j] += (Math.random() - 0.5) * 2 * noiseScale;
                    }
                    transferredProtos.push({
                        mean: newMean,
                        variance: new Float32Array(p.variance),
                        size: p.size * (0.15 + 0.5 * recPerf),
                        accessCount: p.accessCount * 0.5,
                        projNorms: this.#computeProjNorms(newMean),
                        isCore: false,
                        importance: (p.importance || 0) * 0.4
                    });
                }

                const shuffled = donProtos.slice();
                for (let i = shuffled.length - 1; i > 0; i--) {
                    const j = Math.floor(Math.random() * (i + 1));
                    [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
                }
                for (let r = 0; r < numRandom && r < shuffled.length; r++) {
                    const p = shuffled[r];
                    const newMean = new Float32Array(p.mean);
                    const extraNoise = noiseScale * 1.5;
                    for (let j = 0; j < hidden; j++) {
                        newMean[j] += (Math.random() - 0.5) * 2 * extraNoise;
                    }
                    transferredProtos.push({
                        mean: newMean,
                        variance: new Float32Array(p.variance),
                        size: p.size * (0.10 + 0.3 * recPerf),
                        accessCount: p.accessCount * 0.3,
                        projNorms: this.#computeProjNorms(newMean),
                        isCore: false,
                        importance: (p.importance || 0) * 0.3
                    });
                }
            }

            if (transferredProtos.length > maxTransferPerReceiver) {
                transferredProtos.sort((a, b) => 
                    this.#computeProtoUtility(b) - this.#computeProtoUtility(a)
                );
                transferredProtos = transferredProtos.slice(0, maxTransferPerReceiver);
            }

            if (transferredProtos.length > 0) {
                this.#updateSemanticProtos(recIdx, transferredProtos);
            }
        }
    }

    #computeWeightedSum (outputs) {
        return outputs.reduce((sum, out, idx) => {
            if (isValidNumber(out) && isValidNumber(this.#ensembleWeights[idx])) {
                return sum + out * this.#ensembleWeights[idx];
            }
            return sum;
        }, 0);
    }

    #computeAttentionWeights (inputs) {
        if (
            !Array.isArray(inputs) ||
            inputs.length !== this.#inputSize ||
            !inputs.every(isValidNumber)
        ) {
            this.#ensembleWeights = Array(this.#ensembleSize).fill(1 / this.#ensembleSize);
            this.#normalizeEnsembleWeights();
            return;
        }

        const attentionScores = Array(this.#ensembleSize).fill(0);

        const maxConsiderChunks = this.#contextWindow;

        for (let t = 0; t < this.#ensembleSize; t++) {
            const memChunks = this.#attentionMemory[t];
            const memLen = memChunks.length;

            let considerLen = 0;
            let consideredChunks = [];

            if (memLen > 0) {
                considerLen = Math.min(maxConsiderChunks, memLen);
                const startIdx = memLen - considerLen;
                consideredChunks = memChunks.slice(startIdx);
            }

            if (considerLen === 0) {
                attentionScores[t] = 0;
                continue;
            }

            const pooledMemory = consideredChunks.map(chunkProtos => {
                if (!Array.isArray(chunkProtos) || chunkProtos.length === 0) {
                    return Array(this.#hiddenSize).fill(0);
                }
                return this.#weightedMean(chunkProtos);
            });

            const keys = pooledMemory.map(pool =>
                pool.map((v, k) =>
                    isValidNumber(v) && isValidNumber(this.#attentionWeightMatrix[t][k])
                        ? v * this.#attentionWeightMatrix[t][k]
                        : 0
                )
            );

            const innerSums = Array(considerLen).fill(0);
            for (let j = 0; j < considerLen; j++) {
                let sum = 0;
                for (let k = 0; k < this.#hiddenSize; k++) {
                    if (isValidNumber(this.#attentionWeightMatrix[t][k]) && isValidNumber(keys[j][k])) {
                        sum += this.#attentionWeightMatrix[t][k] * keys[j][k];
                    }
                }
                innerSums[j] = sum;
            }

            const biasSum = this.#attentionBias[t].reduce((s, val) => s + (isValidNumber(val) ? val : 0), 0);
            const avgBias = this.#hiddenSize > 0 ? biasSum / this.#hiddenSize : 0;

            let score = 0;
            const scale = 1 / Math.sqrt(this.#hiddenSize);

            for (let i = 0; i < this.#inputSize; i++) {
                if (!isValidNumber(inputs[i])) continue;

                const rowScores = innerSums.map(inner =>
                    inputs[i] * inner * scale + avgBias
                );

                const attentionWeights = this.#softmax(rowScores);

                for (let j = 0; j < considerLen; j++) {
                    if (!isValidNumber(attentionWeights[j])) continue;
                    for (let k = 0; k < this.#hiddenSize; k++) {
                        if (isValidNumber(keys[j][k])) {
                            score += attentionWeights[j] * keys[j][k];
                        }
                    }
                }
            }

            score /= this.#inputSize;

            const performanceScore = isValidNumber(this.#performanceScores[t]) ? this.#performanceScores[t] : 0.5;
            const specializationBoost = 1 + (isValidNumber(this.#specializationScores[t])
                ? this.#specializationScores[t] * this.#swarmIntelligenceFactor
                : 0);

            attentionScores[t] = score * (0.7 + 0.2 * performanceScore + 0.1 * specializationBoost);
        }

        const weights = this.#softmax(attentionScores);

        const finalWeights = weights.map((w, idx) => {
            const performanceScore = isValidNumber(this.#performanceScores[idx]) ? this.#performanceScores[idx] : 0.5;
            const specializationFactor = 1 + (isValidNumber(this.#specializationScores[idx])
                ? this.#specializationScores[idx] * this.#swarmIntelligenceFactor
                : 0);
            const weight = 0.6 * w + 0.3 * performanceScore + 0.1 * specializationFactor;
            return isValidNumber(weight) && weight >= 0 ? weight : 1 / this.#ensembleSize;
        });

        const sum = finalWeights.reduce((s, w) => s + (isValidNumber(w) && w >= 0 ? w : 0), 0);
        if (sum <= 1e-6) {
            this.#ensembleWeights = Array(this.#ensembleSize).fill(1 / this.#ensembleSize);
        } else {
            this.#ensembleWeights = finalWeights.map(w => (isValidNumber(w) && w >= 0 ? w / sum : 1 / this.#ensembleSize));
        }

        this.#normalizeEnsembleWeights();
    }

    #computeSpecializationScores (inputs, outputs) {
        if (
            !Array.isArray(inputs) ||
            inputs.length !== this.#inputSize ||
            !inputs.every(isValidNumber) ||
            !Array.isArray(outputs) ||
            outputs.length !== this.#ensembleSize ||
            !outputs.every(isValidNumber)
        ) {
            return;
        }

        const outputMean = outputs.reduce((sum, v) => sum + v, 0) / outputs.length;
        const outputStd = Math.sqrt(
            outputs.reduce((sum, v) => sum + (v - outputMean) ** 2, 0) / outputs.length
        ) || 1e-6;

        const zScores = outputs.map(out => (out - outputMean) / outputStd);

        this.#specializationScores = zScores.map((z, i) => {
            const performance = isValidNumber(this.#performanceScores[i]) ? this.#performanceScores[i] : 0.5;
            const magnitude = Math.abs(z);
            const bounded = 1 / (1 + Math.exp(-magnitude));
            return bounded * (0.5 + 0.5 * performance);
        });
    }

    #updatePerformanceScores (linearOutputs, target) {
        this.#performanceScores = this.#performanceScores.map((score, idx) => {
            const individualProbability = this.#sigmoid(linearOutputs[idx]);
            const brierScore = Math.pow(individualProbability - target, 2);
            const performance = 1 - brierScore;
            const newScore = 0.9 * score + 0.1 * (isValidNumber(performance) ? performance : 0);
            
            this.#historicalPerformance[idx].push(isValidNumber(newScore) ? newScore : 0);
            if (this.#historicalPerformance[idx].length > this.#maxPerformanceHistory) {
                this.#historicalPerformance[idx].shift();
            }
            
            return isValidNumber(newScore) ? newScore : 0;
        });
    }

    #updateAgreementScores (linearOutputs, finalProbability) {
        this.#agreementScores = this.#agreementScores.map((score, idx) => {
            const individualProbability = this.#sigmoid(linearOutputs[idx]);
            const absDiff = Math.abs(individualProbability - finalProbability);
            let agreement = 1 - absDiff;
            
            const avgHistoricalPerformance = this.#historicalPerformance[idx].length > 0
                ? this.#historicalPerformance[idx].reduce((sum, val) => sum + val, 0) / this.#historicalPerformance[idx].length
                : 0;
            const weightedAgreement = agreement * (0.5 + 0.5 * avgHistoricalPerformance);
            
            const newScore = 0.9 * score + 0.1 * (isValidNumber(weightedAgreement) ? weightedAgreement : 0);
            
            return isValidNumber(newScore) ? newScore : 0;
        });
    }

    #updateTrustScores () {
        const performanceMean = this.#performanceScores.reduce((sum, score) => sum + (isValidNumber(score) ? score : 0), 0) / this.#ensembleSize || 1;
        const performanceStd = Math.sqrt(
            this.#performanceScores.reduce((sum, score) => sum + ((isValidNumber(score) ? score : 0) - performanceMean) ** 2, 0) / this.#ensembleSize
        ) || 1;

        this.#trustScoresHistory = this.#trustScoresHistory.map((history, idx) => {
            const normalizedScore = isValidNumber(this.#performanceScores[idx]) && performanceStd > 0
                ? (this.#performanceScores[idx] - performanceMean) / performanceStd
                : 0;
            const agreementFactor = isValidNumber(this.#agreementScores[idx]) ? this.#agreementScores[idx] : 0.5;
            const historicalTrend = this.#historicalPerformance[idx].length > 1
                ? (this.#historicalPerformance[idx][this.#historicalPerformance[idx].length - 1] - this.#historicalPerformance[idx][0]) /
                this.#historicalPerformance[idx].length
                : 0;
            const specializationBoost = 1 + this.#specializationScores[idx] * this.#swarmIntelligenceFactor;
            const trustScore = this.#sigmoid(normalizedScore * (0.6 + 0.2 * agreementFactor + 0.1 * historicalTrend + 0.1 * specializationBoost));
            history.push(isValidNumber(trustScore) ? trustScore : 0.5);
            if (history.length > this.#maxTrustHistory) history.shift();
            return history;
        });
    }

    #adjustPerformanceScores () {
        const trustMomentum = 0.6;
        this.#performanceScores = this.#performanceScores.map((score, idx) => {
            const recentTrust = this.#trustScoresHistory[idx].slice(-5);
            const avgTrust = recentTrust.length > 0
                ? recentTrust.reduce((sum, val) => sum + (isValidNumber(val) ? val : 0), 0) / recentTrust.length
                : 0.5;
            const historicalWeight = this.#trustScoresHistory[idx].length / this.#maxTrustHistory;
            const specializationFactor = 1 + this.#specializationScores[idx] * this.#swarmIntelligenceFactor;
            return trustMomentum * (isValidNumber(score) ? score : 0) +
                (1 - trustMomentum) * avgTrust * (0.7 + 0.2 * historicalWeight + 0.1 * specializationFactor);
        });
    }

    #updateEnsembleWeights () {
        this.#ensembleWeights = this.#trustScoresHistory.map((history, idx) => {
            const recentTrust = history.slice(-5);
            const avgTrust = recentTrust.length > 0
                ? recentTrust.reduce((sum, val) => sum + (isValidNumber(val) ? val : 0), 0) / recentTrust.length
                : 1 / this.#ensembleSize;
            const specializationFactor = 1 + this.#specializationScores[idx] * this.#swarmIntelligenceFactor;
            return avgTrust * (0.8 + 0.2 * specializationFactor);
        });

        this.#normalizeEnsembleWeights();
    }

    #normalizeEnsembleWeights () {
        const sum = this.#ensembleWeights.reduce((s, w) => s + (isValidNumber(w) && w >= 0 ? w : 0), 0);

        if (sum <= 1e-6) {
            this.#ensembleWeights = Array(this.#ensembleSize).fill(1 / this.#ensembleSize);
            return;
        }

        this.#ensembleWeights = this.#ensembleWeights.map(w => 
            isValidNumber(w) && w >= 0 ? w / sum : 0
        );

        const finalSum = this.#ensembleWeights.reduce((s, w) => s + w, 0);
        if (Math.abs(finalSum - 1) > 1e-6) {
            const maxIndex = this.#ensembleWeights.indexOf(Math.max(...this.#ensembleWeights));
            this.#ensembleWeights[maxIndex] += 1 - finalSum;
        }
    }

    #updateAdaptiveLearningRates () {
        const recent_performance = this.#historicalPerformance.map(history => {
            const len = history.length;
            if (len === 0) return 0.5;
            const sum = history.reduce((s, v) => s + (isValidNumber(v) ? v : 0), 0);
            return sum / len;
        });

        const recent_trust = this.#trustScoresHistory.map(history => {
            const len = history.length;
            if (len === 0) return 0.5;
            const sum = history.reduce((s, v) => s + (isValidNumber(v) ? v : 0), 0);
            return sum / len;
        });

        const sorted_specialization = this.#specializationScores.slice().sort((a, b) => b - a);
        const max_spec = sorted_specialization[Math.floor(this.#ensembleSize * 0.05)] || 1e-10;
        const min_spec = sorted_specialization[this.#ensembleSize - 1] || 0;
        const spec_range = Math.max(max_spec - min_spec, 1e-10);
        const normalized_specialization = this.#specializationScores.map(score => 
            Math.min(Math.max((score - min_spec) / spec_range, 0), 1)
        );

        const composite_scores = recent_performance.map((perf, idx) => {
            const trust = recent_trust[idx];
            const spec = normalized_specialization[idx];
            const weighted_sum = 0.4 * perf + 0.3 * trust + 0.3 * spec;
            return this.#sigmoid(5 * weighted_sum);
        });

        const sorted_composite = composite_scores.slice().sort((a, b) => b - a);
        const thresholdIdx = Math.max(1, Math.floor(composite_scores.length * 0.25));
        const acceptable_threshold = sorted_composite[thresholdIdx] || 0.5;

        const min_lr = this.#learningRate * 0.5;
        const max_lr = this.#learningRate * 1.5;

        this.#adaptiveLearningRate = this.#adaptiveLearningRate.map((lr, idx) => {
            let newLr = lr;
            const score = composite_scores[idx];
            const score_diff = score - acceptable_threshold;

            const adjustment_magnitude = Math.max(-1, Math.min(1, score_diff * 2));
            if (score >= acceptable_threshold) {
                newLr *= (1 - this.#learningRateDecay * this.#learningRate * Math.abs(adjustment_magnitude) * 0.8);
            } else {
                newLr *= (1 + this.#learningRateDecay * this.#learningRate * Math.abs(adjustment_magnitude) * 1.2);
            }

            newLr = 0.95 * newLr + 0.05 * lr;

            return Math.max(min_lr, Math.min(newLr, max_lr));
        });
    }

    #updateMetrics (inputs, outputs, target, prob) {
        this.#updatePerformanceScores(outputs, target);
        this.#updateAgreementScores(outputs, prob);
        this.#computeSpecializationScores(inputs, outputs);
        this.#updateTrustScores();
        this.#adjustPerformanceScores();
        this.#updateEnsembleWeights();
        this.#updateAdaptiveLearningRates();
    }

    #computeSpectralNorm (matrix) {
        if (!Array.isArray(matrix)) {
            const error = new Error('Input is not an array');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error
            return 1;
        }
        if (matrix.length === 0) {
            const error = new Error('Matrix is empty');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error
            return 1;
        }
        if (!matrix.every(row => Array.isArray(row))) {
            const error = new Error('Matrix rows are not arrays');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error
            return 1;
        }
        if (!matrix.every(row => row.length === matrix[0].length)) {
            const error = new Error('Matrix rows have inconsistent lengths');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error
            return 1;
        }
        if (!matrix.every(row => row.every(isValidNumber))) {
            const error = new Error('Matrix contains invalid numbers');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error
            return 1;
        }
        if (matrix[0].length === 0) {
            const error = new Error('Matrix has no columns');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error
            return 1;
        }

        const rows = matrix.length;
        const cols = matrix[0].length;
        let u = Array(cols).fill().map(() => Math.random());
        let v = Array(rows).fill(0);

        const uNormInit = Math.sqrt(u.reduce((sum, x) => sum + (isValidNumber(x) ? x * x : 0), 0)) || 1;
        if (!isValidNumber(uNormInit)) {
            const error = new Error('Invalid initial u norm');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error;
        }
        u = u.map(x => isValidNumber(x) ? x / uNormInit : 0);

        const maxIter = 20;
        const tolerance = 1e-6;
        let prevNorm = 0;
        for (let iter = 0; iter < maxIter; iter++) {
            v = Array(rows).fill(0);
            for (let i = 0; i < rows; i++) {
                for (let j = 0; j < cols; j++) {
                    if (isValidNumber(matrix[i][j]) && isValidNumber(u[j])) {
                        v[i] += matrix[i][j] * u[j];
                    }
                }
            }
            const vNorm = Math.sqrt(v.reduce((sum, x) => sum + (isValidNumber(x) ? x * x : 0), 0)) || 1;
            if (!isValidNumber(vNorm)) {
                const error = new Error(`Invalid v norm in iteration ${iter + 1}`);
                console.error('Error:', error.message, '\nStack:', error.stack);
                throw error;
            }
            v = v.map(x => isValidNumber(x) ? x / vNorm : 0);

            u = Array(cols).fill(0);
            for (let j = 0; j < cols; j++) {
                for (let i = 0; i < rows; i++) {
                    if (isValidNumber(matrix[i][j]) && isValidNumber(v[i])) {
                        u[j] += matrix[i][j] * v[i];
                    }
                }
            }
            const uNorm = Math.sqrt(u.reduce((sum, x) => sum + (isValidNumber(x) ? x * x : 0), 0)) || 1;
            if (!isValidNumber(uNorm)) {
                const error = new Error(`Invalid u norm in iteration ${iter + 1}`);
                console.error('Error:', error.message, '\nStack:', error.stack);
                throw error;
            }
            u = u.map(x => isValidNumber(x) ? x / uNorm : 0);

            let currentNorm = 0;
            const temp = Array(rows).fill(0);
            for (let i = 0; i < rows; i++) {
                for (let j = 0; j < cols; j++) {
                    if (isValidNumber(matrix[i][j]) && isValidNumber(u[j])) {
                        temp[i] += matrix[i][j] * u[j];
                    }
                }
            }
            for (let i = 0; i < rows; i++) {
                if (isValidNumber(temp[i]) && isValidNumber(v[i])) {
                    currentNorm += temp[i] * v[i];
                }
            }
            currentNorm = Math.abs(currentNorm) || 1;
            if (!isValidNumber(currentNorm)) {
                const error = new Error(`Invalid norm in iteration ${iter + 1}`);
                console.error('Error:', error.message, '\nStack:', error.stack);
                throw error;
            }

            if (iter > 0 && Math.abs(currentNorm - prevNorm) < tolerance * Math.max(1, currentNorm)) {
                return currentNorm;
            }
            prevNorm = currentNorm;
        }

        let norm = 0;
        const temp = Array(rows).fill(0);
        for (let i = 0; i < rows; i++) {
            for (let j = 0; j < cols; j++) {
                if (isValidNumber(matrix[i][j]) && isValidNumber(u[j])) {
                    temp[i] += matrix[i][j] * u[j];
                }
            }
        }
        for (let i = 0; i < rows; i++) {
            if (isValidNumber(temp[i]) && isValidNumber(v[i])) {
                norm += temp[i] * v[i];
            }
        }
        const finalNorm = Math.abs(norm) || 1;
        if (!isValidNumber(finalNorm)) {
            const error = new Error('Invalid final norm');
            console.error('Error:', error.message, '\nStack:', error.stack);
            throw error;
        }

        return finalNorm;
    }

    #computeGradientNorm (grad, isMatrix) {
        if (isMatrix) {
            return Math.sqrt(grad.reduce((sum, row) => 
                sum + row.reduce((s, val) => s + (isValidNumber(val) ? val * val : 0), 0), 0)) || 1;
        } else {
            return Math.sqrt(grad.reduce((sum, val) => 
                sum + (isValidNumber(val) ? val * val : 0), 0)) || 1;
        }
    }

    #computeVariance (arr) {
        if (arr.length < 2) return 0;

        const median = arr.slice().sort((a, b) => a - b)[Math.floor(arr.length / 2)];
        const mad = arr.reduce((sum, val) => sum + Math.abs(val - median), 0) / Math.max(arr.length, 1);

        return Math.min(mad, 10);
    }

    #computeEMA (arr, beta) {
        if (arr.length === 0) return 0;

        let ema = arr[0];
        for (let i = 1; i < arr.length; i++) {
            ema = beta * ema + (1 - beta) * arr[i];
        }

        return ema;
    }

    #computeDualEMA (arr, betaShort, betaLong) {
        const shortEMA = this.#computeEMA(arr, betaShort);
        const longEMA = this.#computeEMA(arr, betaLong);
        const variance = this.#computeVariance(arr.slice(-10));
        const svrWeight = Math.min(0.8, Math.max(0.2, 0.5 * (1 - variance / (variance + 1))));

        return svrWeight * shortEMA + (1 - svrWeight) * longEMA;
    }

    #computeGradientConformity (norms) {
        if (norms.length < 2) return 1;

        const directions = norms.slice(1).map((val, i) => Math.sign(val - norms[i]));
        const consistency = directions.reduce((sum, val, i, arr) => sum + (i > 0 && val === arr[i - 1] ? 1 : 0), 0) / (directions.length || 1);

        return Math.min(1, Math.max(0.5, consistency));
    }

    #computeKernelRate (history) {
        const median = history.slice(-10).sort((a, b) => a - b)[Math.floor(history.length / 2)] || 0;
        const scaledHistory = history.slice(-10).map(val => (val - median) / (Math.abs(median) + 1e-10));
        const kernel = scaledHistory.map((val, i) => Math.exp(-0.1 * (scaledHistory.length - i - 1) ** 2));
        const kernelSum = kernel.reduce((sum, val) => sum + val, 0);

        return kernelSum > 0 ? kernel.reduce((sum, val, i) => sum + val * scaledHistory[i], 0) / kernelSum + 0.9 : 0.9;
    }

    #computeFractalDimension (arr) {
        if (arr.length < 2) return 1;

        const diffs = arr.slice(1).map((val, i) => Math.abs(val - arr[i]));
        const logDiffs = diffs.map(d => Math.log(Math.max(d, 1e-10)));
        const logScale = Math.log(arr.length);
        const fractalDim = logDiffs.length > 0 ? Math.abs(logDiffs.reduce((sum, val) => sum + val, 0) / logScale) : 1;

        return Math.min(2, Math.max(1, fractalDim));
    }

    #computePercentile (norms, percentile) {
        const sortedNorms = [...norms].sort((a, b) => a - b);
        const index = Math.floor(percentile * (sortedNorms.length - 1));

        return sortedNorms[index] || 1.0;
    }

    #computeNTKStability (norms, lossVariance) {
        if (norms.length < 2) return 1;

        const average = (arr) => arr.reduce((sum, val) => sum + val, 0) / Math.max(arr.length, 1);

        const medianNorm = norms.slice(-10).sort((a, b) => a - b)[Math.floor(norms.length / 2)] || 1;
        const bandwidth = Math.min(-0.01, Math.max(-0.1, -0.05 / (1 + lossVariance * medianNorm)));
        const kernel = norms.slice(-10).map((val, i) => Math.exp(bandwidth * (val - average(norms)) ** 2));
        const stability = kernel.reduce((sum, val) => sum + val, 0) / kernel.length;

        return Math.min(1.5, Math.max(0.5, stability));
    }

    #computeDynamicPercentile (norms, basePercentile) {
        const mad = this.#computeVariance(norms);
        const smoothness = this.#computeGradientConformity(norms);
        const adjustment = Math.min(0.05, (mad + 0.1 * smoothness) / (mad + smoothness + 1));

        return Math.min(0.99, Math.max(0.75, basePercentile - adjustment));
    }

    #computeSparseThreshold (norms) {
        const mad = this.#computeVariance(norms);

        return Math.max(1e-6, Math.min(1e-4, mad / 10));
    }

    #scaleGradientMatrix (gradMatrix, threshold, minScaleFactor, alpha, decay, sparseThreshold) {
        const spectralNorm = this.#computeSpectralNorm(gradMatrix);
        if (spectralNorm <= threshold) return;

        const scale = Math.max(minScaleFactor, Math.pow(threshold / spectralNorm, alpha) * decay);
        const quantScale = Math.ceil(scale * 255) / 255;
        for (let i = 0; i < gradMatrix.length; i++) {
            for (let j = 0; j < gradMatrix[i].length; j++) {
                if (Math.abs(gradMatrix[i][j]) > sparseThreshold) {
                    gradMatrix[i][j] *= quantScale;
                }
            }
        }
    }

    #scaleGradientVector (gradVector, threshold, minScaleFactor, alpha, decay, sparseThreshold) {
        const l2Norm = this.#computeGradientNorm(gradVector, false);
        if (l2Norm <= threshold) return;

        const scale = Math.max(minScaleFactor, Math.pow(threshold / l2Norm, alpha) * decay);
        const quantScale = Math.ceil(scale * 255) / 255;
        for (let i = 0; i < gradVector.length; i++) {
            if (Math.abs(gradVector[i]) > sparseThreshold) {
                gradVector[i] *= quantScale;
            }
        }
    }

    #scaleGradients (idx) {
        const baseAttentionPercentile = 0.95;
        const baseFfnPercentile = 0.9;
        const baseVectorPercentile = 0.8;

        const trustVariance = this.#computeVariance(this.#trustScoresHistory[idx].slice(-10));
        const lossVariance = this.#computeVariance(this.#historicalPerformance[idx].slice(-10));
        const stabilityMetric = 0.5 * lossVariance + 0.5 * trustVariance;

        const minScaleFactor = Math.max(0.01, 0.1 / (Math.sqrt(this.#ensembleSize) * (1 + stabilityMetric)));

        const emaBetaShort = Math.min(0.95, Math.max(0.8, this.#computeKernelRate(this.#historicalPerformance[idx])));
        const emaBetaLong = Math.min(0.999, Math.max(0.95, this.#computeKernelRate(this.#trustScoresHistory[idx])));

        const fractalDim = this.#computeFractalDimension(this.#historicalPerformance[idx].slice(-10));
        const componentWeights = {
            ensemble: Math.min(0.4, Math.max(0.1, 0.2 * (1 + 0.1 * fractalDim))),
            specialization: Math.min(0.4, Math.max(0.1, 0.2 * (1 + 0.15 * fractalDim * Math.min(this.#specializationScores[idx], 10)))),
            trust: Math.min(0.4, Math.max(0.1, 0.2 * (1 - 0.1 * fractalDim))),
            historicalPerformance: Math.min(0.4, Math.max(0.1, 0.2 * (1 + 0.05 * fractalDim))),
            performance: Math.min(0.4, Math.max(0.1, 0.2 * (1 + 0.05 * fractalDim)))
        };
        const totalWeight = Object.values(componentWeights).reduce((sum, w) => sum + w, 0);
        Object.keys(componentWeights).forEach(key => {
            componentWeights[key] /= Math.max(totalWeight, 1e-8);
        });

        const trust_ema = this.#computeDualEMA(this.#trustScoresHistory[idx], emaBetaShort, emaBetaLong);
        const perf_ema = this.#computeDualEMA(this.#historicalPerformance[idx], emaBetaShort, emaBetaLong);

        const robustnessFactor = Math.min(0.9, Math.max(0.1, 0.5 + 0.5 * (trust_ema / (Math.abs(trust_ema) + Math.abs(perf_ema) + 1e-10))));
        const compositeScore = (
            this.#ensembleWeights[idx] * componentWeights.ensemble +
            this.#specializationScores[idx] * componentWeights.specialization +
            trust_ema * componentWeights.trust +
            perf_ema * componentWeights.historicalPerformance +
            this.#performanceScores[idx] * componentWeights.performance
        ) * robustnessFactor;

        const totalCompositeScore = this.#ensembleWeights.reduce((sum, _, i) => {
            const memberTrustEma = this.#computeDualEMA(this.#trustScoresHistory[i], emaBetaShort, emaBetaLong);
            const memberPerfEma = this.#computeDualEMA(this.#historicalPerformance[i], emaBetaShort, emaBetaLong);
            const memberRobustness = Math.min(0.9, Math.max(0.1, 0.5 + 0.5 * (memberTrustEma / (Math.abs(memberTrustEma) + Math.abs(memberPerfEma) + 1e-10))));
            return sum + (
                this.#ensembleWeights[i] * componentWeights.ensemble +
                this.#specializationScores[i] * componentWeights.specialization +
                memberTrustEma * componentWeights.trust +
                memberPerfEma * componentWeights.historicalPerformance +
                this.#performanceScores[i] * componentWeights.performance
            ) * memberRobustness;
        }, 0);

        const normalizedScore = Math.min(1, Math.max(0, 0.8 * (compositeScore / Math.max(totalCompositeScore, 1e-8)) + 0.2 / this.#ensembleSize));
        const ntkStability = this.#computeNTKStability(this.#historicalPerformance[idx].slice(-10), lossVariance);
        const weightFactorMin = 0.5 / (1 + lossVariance);
        const weightFactorMax = 1 + this.#ensembleSize / (10 * (1 + lossVariance));
        const weightFactor = Math.min(weightFactorMax, Math.max(weightFactorMin, normalizedScore * this.#ensembleSize * ntkStability));

        const sigmoidSlope = 5 / (1 + trustVariance);
        const alpha = Math.min(1, Math.max(0.1, 0.5 + 0.5 * (1 / (1 + Math.exp(sigmoidSlope * trust_ema)) - 0.5)));

        const dynamicK = 4 / (1 + trustVariance);
        const decay = Math.max(0.1, Math.min(1, 1 / (1 + dynamicK * trustVariance)));

        const attentionMatrixNorms = [];
        const ffnMatrixNorms = [];
        const vectorNorms = [];
        const specMatrixNorms = [];

        ffnMatrixNorms.push(this.#computeSpectralNorm(this.#gradientAccumulation[idx].outputWeights));
        vectorNorms.push(this.#computeGradientNorm(this.#gradientAccumulation[idx].outputBias, false));

        for (let layer = 0; layer < this.#numLayers; layer++) {
            ['Wq', 'Wk', 'Wv', 'Wo'].forEach(key => {
                const gradMatrix = this.#gradientAccumulation[idx].attentionWeights[layer][key];
                attentionMatrixNorms.push(this.#computeSpectralNorm(gradMatrix));
            });

            ['gate_proj', 'up_proj'].forEach(key => {
                const gradMatrix = this.#gradientAccumulation[idx].ffnWeights[layer][key];
                ffnMatrixNorms.push(this.#computeSpectralNorm(gradMatrix));
            });
            const downGrad = this.#gradientAccumulation[idx].ffnWeights[layer].down_proj;
            ffnMatrixNorms.push(this.#computeSpectralNorm(downGrad));

            vectorNorms.push(this.#computeGradientNorm(this.#gradientAccumulation[idx].layerNormWeights[layer].gamma1, false));
            vectorNorms.push(this.#computeGradientNorm(this.#gradientAccumulation[idx].layerNormWeights[layer].gamma2, false));
        }

        vectorNorms.push(this.#computeGradientNorm(this.#gradientAccumulation[idx].attentionBias, false));
        vectorNorms.push(this.#computeGradientNorm(this.#gradientAccumulation[idx].attentionWeightMatrix, false));

        specMatrixNorms.push(this.#computeSpectralNorm(this.#gradientAccumulation[idx].specializationWeights));

        const sparseThreshold = this.#computeSparseThreshold([...attentionMatrixNorms, ...ffnMatrixNorms, ...vectorNorms, ...specMatrixNorms]);

        const attentionPercentile = this.#computeDynamicPercentile(attentionMatrixNorms, baseAttentionPercentile);
        const ffnPercentile = this.#computeDynamicPercentile(ffnMatrixNorms, baseFfnPercentile);
        const vectorPercentile = this.#computeDynamicPercentile(vectorNorms, baseVectorPercentile);
        const specPercentile = this.#computeDynamicPercentile(specMatrixNorms, baseAttentionPercentile);

        const attentionMatrixThreshold = Math.min(this.#computePercentile(attentionMatrixNorms, attentionPercentile), 1.0) * weightFactor;
        const ffnMatrixThreshold = Math.min(this.#computePercentile(ffnMatrixNorms, ffnPercentile), 1.0) * weightFactor;
        const vectorThreshold = Math.min(this.#computePercentile(vectorNorms, vectorPercentile), 1.0) * weightFactor;
        const specMatrixThreshold = Math.min(this.#computePercentile(specMatrixNorms, specPercentile), 1.0) * weightFactor;

        this.#scaleGradientMatrix(this.#gradientAccumulation[idx].outputWeights, ffnMatrixThreshold, minScaleFactor, alpha, decay, sparseThreshold);
        this.#scaleGradientVector(this.#gradientAccumulation[idx].outputBias, vectorThreshold, minScaleFactor, alpha, decay, sparseThreshold);

        for (let layer = 0; layer < this.#numLayers; layer++) {
            ['Wq', 'Wk', 'Wv', 'Wo'].forEach(key => {
                this.#scaleGradientMatrix(this.#gradientAccumulation[idx].attentionWeights[layer][key], attentionMatrixThreshold, minScaleFactor, alpha, decay, sparseThreshold);
            });

            ['gate_proj', 'up_proj'].forEach(key => {
                this.#scaleGradientMatrix(this.#gradientAccumulation[idx].ffnWeights[layer][key], ffnMatrixThreshold, minScaleFactor, alpha, decay, sparseThreshold);
            });
            this.#scaleGradientMatrix(this.#gradientAccumulation[idx].ffnWeights[layer].down_proj, ffnMatrixThreshold, minScaleFactor, alpha, decay, sparseThreshold);

            this.#scaleGradientVector(this.#gradientAccumulation[idx].layerNormWeights[layer].gamma1, vectorThreshold, minScaleFactor, alpha, decay, sparseThreshold);
            this.#scaleGradientVector(this.#gradientAccumulation[idx].layerNormWeights[layer].gamma2, vectorThreshold, minScaleFactor, alpha, decay, sparseThreshold);
        }

        this.#scaleGradientVector(this.#gradientAccumulation[idx].attentionBias, vectorThreshold, minScaleFactor, alpha, decay, sparseThreshold);
        this.#scaleGradientVector(this.#gradientAccumulation[idx].attentionWeightMatrix, vectorThreshold, minScaleFactor, alpha, decay, sparseThreshold);
        this.#scaleGradientMatrix(this.#gradientAccumulation[idx].specializationWeights, specMatrixThreshold, minScaleFactor, alpha, decay, sparseThreshold);
    }

    #accumulateGradients (inputs, outputs, target, probability, layerOutputs, activations, attentionIntermediates) {
        const dL_dLogit = probability - target;

        let finalLogit = 0;
        for (let i = 0; i < this.#ensembleSize; i++) {
            if (isValidNumber(outputs[i]) && isValidNumber(this.#ensembleWeights[i])) {
                finalLogit += this.#ensembleWeights[i] * outputs[i];
            }
        }

        this.#transformers.forEach((transformer, idx) => {
            const lr = this.#adaptiveLearningRate[idx];

            const memberWeight = isValidNumber(this.#ensembleWeights[idx]) ? this.#ensembleWeights[idx] : 1 / this.#ensembleSize;
            const effective_dL = dL_dLogit * memberWeight;

            let agreement_dL = 0;
            if (isValidNumber(outputs[idx])) {
                agreement_dL = dL_dLogit * memberWeight * (outputs[idx] - finalLogit);
            }

            let attWeightGrad = 0;
            if (isValidNumber(agreement_dL)) {
                attWeightGrad = agreement_dL / Math.sqrt(this.#hiddenSize);
            }
            for (let k = 0; k < this.#hiddenSize; k++) {
                if (isValidNumber(attWeightGrad)) {
                    this.#gradientAccumulation[idx].attentionBias[k] += attWeightGrad * lr;
                }
                for (let i = 0; i < this.#inputSize; i++) {
                    if (!isValidNumber(inputs[i])) continue;
                    const specFactor = isValidNumber(this.#specializationScores[idx])
                        ? 1 + this.#specializationScores[idx] * this.#swarmIntelligenceFactor
                        : 1;
                    const update = attWeightGrad * inputs[i] * specFactor * lr;
                    if (isValidNumber(update)) {
                        this.#gradientAccumulation[idx].attentionWeightMatrix[k] += update;
                    }
                }
            }

            for (let j = 0; j < this.#hiddenSize; j++) {
                for (let k = 0; k < this.#hiddenSize; k++) {
                    const inputIdx = (j + k) % this.#inputSize;
                    if (!isValidNumber(inputs[inputIdx])) continue;
                    const specFactor = isValidNumber(this.#specializationScores[idx])
                        ? 1 + this.#specializationScores[idx] * this.#swarmIntelligenceFactor
                        : 1;
                    const update = dL_dLogit * inputs[inputIdx] * specFactor * lr * 0.01;
                    if (isValidNumber(update)) {
                        this.#gradientAccumulation[idx].specializationWeights[j][k] += update;
                    }
                }
            }

            const finalX = layerOutputs[idx][this.#numLayers];
            let pooled = Array(this.#hiddenSize).fill(0);
            let validCount = 0;
            for (let pos = 0; pos < this.#inputSize; pos++) {
                let posHidden = finalX[pos];
                if (this.#numLayers > 0) {
                    const lastLayerIdx = this.#numLayers - 1;
                    posHidden = this.#rmsNorm(posHidden, transformer.layerNormWeights[lastLayerIdx].gamma2);
                }
                for (let j = 0; j < this.#hiddenSize; j++) {
                    if (isValidNumber(posHidden[j])) {
                        pooled[j] += posHidden[j];
                    }
                }
                validCount++;
            }
            if (validCount > 0) {
                pooled = pooled.map(v => v / validCount);
            }

            let gradPooled = Array(this.#hiddenSize).fill(0);
            for (let i = 0; i < this.#hiddenSize; i++) {
                const w = transformer.outputWeights[i][0];
                if (isValidNumber(pooled[i]) && isValidNumber(w)) {
                    this.#gradientAccumulation[idx].outputWeights[i][0] += effective_dL * pooled[i] * lr;
                    gradPooled[i] += effective_dL * w;
                }
            }
            this.#gradientAccumulation[idx].outputBias[0] += effective_dL * lr;

            let grad = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
            if (this.#numLayers > 0) {
                const lastLayer = this.#numLayers - 1;
                const gamma = transformer.layerNormWeights[lastLayer].gamma2;
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    const x = finalX[pos];
                    const sq_sum = x.reduce((s, v) => s + v * v, 0);
                    const rms = Math.sqrt(sq_sum / this.#hiddenSize + 1e-6);
                    const norm_x = x.map(v => v / rms);

                    const incoming = gradPooled.map(g => g / validCount);

                    const d_gamma = incoming.map((g, i) => g * norm_x[i]);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        this.#gradientAccumulation[idx].layerNormWeights[lastLayer].gamma2[j] += d_gamma[j] * lr;
                    }

                    const d_norm = incoming.map((g, i) => g * gamma[i]);
                    const dot = d_norm.reduce((s, dn, i) => s + dn * norm_x[i], 0);
                    const d_x = d_norm.map((dn, i) => dn / rms - norm_x[i] * dot / (this.#hiddenSize + 1e-8));
                    grad[pos] = d_x;
                }
            } else {
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    grad[pos] = gradPooled.map(g => g / validCount);
                }
            }

            for (let layer = this.#numLayers - 1; layer >= 0; layer--) {
                const act = activations[idx][layer];
                const inter = attentionIntermediates[idx][layer];
                const headSize = this.#hiddenSize / this.#numHeads;

                let gradToAttentionResidual = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        gradToAttentionResidual[pos][j] = grad[pos][j];
                    }
                }

                for (let pos = 0; pos < this.#inputSize; pos++) {
                    const x = act.normAttention[pos];
                    const gate = Array(this.#feedForwardSize).fill(0);
                    const up = Array(this.#feedForwardSize).fill(0);

                    for (let i = 0; i < this.#hiddenSize; i++) {
                        for (let j = 0; j < this.#feedForwardSize; j++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            gate[j] += isValidNumber(x[i]) && isValidNumber(transformer.ffnWeights[layer].gate_proj[i][j])
                                ? x[i] * transformer.ffnWeights[layer].gate_proj[i][j] * specWeight
                                : 0;
                            up[j] += isValidNumber(x[i]) && isValidNumber(transformer.ffnWeights[layer].up_proj[i][j])
                                ? x[i] * transformer.ffnWeights[layer].up_proj[i][j] * specWeight
                                : 0;
                        }
                    }

                    const silu_gate = gate.map(this.#silu);
                    const activated = silu_gate.map((s, j) => s * up[j]);

                    const incoming = grad[pos];

                    for (let j = 0; j < this.#feedForwardSize; j++) {
                        for (let k = 0; k < this.#hiddenSize; k++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            this.#gradientAccumulation[idx].ffnWeights[layer].down_proj[j][k] += incoming[k] * activated[j] * specWeight * lr;
                        }
                    }

                    const d_activated = Array(this.#feedForwardSize).fill(0);
                    for (let j = 0; j < this.#feedForwardSize; j++) {
                        for (let k = 0; k < this.#hiddenSize; k++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            d_activated[j] += incoming[k] * transformer.ffnWeights[layer].down_proj[j][k] * specWeight;
                        }
                    }

                    const d_silu = d_activated.map((d, j) => d * up[j]);
                    const d_up = d_activated.map((d, j) => d * silu_gate[j]);
                    const d_gate = d_silu.map((d, j) => d * this.#siluDerivative(gate[j]));

                    let d_normAttention = Array(this.#hiddenSize).fill(0);
                    for (let i = 0; i < this.#hiddenSize; i++) {
                        for (let j = 0; j < this.#feedForwardSize; j++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;

                            this.#gradientAccumulation[idx].ffnWeights[layer].gate_proj[i][j] += d_gate[j] * x[i] * specWeight * lr;
                            this.#gradientAccumulation[idx].ffnWeights[layer].up_proj[i][j] += d_up[j] * x[i] * specWeight * lr;

                            d_normAttention[i] += d_gate[j] * transformer.ffnWeights[layer].gate_proj[i][j] * specWeight +
                                                  d_up[j] * transformer.ffnWeights[layer].up_proj[i][j] * specWeight;
                        }
                    }

                    const gamma2 = transformer.layerNormWeights[layer].gamma2;
                    const sq_sum = x.reduce((s, v) => s + v * v, 0);
                    const rms = Math.sqrt(sq_sum / this.#hiddenSize + 1e-6);
                    const norm_x = x.map(v => v / rms);

                    const d_gamma = d_normAttention.map((g, i) => g * norm_x[i]);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        this.#gradientAccumulation[idx].layerNormWeights[layer].gamma2[j] += d_gamma[j] * lr;
                    }

                    const d_norm = d_normAttention.map((g, i) => g * gamma2[i]);
                    const dot = d_norm.reduce((s, dn, i) => s + dn * norm_x[i], 0);
                    const d_x_branch = d_norm.map((dn, i) => dn / rms - norm_x[i] * dot / (this.#hiddenSize + 1e-8));

                    for (let j = 0; j < this.#hiddenSize; j++) {
                        gradToAttentionResidual[pos][j] += d_x_branch[j];
                    }
                }

                const gradToAttentionOutput = gradToAttentionResidual;

                const d_v = Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(headSize).fill(0)));
                const d_scores = Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(this.#inputSize).fill(0)));
                const d_q = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
                const d_k = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));

                for (let queryPos = 0; queryPos < this.#inputSize; queryPos++) {
                    const incoming = gradToAttentionOutput[queryPos];

                    for (let outDim = 0; outDim < this.#hiddenSize; outDim++) {
                        for (let preDim = 0; preDim < this.#hiddenSize; preDim++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim], 0.5), 1.5)
                                : 1;
                            this.#gradientAccumulation[idx].attentionWeights[layer].Wo[preDim][outDim] += incoming[outDim] * inter.preWoOutput[queryPos][preDim] * specWeight * lr;
                        }
                    }

                    const d_preWo = Array(this.#hiddenSize).fill(0);
                    for (let outDim = 0; outDim < this.#hiddenSize; outDim++) {
                        for (let preDim = 0; preDim < this.#hiddenSize; preDim++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim], 0.5), 1.5)
                                : 1;
                            d_preWo[preDim] += incoming[outDim] * transformer.attentionWeights[layer].Wo[preDim][outDim] * specWeight;
                        }
                    }

                    for (let h = 0; h < this.#numHeads; h++) {
                        const offset = h * headSize;
                        for (let dim = 0; dim < headSize; dim++) {
                            const idx = offset + dim;
                            let d_att = isValidNumber(d_preWo[idx]) ? d_preWo[idx] : 0;

                            for (let valuePos = 0; valuePos < this.#inputSize; valuePos++) {
                                let prob = isValidNumber(inter.attentionProbs[h][queryPos][valuePos]) ? inter.attentionProbs[h][queryPos][valuePos] : 0;
                                let vVal = isValidNumber(inter.V[valuePos][idx]) ? inter.V[valuePos][idx] : 0;

                                d_scores[h][queryPos][valuePos] += d_att * vVal;
                                d_v[h][valuePos][dim] += d_att * prob;
                            }
                        }
                    }
                }

                for (let h = 0; h < this.#numHeads; h++) {
                    for (let qp = 0; qp < this.#inputSize; qp++) {
                        let weighted = 0;
                        for (let vp = 0; vp < this.#inputSize; vp++) {
                            const p = isValidNumber(inter.attentionProbs[h][qp][vp]) ? inter.attentionProbs[h][qp][vp] : 0;
                            const dp = isValidNumber(d_scores[h][qp][vp]) ? d_scores[h][qp][vp] : 0;
                            weighted += p * dp;
                        }
                        for (let vp = 0; vp < this.#inputSize; vp++) {
                            const p = isValidNumber(inter.attentionProbs[h][qp][vp]) ? inter.attentionProbs[h][qp][vp] : 0;
                            const dp = isValidNumber(d_scores[h][qp][vp]) ? d_scores[h][qp][vp] : 0;
                            d_scores[h][qp][vp] = p * (dp - weighted);
                        }
                    }
                }

                for (let h = 0; h < this.#numHeads; h++) {
                    for (let queryPos = 0; queryPos < this.#inputSize; queryPos++) {
                        for (let keyPos = 0; keyPos < this.#inputSize; keyPos++) {
                            let scaled = isValidNumber(d_scores[h][queryPos][keyPos]) ? d_scores[h][queryPos][keyPos] / Math.sqrt(headSize) : 0;
                            for (let dim = 0; dim < headSize; dim++) {
                                const idx = h * headSize + dim;
                                d_q[queryPos][idx] += scaled * (isValidNumber(inter.K[keyPos][idx]) ? inter.K[keyPos][idx] : 0);
                                d_k[keyPos][idx] += scaled * (isValidNumber(inter.Q[queryPos][idx]) ? inter.Q[queryPos][idx] : 0);
                            }
                        }
                    }
                }

                let gradToNormX = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));

                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let inputDim = 0; inputDim < this.#hiddenSize; inputDim++) {
                        let contrib = 0;
                        for (let outputDim = 0; outputDim < this.#hiddenSize; outputDim++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim], 0.5), 1.5)
                                : 1;
                            contrib += d_q[pos][outputDim] * transformer.attentionWeights[layer].Wq[inputDim][outputDim] * specWeight;
                        }
                        gradToNormX[pos][inputDim] += contrib;
                    }
                }

                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let inputDim = 0; inputDim < this.#hiddenSize; inputDim++) {
                        let contrib = 0;
                        for (let outputDim = 0; outputDim < this.#hiddenSize; outputDim++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim], 0.5), 1.5)
                                : 1;
                            contrib += d_k[pos][outputDim] * transformer.attentionWeights[layer].Wk[inputDim][outputDim] * specWeight;
                        }
                        gradToNormX[pos][inputDim] += contrib;
                    }
                }

                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let inputDim = 0; inputDim < this.#hiddenSize; inputDim++) {
                        let contrib = 0;
                        for (let h = 0; h < this.#numHeads; h++) {
                            for (let dim = 0; dim < headSize; dim++) {
                                const outputDim = h * headSize + dim;
                                const specWeight = isValidNumber(this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim])
                                    ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim], 0.5), 1.5)
                                    : 1;
                                contrib += d_v[h][pos][dim] * transformer.attentionWeights[layer].Wv[inputDim][outputDim] * specWeight;
                            }
                        }
                        gradToNormX[pos][inputDim] += contrib;
                    }
                }

                for (let inputDim = 0; inputDim < this.#hiddenSize; inputDim++) {
                    for (let outputDim = 0; outputDim < this.#hiddenSize; outputDim++) {
                        let wqUpdate = 0;
                        let wkUpdate = 0;
                        let wvUpdate = 0;

                        const specWeight = isValidNumber(this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim])
                            ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][inputDim % this.#hiddenSize][outputDim], 0.5), 1.5)
                            : 1;

                        for (let pos = 0; pos < this.#inputSize; pos++) {
                            wqUpdate += d_q[pos][outputDim] * act.normX[pos][inputDim];
                            wkUpdate += d_k[pos][outputDim] * act.normX[pos][inputDim];

                            const head = Math.floor(outputDim / headSize);
                            const dim = outputDim % headSize;
                            wvUpdate += d_v[head][pos][dim] * act.normX[pos][inputDim];
                        }

                        this.#gradientAccumulation[idx].attentionWeights[layer].Wq[inputDim][outputDim] += wqUpdate * specWeight * lr;
                        this.#gradientAccumulation[idx].attentionWeights[layer].Wk[inputDim][outputDim] += wkUpdate * specWeight * lr;
                        this.#gradientAccumulation[idx].attentionWeights[layer].Wv[inputDim][outputDim] += wvUpdate * specWeight * lr;
                    }
                }

                const gamma1 = transformer.layerNormWeights[layer].gamma1;
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    const x = act.normX[pos];
                    const sq_sum = x.reduce((s, v) => s + v * v, 0);
                    const rms = Math.sqrt(sq_sum / this.#hiddenSize + 1e-6);
                    const norm_x = x.map(v => v / rms);

                    const incoming = gradToNormX[pos];

                    const d_gamma = incoming.map((g, i) => g * norm_x[i]);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        this.#gradientAccumulation[idx].layerNormWeights[layer].gamma1[j] += d_gamma[j] * lr;
                    }

                    const d_norm = incoming.map((g, i) => g * gamma1[i]);
                    const dot = d_norm.reduce((s, dn, i) => s + dn * norm_x[i], 0);
                    const d_x = d_norm.map((dn, i) => dn / rms - norm_x[i] * dot / (this.#hiddenSize + 1e-8));

                    for (let j = 0; j < this.#hiddenSize; j++) {
                        grad[pos][j] = gradToAttentionOutput[pos][j] + d_x[j];
                    }
                }
            }
        });
    }

    #applyGradients (shouldScale = true, shouldClone = false, steps = 1) {
        let gradClone;
        if (shouldClone) gradClone = this.#setGradientStructure();

        this.#transformers.forEach((transformer, idx) => {
            if (shouldScale) this.#scaleGradients(idx);

            for (let k = 0; k < this.#hiddenSize; k++) {
                const biasAcc = this.#gradientAccumulation[idx].attentionBias[k];
                if (isValidNumber(biasAcc)) {
                    const finalBiasAcc = biasAcc / steps;
                    this.#attentionBias[idx][k] -= finalBiasAcc;
                    if (shouldClone) gradClone[idx].attentionBias[k] = finalBiasAcc;
                }

                const matrixAcc = this.#gradientAccumulation[idx].attentionWeightMatrix[k];
                if (isValidNumber(matrixAcc)) {
                    const finalMatrixAcc = matrixAcc / steps;
                    this.#attentionWeightMatrix[idx][k] -= finalMatrixAcc;
                    if (shouldClone) gradClone[idx].attentionWeightMatrix[k] = finalMatrixAcc;
                }
            }

            for (let j = 0; j < this.#hiddenSize; j++) {
                for (let k = 0; k < this.#hiddenSize; k++) {
                    const specAcc = this.#gradientAccumulation[idx].specializationWeights[j][k];
                    if (isValidNumber(specAcc)) {
                        const finalSpecAcc = specAcc / steps;
                        this.#specializationWeights[idx][j][k] -= finalSpecAcc;
                        if (shouldClone) gradClone[idx].specializationWeights[j][k] = finalSpecAcc;
                    }
                }
            }

            for (let i = 0; i < this.#hiddenSize; i++) {
                const outWeightAcc = this.#gradientAccumulation[idx].outputWeights[i][0];
                if (isValidNumber(outWeightAcc)) {
                    const finalOutWeightAcc = outWeightAcc / steps;
                    transformer.outputWeights[i][0] -= finalOutWeightAcc;
                    if (shouldClone) gradClone[idx].outputWeights[i][0] = finalOutWeightAcc;
                }
            }
            const outBiasAcc = this.#gradientAccumulation[idx].outputBias[0];
            if (isValidNumber(outBiasAcc)) {
                const finalOutBiasAcc = outBiasAcc / steps;
                transformer.outputBias[0] -= finalOutBiasAcc;
                if (shouldClone) gradClone[idx].outputBias[0] = finalOutBiasAcc;
            }

            for (let layer = 0; layer < this.#numLayers; layer++) {
                for (let i = 0; i < this.#hiddenSize; i++) {
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        ['Wq', 'Wk', 'Wv', 'Wo'].forEach(key => {
                            const acc = this.#gradientAccumulation[idx].attentionWeights[layer][key][i][j];
                            if (isValidNumber(acc)) {
                                const finalAcc = acc / steps;
                                transformer.attentionWeights[layer][key][i][j] -= finalAcc;
                                if (shouldClone) gradClone[idx].attentionWeights[layer][key][i][j] = finalAcc;
                            }
                        });
                    }
                }

                for (let i = 0; i < this.#hiddenSize; i++) {
                    for (let j = 0; j < this.#feedForwardSize; j++) {
                        const gateAcc = this.#gradientAccumulation[idx].ffnWeights[layer].gate_proj[i][j];
                        if (isValidNumber(gateAcc)) {
                            const finalAcc = gateAcc / steps;
                            transformer.ffnWeights[layer].gate_proj[i][j] -= finalAcc;
                            if (shouldClone) gradClone[idx].ffnWeights[layer].gate_proj[i][j] = finalAcc;
                        }
                        const upAcc = this.#gradientAccumulation[idx].ffnWeights[layer].up_proj[i][j];
                        if (isValidNumber(upAcc)) {
                            const finalAcc = upAcc / steps;
                            transformer.ffnWeights[layer].up_proj[i][j] -= finalAcc;
                            if (shouldClone) gradClone[idx].ffnWeights[layer].up_proj[i][j] = finalAcc;
                        }
                    }
                }
                for (let i = 0; i < this.#feedForwardSize; i++) {
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        const downAcc = this.#gradientAccumulation[idx].ffnWeights[layer].down_proj[i][j];
                        if (isValidNumber(downAcc)) {
                            const finalAcc = downAcc / steps;
                            transformer.ffnWeights[layer].down_proj[i][j] -= finalAcc;
                            if (shouldClone) gradClone[idx].ffnWeights[layer].down_proj[i][j] = finalAcc;
                        }
                    }
                }

                for (let i = 0; i < this.#hiddenSize; i++) {
                    const gamma1Acc = this.#gradientAccumulation[idx].layerNormWeights[layer].gamma1[i];
                    if (isValidNumber(gamma1Acc)) {
                        const finalAcc = gamma1Acc / steps;
                        transformer.layerNormWeights[layer].gamma1[i] -= finalAcc;
                        if (shouldClone) gradClone[idx].layerNormWeights[layer].gamma1[i] = finalAcc;
                    }
                    const gamma2Acc = this.#gradientAccumulation[idx].layerNormWeights[layer].gamma2[i];
                    if (isValidNumber(gamma2Acc)) {
                        const finalAcc = gamma2Acc / steps;
                        transformer.layerNormWeights[layer].gamma2[i] -= finalAcc;
                        if (shouldClone) gradClone[idx].layerNormWeights[layer].gamma2[i] = finalAcc;
                    }
                }
            }
        });

        if (shouldClone) return gradClone;
    }

    #rollbackGradients (gradClone) {
        this.#transformers.forEach((transformer, idx) => {
            for (let k = 0; k < this.#hiddenSize; k++) {
                const attBias = gradClone[idx].attentionBias[k];
                if (isValidNumber(attBias)) this.#attentionBias[idx][k] += attBias;

                const attMatrix = gradClone[idx].attentionWeightMatrix[k];
                if (isValidNumber(attMatrix)) this.#attentionWeightMatrix[idx][k] += attMatrix;
            }

            for (let j = 0; j < this.#hiddenSize; j++) {
                for (let k = 0; k < this.#hiddenSize; k++) {
                    const specWeight = gradClone[idx].specializationWeights[j][k];
                    if (isValidNumber(specWeight)) this.#specializationWeights[idx][j][k] += specWeight;
                }
            }

            for (let i = 0; i < this.#hiddenSize; i++) {
                const outputWeight = gradClone[idx].outputWeights[i][0];
                if (isValidNumber(outputWeight)) transformer.outputWeights[i][0] += outputWeight;
            }
            const outputBias = gradClone[idx].outputBias[0];
            if (isValidNumber(outputBias)) transformer.outputBias[0] += outputBias;

            for (let layer = 0; layer < this.#numLayers; layer++) {
                for (let i = 0; i < this.#hiddenSize; i++) {
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        ['Wq', 'Wk', 'Wv', 'Wo'].forEach(key => {
                            const weight = gradClone[idx].attentionWeights[layer][key][i][j];
                            if (isValidNumber(weight)) transformer.attentionWeights[layer][key][i][j] += weight;
                        });
                    }
                }

                for (let i = 0; i < this.#hiddenSize; i++) {
                    for (let j = 0; j < this.#feedForwardSize; j++) {
                        const gateWeight = gradClone[idx].ffnWeights[layer].gate_proj[i][j];
                        if (isValidNumber(gateWeight)) transformer.ffnWeights[layer].gate_proj[i][j] += gateWeight;
                        const upWeight = gradClone[idx].ffnWeights[layer].up_proj[i][j];
                        if (isValidNumber(upWeight)) transformer.ffnWeights[layer].up_proj[i][j] += upWeight;
                    }
                }
                for (let i = 0; i < this.#feedForwardSize; i++) {
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        const downWeight = gradClone[idx].ffnWeights[layer].down_proj[i][j];
                        if (isValidNumber(downWeight)) transformer.ffnWeights[layer].down_proj[i][j] += downWeight;
                    }
                }

                for (let i = 0; i < this.#hiddenSize; i++) {
                    const gamma1Weight = gradClone[idx].layerNormWeights[layer].gamma1[i];
                    if (isValidNumber(gamma1Weight)) transformer.layerNormWeights[layer].gamma1[i] += gamma1Weight;
                    const gamma2Weight = gradClone[idx].layerNormWeights[layer].gamma2[i];
                    if (isValidNumber(gamma2Weight)) transformer.layerNormWeights[layer].gamma2[i] += gamma2Weight;
                }
            }
        });
    }

    #distillKnowledge (outputs, attentionIntermediatesAll, activationsAll, layerOutputsAll) {
        if (
            !Array.isArray(outputs) ||
            outputs.length !== this.#ensembleSize ||
            !outputs.every(isValidNumber) ||
            attentionIntermediatesAll.length !== this.#ensembleSize ||
            activationsAll.length !== this.#ensembleSize ||
            layerOutputsAll.length !== this.#ensembleSize
        ) {
            return;
        }

        const sortedIndices = this.#performanceScores
            .map((score, idx) => ({ score: isValidNumber(score) ? score : 0, idx }))
            .sort((a, b) => b.score - a.score);

        const topTierCount = Math.max(1, Math.floor(this.#ensembleSize * 0.30));
        const middleTierEnd = topTierCount + Math.floor(this.#ensembleSize * 0.40);
        const topPerformers = sortedIndices.slice(0, topTierCount).map(({ idx }) => idx);

        const topOutputs = topPerformers.map(idx => outputs[idx]);
        const topWeights = topPerformers.map(idx => this.#ensembleWeights[idx]);
        const weightSum = topWeights.reduce((sum, w) => sum + (isValidNumber(w) ? w : 0), 0) || 1;
        const teacherLogit = topOutputs.reduce((sum, output, i) =>
            sum + output * (topWeights[i] / weightSum), 0
        );

        const varianceTop = topOutputs.reduce((sum, out) => sum + Math.pow(out - teacherLogit, 2), 0) / topOutputs.length;
        const stdTopOutputs = Math.sqrt(varianceTop + 1e-8);

        const temperature = 2.0;
        const momentum = 0.9;
        const outputKdScale = 0.2;

        this.#transformers.forEach((transformer, idx) => {
            if (topPerformers.includes(idx)) return;

            const rank = sortedIndices.findIndex(entry => entry.idx === idx);
            const isMiddleTier = rank >= topTierCount && rank < middleTierEnd;
            const isBottomTier = rank >= middleTierEnd;

            const kdStrengthMultiplier = isMiddleTier ? 1.0 : 0.7;

            const diversityScore = Math.min(1, Math.abs(outputs[idx] - teacherLogit) / (stdTopOutputs || 1));
            const performanceGap = this.#performanceScores[topPerformers[0]] - this.#performanceScores[idx];

            let klWeight = Math.min(1.0, Math.max(0.1, performanceGap / (this.#performanceScores[topPerformers[0]] || 1)));
            klWeight *= (1 - diversityScore);
            klWeight *= kdStrengthMultiplier;
            klWeight = Math.min(1.0, klWeight);

            const studentLogit = outputs[idx];
            const studentProb = this.#sigmoid(studentLogit / temperature);
            const teacherProb = this.#sigmoid(teacherLogit / temperature);

            let baseGrad = (studentProb - teacherProb) / temperature;
            baseGrad *= klWeight;
            if (!isValidNumber(baseGrad)) baseGrad = 0;

            const adjustedLearningRate = this.#adaptiveLearningRate[idx];

            const finalX = layerOutputsAll[idx][this.#numLayers];
            let pooled = Array(this.#hiddenSize).fill(0);
            let validCount = 0;
            for (let pos = 0; pos < this.#inputSize; pos++) {
                let posHidden = finalX[pos];
                if (this.#numLayers > 0) {
                    const lastLayerIdx = this.#numLayers - 1;
                    posHidden = this.#rmsNorm(posHidden, transformer.layerNormWeights[lastLayerIdx].gamma2);
                }
                for (let j = 0; j < this.#hiddenSize; j++) {
                    if (isValidNumber(posHidden[j])) {
                        pooled[j] += posHidden[j];
                    }
                }
                validCount++;
            }
            if (validCount > 0) {
                pooled = pooled.map(v => v / validCount);
            }

            const kd_dL = baseGrad * outputKdScale;
            for (let i = 0; i < this.#hiddenSize; i++) {
                if (isValidNumber(pooled[i])) {
                    const update = kd_dL * pooled[i] * adjustedLearningRate;
                    this.#gradientAccumulation[idx].outputWeights[i][0] += (1 - momentum) * update;
                }
            }
            this.#gradientAccumulation[idx].outputBias[0] += (1 - momentum) * kd_dL * adjustedLearningRate;

            if (!isBottomTier) return;

            let gradPooled = Array(this.#hiddenSize).fill(0);
            for (let i = 0; i < this.#hiddenSize; i++) {
                if (isValidNumber(transformer.outputWeights[i][0])) {
                    gradPooled[i] = kd_dL * transformer.outputWeights[i][0];
                }
            }

            let grad = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
            if (this.#numLayers > 0) {
                const lastLayerIdx = this.#numLayers - 1;
                const gamma = transformer.layerNormWeights[lastLayerIdx].gamma2;
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    const x = finalX[pos];
                    const sq_sum = x.reduce((s, v) => s + v * v, 0);
                    const rms = Math.sqrt(sq_sum / this.#hiddenSize + 1e-6);
                    const norm_x = x.map(v => v / rms);

                    const incoming = gradPooled.map(g => g / validCount);

                    const d_gamma = incoming.map((g, i) => g * norm_x[i]);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        if (isValidNumber(d_gamma[j])) {
                            const update = d_gamma[j] * adjustedLearningRate * 0.5;
                            this.#gradientAccumulation[idx].layerNormWeights[lastLayerIdx].gamma2[j] += (1 - momentum) * update;
                        }
                    }

                    const d_norm = incoming.map((g, i) => g * gamma[i]);
                    const dot = d_norm.reduce((s, dn, i) => s + dn * norm_x[i], 0);
                    const d_x = d_norm.map((dn, i) => dn / rms - norm_x[i] * dot / (this.#hiddenSize + 1e-8));
                    grad[pos] = d_x;
                }
            } else {
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    grad[pos] = gradPooled.map(g => g / validCount);
                }
            }

            for (let layer = this.#numLayers - 1; layer >= 0; layer--) {
                const act = activationsAll[idx][layer];
                const inter = attentionIntermediatesAll[idx][layer];
                const headSize = this.#hiddenSize / this.#numHeads;

                let gradToAttentionResidual = grad.map(row => row.slice());

                for (let pos = 0; pos < this.#inputSize; pos++) {
                    const x = act.normAttention[pos];

                    const gate = Array(this.#feedForwardSize).fill(0);
                    const up = Array(this.#feedForwardSize).fill(0);
                    for (let i = 0; i < this.#hiddenSize; i++) {
                        for (let j = 0; j < this.#feedForwardSize; j++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            gate[j] += isValidNumber(x[i]) && isValidNumber(transformer.ffnWeights[layer].gate_proj[i][j])
                                ? x[i] * transformer.ffnWeights[layer].gate_proj[i][j] * specWeight
                                : 0;
                            up[j] += isValidNumber(x[i]) && isValidNumber(transformer.ffnWeights[layer].up_proj[i][j])
                                ? x[i] * transformer.ffnWeights[layer].up_proj[i][j] * specWeight
                                : 0;
                        }
                    }
                    const silu_gate = gate.map(this.#silu);
                    const activated = silu_gate.map((s, j) => s * up[j]);

                    const incoming = grad[pos];

                    for (let j = 0; j < this.#feedForwardSize; j++) {
                        for (let k = 0; k < this.#hiddenSize; k++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            const update = incoming[k] * activated[j] * specWeight * adjustedLearningRate;
                            if (isValidNumber(update)) {
                                this.#gradientAccumulation[idx].ffnWeights[layer].down_proj[j][k] += (1 - momentum) * update;
                            }
                        }
                    }

                    const d_activated = Array(this.#feedForwardSize).fill(0);
                    for (let j = 0; j < this.#feedForwardSize; j++) {
                        for (let k = 0; k < this.#hiddenSize; k++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][k % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            d_activated[j] += incoming[k] * transformer.ffnWeights[layer].down_proj[j][k] * specWeight;
                        }
                    }
                    const d_silu = d_activated.map((d, j) => d * up[j]);
                    const d_up = d_activated.map((d, j) => d * silu_gate[j]);
                    const d_gate = d_silu.map((d, j) => d * this.#siluDerivative(gate[j]));

                    for (let i = 0; i < this.#hiddenSize; i++) {
                        for (let j = 0; j < this.#feedForwardSize; j++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            const gateUpdate = d_gate[j] * x[i] * specWeight * adjustedLearningRate;
                            if (isValidNumber(gateUpdate)) {
                                this.#gradientAccumulation[idx].ffnWeights[layer].gate_proj[i][j] += (1 - momentum) * gateUpdate;
                            }
                            const upUpdate = d_up[j] * x[i] * specWeight * adjustedLearningRate;
                            if (isValidNumber(upUpdate)) {
                                this.#gradientAccumulation[idx].ffnWeights[layer].up_proj[i][j] += (1 - momentum) * upUpdate;
                            }
                        }
                    }

                    let d_normAttention = Array(this.#hiddenSize).fill(0);
                    for (let i = 0; i < this.#hiddenSize; i++) {
                        for (let j = 0; j < this.#feedForwardSize; j++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][i % this.#hiddenSize][j % this.#hiddenSize], 0.5), 1.5)
                                : 1;
                            d_normAttention[i] += d_gate[j] * transformer.ffnWeights[layer].gate_proj[i][j] * specWeight +
                                                  d_up[j] * transformer.ffnWeights[layer].up_proj[i][j] * specWeight;
                        }
                    }

                    const gamma2 = transformer.layerNormWeights[layer].gamma2;
                    const sq_sum2 = x.reduce((s, v) => s + v * v, 0);
                    const rms2 = Math.sqrt(sq_sum2 / this.#hiddenSize + 1e-6);
                    const norm_x2 = x.map(v => v / rms2);
                    const d_gamma2 = d_normAttention.map((g, i) => g * norm_x2[i]);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        if (isValidNumber(d_gamma2[j])) {
                            const update = d_gamma2[j] * adjustedLearningRate * 0.5;
                            this.#gradientAccumulation[idx].layerNormWeights[layer].gamma2[j] += (1 - momentum) * update;
                        }
                    }
                    const d_norm2 = d_normAttention.map((g, i) => g * gamma2[i]);
                    const dot2 = d_norm2.reduce((s, dn, i) => s + dn * norm_x2[i], 0);
                    const d_branch = d_norm2.map((dn, i) => dn / rms2 - norm_x2[i] * dot2 / (this.#hiddenSize + 1e-8));

                    for (let j = 0; j < this.#hiddenSize; j++) {
                        gradToAttentionResidual[pos][j] += d_branch[j];
                    }
                }

                const gradAttentionOutput = gradToAttentionResidual;

                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let outDim = 0; outDim < this.#hiddenSize; outDim++) {
                        for (let preDim = 0; preDim < this.#hiddenSize; preDim++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim], 0.5), 1.5)
                                : 1;
                            const update = gradAttentionOutput[pos][outDim] * inter.preWoOutput[pos][preDim] * specWeight * adjustedLearningRate;
                            if (isValidNumber(update)) {
                                this.#gradientAccumulation[idx].attentionWeights[layer].Wo[preDim][outDim] += (1 - momentum) * update;
                            }
                        }
                    }
                }

                let d_preWo = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let outDim = 0; outDim < this.#hiddenSize; outDim++) {
                        for (let preDim = 0; preDim < this.#hiddenSize; preDim++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][preDim % this.#hiddenSize][outDim], 0.5), 1.5)
                                : 1;
                            d_preWo[pos][preDim] += gradAttentionOutput[pos][outDim] * transformer.attentionWeights[layer].Wo[preDim][outDim] * specWeight;
                        }
                    }
                }

                const d_v = Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(headSize).fill(0)));
                const d_scores = Array(this.#numHeads).fill().map(() => Array(this.#inputSize).fill().map(() => Array(this.#inputSize).fill(0)));
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let h = 0; h < this.#numHeads; h++) {
                        const offset = h * headSize;
                        for (let dim = 0; dim < headSize; dim++) {
                            const fullDim = offset + dim;
                            const d_att = d_preWo[pos][fullDim];
                            if (!isValidNumber(d_att)) continue;
                            for (let vPos = 0; vPos < this.#inputSize; vPos++) {
                                d_scores[h][pos][vPos] += d_att * inter.V[vPos][fullDim];
                                d_v[h][vPos][dim] += d_att * inter.attentionProbs[h][pos][vPos];
                            }
                        }
                    }
                }

                for (let h = 0; h < this.#numHeads; h++) {
                    for (let pos = 0; pos < this.#inputSize; pos++) {
                        let weighted = 0;
                        for (let vPos = 0; vPos < this.#inputSize; vPos++) {
                            weighted += inter.attentionProbs[h][pos][vPos] * d_scores[h][pos][vPos];
                        }
                        for (let vPos = 0; vPos < this.#inputSize; vPos++) {
                            d_scores[h][pos][vPos] = inter.attentionProbs[h][pos][vPos] * (d_scores[h][pos][vPos] - weighted);
                        }
                    }
                }

                const d_q = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
                const d_k = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
                for (let h = 0; h < this.#numHeads; h++) {
                    for (let qPos = 0; qPos < this.#inputSize; qPos++) {
                        for (let kPos = 0; kPos < this.#inputSize; kPos++) {
                            const scaled = d_scores[h][qPos][kPos] / Math.sqrt(headSize);
                            if (!isValidNumber(scaled)) continue;
                            for (let dim = 0; dim < headSize; dim++) {
                                const fullDim = h * headSize + dim;
                                d_q[qPos][fullDim] += scaled * inter.K[kPos][fullDim];
                                d_k[kPos][fullDim] += scaled * inter.Q[qPos][fullDim];
                            }
                        }
                    }
                }

                let gradNormX = Array(this.#inputSize).fill().map(() => Array(this.#hiddenSize).fill(0));
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    for (let inDim = 0; inDim < this.#hiddenSize; inDim++) {
                        let contribQ = 0, contribK = 0, contribV = 0;
                        for (let outDim = 0; outDim < this.#hiddenSize; outDim++) {
                            const specWeight = isValidNumber(this.#specializationWeights[idx][inDim % this.#hiddenSize][outDim])
                                ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][inDim % this.#hiddenSize][outDim], 0.5), 1.5)
                                : 1;
                            contribQ += d_q[pos][outDim] * transformer.attentionWeights[layer].Wq[inDim][outDim] * specWeight;
                            contribK += d_k[pos][outDim] * transformer.attentionWeights[layer].Wk[inDim][outDim] * specWeight;
                            contribV += d_v[Math.floor(outDim / headSize)][pos][outDim % headSize] * transformer.attentionWeights[layer].Wv[inDim][outDim] * specWeight;
                        }
                        gradNormX[pos][inDim] = contribQ + contribK + contribV;
                    }
                }

                for (let inDim = 0; inDim < this.#hiddenSize; inDim++) {
                    for (let outDim = 0; outDim < this.#hiddenSize; outDim++) {
                        let wqUpdate = 0, wkUpdate = 0, wvUpdate = 0;
                        const specWeight = isValidNumber(this.#specializationWeights[idx][inDim % this.#hiddenSize][outDim])
                            ? Math.min(Math.max(1 + this.#specializationScores[idx] * this.#specializationWeights[idx][inDim % this.#hiddenSize][outDim], 0.5), 1.5)
                            : 1;
                        for (let pos = 0; pos < this.#inputSize; pos++) {
                            wqUpdate += d_q[pos][outDim] * act.normX[pos][inDim];
                            wkUpdate += d_k[pos][outDim] * act.normX[pos][inDim];
                            const h = Math.floor(outDim / headSize);
                            const dim = outDim % headSize;
                            wvUpdate += d_v[h][pos][dim] * act.normX[pos][inDim];
                        }
                        if (isValidNumber(wqUpdate)) this.#gradientAccumulation[idx].attentionWeights[layer].Wq[inDim][outDim] += (1 - momentum) * wqUpdate * specWeight * adjustedLearningRate;
                        if (isValidNumber(wkUpdate)) this.#gradientAccumulation[idx].attentionWeights[layer].Wk[inDim][outDim] += (1 - momentum) * wkUpdate * specWeight * adjustedLearningRate;
                        if (isValidNumber(wvUpdate)) this.#gradientAccumulation[idx].attentionWeights[layer].Wv[inDim][outDim] += (1 - momentum) * wvUpdate * specWeight * adjustedLearningRate;
                    }
                }

                const gamma1 = transformer.layerNormWeights[layer].gamma1;
                for (let pos = 0; pos < this.#inputSize; pos++) {
                    const x = act.normX[pos];
                    const sq_sum = x.reduce((s, v) => s + v * v, 0);
                    const rms = Math.sqrt(sq_sum / this.#hiddenSize + 1e-6);
                    const norm_x = x.map(v => v / rms);

                    const incoming = gradNormX[pos];
                    const d_gamma = incoming.map((g, i) => g * norm_x[i]);
                    for (let j = 0; j < this.#hiddenSize; j++) {
                        if (isValidNumber(d_gamma[j])) {
                            const update = d_gamma[j] * adjustedLearningRate * 0.2;
                            this.#gradientAccumulation[idx].layerNormWeights[layer].gamma1[j] += (1 - momentum) * update;
                        }
                    }

                    const d_norm = incoming.map((g, i) => g * gamma1[i]);
                    const dot = d_norm.reduce((s, dn, i) => s + dn * norm_x[i], 0);
                    const d_pre = d_norm.map((dn, i) => dn / rms - norm_x[i] * dot / (this.#hiddenSize + 1e-8));

                    for (let j = 0; j < this.#hiddenSize; j++) {
                        grad[pos][j] = gradToAttentionResidual[pos][j] + d_pre[j];
                    }
                }
            }
        });
    }

    predict (inputs) {
        if ( !Array.isArray(inputs) || inputs.length !== this.#inputSize || !inputs.every(isValidNumber) ) { return 0 }

        const predictionResult = this.#updateHiveState(inputs, null, false, false, false, true);
        return predictionResult.probability;
    }

    train (inputs, target) {
        if ( !Array.isArray(inputs) || inputs.length !== this.#inputSize || !inputs.every(isValidNumber) || !isValidNumber(target) ) { return }

        this.#trainingStepCount++;

        const trainingResults = this.#updateHiveState(inputs, target, true, true, true, true);

        this.#accumulateGradients(
            inputs, trainingResults.outputs, target, trainingResults.probability, 
            trainingResults.layerOutputs, trainingResults.activations, trainingResults.attentionIntermediates
        );

        if (this.#trainingStepCount % this.#gradientResetFrequency === 0) {
            const clonedGrads = this.#applyGradients(true, true, this.#gradientResetFrequency);

            this.#gradientAccumulation = structuredClone(clonedGrads);

            const freshResults = this.#updateHiveState(inputs, target , true, false, true, true);

            this.#distillKnowledge(
                freshResults.outputs, freshResults.attentionIntermediates, 
                freshResults.activations, freshResults.layerOutputs
            );

            this.#rollbackGradients(clonedGrads);

            this.#applyGradients(false, false, 1);

            this.#updateHiveState(inputs, target, false, false, true, false);

            this.#hiveMemorySharing();

            this.#gradientAccumulation = this.#setGradientStructure();
        }

        return this.#trainingStepCount
    }

    dumpState () {
        return this.#saveState();
    }
}

export default HiveMind;