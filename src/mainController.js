import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';
import { availableParallelism } from 'node:os';

const TRAINING_FILE = path.join(import.meta.dirname, 'candles.jsonl');

const TRAINING_CUTOFF = null;

const BASE_PROCESS_COUNT = 1;
const FORCE_MINIMAL_DIMENSIONS = true;
const MAX_CONCURRENT_WORKERS = Math.max(1, Math.floor(availableParallelism() * 0.25));

const LAYER_DIMENSIONS = [16, 2];
const NUM_CONTROLLERS_PER_LAYER = new Array(LAYER_DIMENSIONS[0]).fill(LAYER_DIMENSIONS[1]);
const MIN_LAYER_WEIGHT = 3;
const MAX_LAYER_WEIGHT = 7;
const SIZE_BONUS_FACTOR = 0.15;

const INPUT_LAYER_BOOST = 0.15;

const BASE_POPULATION = 64;
const POP_LAYER_BOOST = 0.15;

const BASE_CACHE_SIZE = 250;
const CACHE_LAYER_BOOST = 0.5;

const BASE_ATR_FACTOR = 2.5;
const BASE_STOP_FACTOR = 1;
const MIN_PRICE_MOVE = 0.0021;
const MAX_PRICE_MOVE = 0.05;
const PRICE_LAYER_BOOST = 0.10;

const Y = '\x1b[33m';
const X = '\x1b[0m';
const G = '\x1b[32m';
const R = '\x1b[31m';
const M = '\x1b[35m';
const C = '\x1b[36m';

const cache = [];
const hiveLayers = [];

const NUM_LAYERS = NUM_CONTROLLERS_PER_LAYER.length;
const MAX_LAYER = NUM_LAYERS - 1;
const MAX_CACHE_SIZE = Math.round(BASE_CACHE_SIZE * (1 + CACHE_LAYER_BOOST * MAX_LAYER));

let tempBaseWeights = [];
if (NUM_LAYERS === 1) {
    tempBaseWeights = [MIN_LAYER_WEIGHT];
} else {
    const increment = (MAX_LAYER_WEIGHT - MIN_LAYER_WEIGHT) / (NUM_LAYERS - 1);
    const avgControllers = NUM_CONTROLLERS_PER_LAYER.reduce((a, b) => a + b, 0) / NUM_LAYERS;

    for (let layer = 0; layer < NUM_LAYERS; layer++) {
        const rawWeight = MIN_LAYER_WEIGHT + layer * increment;
        const sizeFactor = 1 + SIZE_BONUS_FACTOR * (NUM_CONTROLLERS_PER_LAYER[layer] / avgControllers - 1);
        tempBaseWeights.push(Number((rawWeight * sizeFactor).toFixed(3)));
    }
}

const LAYER_BASE_WEIGHTS = tempBaseWeights;

for (let layer = 0; layer < NUM_LAYERS; layer++) {
    const numControllers = NUM_CONTROLLERS_PER_LAYER[layer];
    const baseWeight = LAYER_BASE_WEIGHTS[layer];
    const controllers = [];
    const half = Math.floor(numControllers / 2);

    for (let id = 0; id < numControllers; id++) {
        const type = id < half ? 'positive' : 'negative';
        const directoryPath = path.join(import.meta.dirname, '..', 'state', `Layer${layer}`, `L${layer}C${id}`);

        controllers.push({
            id,
            type,
            directoryPath,
            layer,
            signalSpeed: 0,
            lastSignal: {},
            weight: baseWeight
        });
    }

    hiveLayers.push(controllers);
}

console.log('--------------------------------------------------');

const allControllers = hiveLayers.flat();

const getLayerParams = (layer) => {
    const reversedLayer = NUM_LAYERS - 1 - layer;
    const cacheFactor = 1 + CACHE_LAYER_BOOST * layer;
    const moneyFactor = 1 + PRICE_LAYER_BOOST * layer;
    const popFactor = 1 + POP_LAYER_BOOST * reversedLayer;

    return {
        cacheSize: Math.round(BASE_CACHE_SIZE * cacheFactor),
        atrFactor: Number((BASE_ATR_FACTOR * moneyFactor).toFixed(3)),
        stopFactor: Number((BASE_STOP_FACTOR * moneyFactor).toFixed(3)),
        minPriceMovement: Number((MIN_PRICE_MOVE * moneyFactor).toFixed(3)),
        maxPriceMovement: Number((MAX_PRICE_MOVE * moneyFactor).toFixed(3)),
        inputMult : Number((INPUT_LAYER_BOOST * layer).toFixed(3)),
        pop : Math.round(BASE_POPULATION * popFactor)
    };
};

const runWorker = async (co) => {
    const { cacheSize, atrFactor, stopFactor, minPriceMovement, maxPriceMovement, inputMult, pop } = getLayerParams(co.layer);
    const layerCache = cache.slice(-cacheSize);

    return new Promise((resolve, reject) => {
        const worker = new Worker(new URL('./worker.js', import.meta.url), {
            workerData: {
                id: `L${co.layer}C${co.id}`,
                directoryPath: co.directoryPath,
                cacheSize,
                populationPerController: pop,
                cache: layerCache,
                type: co.type,
                priceObj: {
                    atrFactor,
                    stopFactor,
                    minPriceMovement,
                    maxPriceMovement,
                },
                inputMult,
                processCount : BASE_PROCESS_COUNT,
                forceMin : FORCE_MINIMAL_DIMENSIONS
            },
        });

        worker.on('message', (msg) => {
            worker.terminate();
            if (msg.error) {
                reject(new Error(msg.error));
            } else {
                resolve({
                    co,
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

const processBatch = async (counter) => {
    console.log(`New batch(${C}${counter}${X}):`);

    const totalStart = performance.now();
    let progressTracker = 0;
    const results = [];

    console.log(`Processed ${C}${progressTracker}${X}/${C}${allControllers.length}${X} controllers (${C}${((progressTracker / allControllers.length) * 100).toFixed(2)}${X}%)...`);

    for (let index = 0; index < allControllers.length; index += MAX_CONCURRENT_WORKERS) {
        const chunk = allControllers.slice(index, index + MAX_CONCURRENT_WORKERS);
        const promises = chunk.map(co => runWorker(co));

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
            console.log(`Processed ${C}${progressTracker}${X}/${C}${allControllers.length}${X} controllers (${C}${((progressTracker / allControllers.length) * 100).toFixed(2)}${X}%)...`);

            results.push(result);
        });
    }

    const totalEnd = performance.now();
    console.log(`Batch completed in ${C}${((totalEnd - totalStart) / 1000).toFixed(3)}${X} seconds`);

    for (const { co, signal, duration } of results) {
        co.lastSignal = signal;
        co.signalSpeed = duration;
    }
};

let maxLayerIdLen = 0;
let maxControllerIdLen = 0;

for (let layer = 0; layer < hiveLayers.length; layer++) {
    const plainLayerId = `L${layer}`;
    maxLayerIdLen = Math.max(maxLayerIdLen, plainLayerId.length);

    const controllers = hiveLayers[layer];
    for (const co of controllers) {
        const controllerId = `L${layer}C${co.id}`;
        maxControllerIdLen = Math.max(maxControllerIdLen, controllerId.length);
    }
}

let counter = 0;
const processCandles = async () => {
    const fileStream = fs.createReadStream(TRAINING_FILE);
    const rd = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity,
    });

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
        if (cache.length > MAX_CACHE_SIZE) {
            cache.shift();
        }

        if (cache.length === MAX_CACHE_SIZE) {
            counter++;
            await processBatch(counter);

            console.log('--');
            for (let layer = 0; layer < hiveLayers.length; layer++) {
                const controllers = hiveLayers[layer];

                const blocks = [];
                for (const cluster of controllers) {
                    const idStr = `L${layer}C${cluster.id}`.padStart(maxControllerIdLen, ' ');
                    const probNum = cluster.lastSignal.prob ?? 0;
                    const probStr = probNum.toFixed(3).padStart(7, ' ');
                    const speedStr = (cluster.signalSpeed / 1000).toFixed(3).padStart(7, ' ');

                    const colorBracket = (brckt) => cluster.type === 'positive' ? `${G}${brckt}${X}` : `${R}${brckt}${X}`;

                    const inner = `${C}${idStr}${X}: ${M}${probStr}${X}% | ${Y}${speedStr}${X}(s)`;
                    const block = `${colorBracket('[')}${inner}${colorBracket(']')}`;

                    blocks.push(block);
                }

                const plainLayerId = `L${layer}`;
                const paddedLayerId = plainLayerId.padStart(maxLayerIdLen, ' ');

                console.log(`${C}${paddedLayerId}${X} =>  ${blocks.join('  ')}`);
            }

            console.log('--')

            let buyStrength = 0;
            let sellStrength = 0;
            let posWeight = 0;
            let negWeight = 0;

            allControllers.forEach(co => {
                const prob = co.lastSignal.prob ?? 0;
                if (prob < 0 || prob > 100) return;

                const params = getLayerParams(co.layer);
                const depthFactor = params.atrFactor / BASE_ATR_FACTOR;
                const confidenceExcess = Math.max(0, (prob - 50) / 50);
                const boost = confidenceExcess * depthFactor * 2.0;
                const effectiveWeight = co.weight * (1 + boost);

                if (co.type === 'positive') {
                    buyStrength += effectiveWeight * prob;
                    posWeight += effectiveWeight;
                } else {
                    sellStrength += effectiveWeight * prob;
                    negWeight += effectiveWeight;
                }
            });

            const weightedAvgBuyProb = posWeight > 0 ? (buyStrength / posWeight) : 0;
            const weightedAvgSellProb = negWeight > 0 ? (sellStrength / negWeight) : 0;

            const netDiff = weightedAvgBuyProb - weightedAvgSellProb;
            const finalBuyProb = 50 + (netDiff / 2);
            const finalSellProb = 100 - finalBuyProb;

            const predictDiff = Number((finalBuyProb > 50 ? finalBuyProb - 50 : 50 - finalSellProb).toFixed(3));
            const predictDiffColored = predictDiff > 0 ? `${G}${predictDiff}${X}` : predictDiff < 0 ? `${R}${predictDiff}${X}` : `${C}${predictDiff}${X}`;

            console.log(`Final Buy / Sell Probabilities: ${M}${finalBuyProb.toFixed(3)}${X}% - ${M}${finalSellProb.toFixed(3)}${X}% (${predictDiffColored} %)`);
            console.log('--------------------------------------------------');

            if (counter === TRAINING_CUTOFF) {
                console.log(`Done! Cutoff = ${TRAINING_CUTOFF}`);
                process.exit();
            }
        }
    }

    console.log('End of file reached. Processing complete.');
};

processCandles().catch(err => {
    console.error('Unexpected error during processing:', err);
});