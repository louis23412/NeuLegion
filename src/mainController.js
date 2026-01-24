import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

const trainingFile = path.join(import.meta.dirname, 'candles.jsonl');

const cache = [];

const trainingCutoff = 198;

const NUM_CONTROLLERS_PER_LAYER = [2, 2, 2, 2];
const layerBaseWeights = [5, 6, 7, 8];
const BASE_POPULATION = 64;
const BASE_CACHE_SIZE = 250;
const LAYER_CACHE_INCREMENT = 250;
const MAX_CONCURRENT_WORKERS = 2;

const baseAtrFactor = 2.5;
const baseStopFactor = 1;
const minPriceMovement = 0.0021;
const maxPriceMovement = 0.05;
const PRICE_LAYER_BOOST = 0.15;

const Y = '\x1b[33m';
const X = '\x1b[0m';
const G = '\x1b[32m';
const R = '\x1b[31m';
const M = '\x1b[35m';
const C = '\x1b[36m';

const NUM_LAYERS = NUM_CONTROLLERS_PER_LAYER.length;
const MAX_LAYER = NUM_LAYERS - 1;
const MAX_CACHE_SIZE = BASE_CACHE_SIZE + LAYER_CACHE_INCREMENT * MAX_LAYER;

const getLayerParams = (layer) => {
    const factor = 1 + PRICE_LAYER_BOOST * layer;
    return {
        cacheSize: BASE_CACHE_SIZE + LAYER_CACHE_INCREMENT * layer,
        atrFactor: Number((baseAtrFactor * factor).toFixed(3)),
        stopFactor: Number((baseStopFactor * factor).toFixed(3)),
        minPriceMovement: Number((minPriceMovement * factor).toFixed(3)),
        maxPriceMovement: Number((maxPriceMovement * factor).toFixed(3)),
    };
};

const hiveLayers = [];
let totalPop = 0;
let totalWeight = 0;

console.log('Initializing...');
for (let layer = 0; layer < NUM_LAYERS; layer++) {
    const numControllers = NUM_CONTROLLERS_PER_LAYER[layer];
    const baseWeight = layerBaseWeights[layer];
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
            pop: BASE_POPULATION,
            weight: baseWeight,
        });
    }

    hiveLayers.push(controllers);

    const layerPop = numControllers * BASE_POPULATION;
    totalPop += layerPop;
    totalWeight += numControllers * baseWeight;

    console.log(`Hive layer ${layer} READY! [${numControllers} controllers × ${BASE_POPULATION} population] - Layer population = ${layerPop}`);
}

console.log(`Total Population: ${totalPop}`);
console.log('--------------------------------------------------');

const allControllers = hiveLayers.flat();

const runWorker = async (co) => {
    const { cacheSize, atrFactor, stopFactor, minPriceMovement, maxPriceMovement } = getLayerParams(co.layer);
    const layerCache = cache.slice(-cacheSize);

    return new Promise((resolve, reject) => {
        const worker = new Worker(new URL('./worker.js', import.meta.url), {
            workerData: {
                id: `L${co.layer}C${co.id}`,
                directoryPath: co.directoryPath,
                cacheSize,
                populationPerController: co.pop,
                cache: layerCache,
                type: co.type,
                priceObj: {
                    atrFactor,
                    stopFactor,
                    minPriceMovement,
                    maxPriceMovement,
                },
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
    console.log(`New batch(${Y}${counter}${X}):`);

    const totalStart = performance.now();
    let progressTracker = 0;
    const results = [];

    console.log(`Processed ${Y}${progressTracker}${X}/${Y}${allControllers.length}${X} controllers (${Y}${((progressTracker / allControllers.length) * 100).toFixed(2)}${X}%)...`);

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
            console.log(`Processed ${Y}${progressTracker}${X}/${Y}${allControllers.length}${X} controllers (${Y}${((progressTracker / allControllers.length) * 100).toFixed(2)}${X}%)...`);

            results.push(result);
        });
    }

    const totalEnd = performance.now();
    console.log(`Batch completed in ${Y}${((totalEnd - totalStart) / 1000).toFixed(3)}${X} seconds`);

    for (const { co, signal, duration } of results) {
        co.lastSignal = signal;
        co.signalSpeed = duration;
    }
};

let counter = 0;
const processCandles = async () => {
    const fileStream = fs.createReadStream(trainingFile);
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

                for (const cluster of controllers) {
                    const contribution = (cluster.weight / totalWeight * 100).toFixed(3);
                    const clusterId = `L${layer}C${cluster.id}P${cluster.pop}`;
                    const color = cluster.type === 'positive' ? G : R;
                    console.log(`Cluster ${color}${clusterId}${X} [ Prob ${C}${cluster.lastSignal.prob}${X} % - cont ${Y}${contribution}${X} %] => Speed(s) : ${M}${(cluster.signalSpeed / 1000).toFixed(3)}${X} | Steps : ${Y}${cluster.lastSignal.lastTrainingStep}${X} | Skipped : ${Y}${cluster.lastSignal.skippedTraining}${X} | Simulations : ${Y}${cluster.lastSignal.openSimulations}${X} | Pending : ${Y}${cluster.lastSignal.pendingClosedTrades}${X}`);
                }

                const { cacheSize, atrFactor, stopFactor, minPriceMovement, maxPriceMovement } = getLayerParams(layer);
                console.log(`Cache : ${Y}${cacheSize}${X} | ATR : ${Y}${atrFactor}${X} | Stop : ${Y}${stopFactor}${X} | Min price move : ${Y}${(minPriceMovement * 100).toFixed(3)}${X}% | Max price move : ${Y}${(maxPriceMovement * 100).toFixed(3)}${X}%`);
                console.log('--');
            }

            console.log('Overall Stats:');

            let buyStrength = 0;
            let sellStrength = 0;
            let posWeight = 0;
            let negWeight = 0;

            allControllers.forEach(co => {
                const prob = co.lastSignal.prob ?? 0;
                if (prob < 0 || prob > 100) return;

                const params = getLayerParams(co.layer);
                const depthFactor = params.atrFactor / baseAtrFactor;
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

            console.log(`Weighted Average Buy / Sell Signal Probabilities: ${G}${weightedAvgBuyProb.toFixed(3)}${X}% - ${R}${weightedAvgSellProb.toFixed(3)}${X}%`);

            const netDiff = weightedAvgBuyProb - weightedAvgSellProb;
            const finalBuyProb = 50 + (netDiff / 2);
            const finalSellProb = 100 - finalBuyProb;

            console.log(`Final Buy / Sell Probabilities: ${G}${finalBuyProb.toFixed(3)}${X}% - ${R}${finalSellProb.toFixed(3)}${X}%`);

            const finalSignal = finalBuyProb > 50 ? `${G}BUY${X}` : finalBuyProb < 50 ? `${R}SELL${X}` : `${Y}HOLD${X}`;
            console.log(`Final Signal: ${finalSignal}`);

            console.log('--------------------------------------------------');

            if (counter === trainingCutoff) {
                console.log(`Done! Cutoff = ${trainingCutoff}`);
                process.exit();
            }
        }
    }

    console.log('End of file reached. Processing complete.');
};

processCandles().catch(err => {
    console.error('Unexpected error during processing:', err);
});