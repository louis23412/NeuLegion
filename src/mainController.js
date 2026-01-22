import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

const trainingFile = path.join(import.meta.dirname, 'candles.jsonl');

const hiveLayers = {};
const cache = [];

const NUM_CONTROLLERS_PER_LAYER = [2, 2, 4, 8];
const layerBaseWeights = [10, 9, 8, 7];
const BASE_POPULATION = 32;
const CACHE_SIZE = 250;
const MAX_CONCURRENT_WORKERS = 2;

let layer = 0;

console.log('Initializing...');
for (const numControllers of NUM_CONTROLLERS_PER_LAYER) {
    hiveLayers[layer] = [];

    const half = Math.floor(numControllers / 2);

    for (let id = 0; id < numControllers; id++) {
        const directoryPath = path.join(import.meta.dirname, '..', 'state', `Layer${layer}`, `L${layer}C${id}`);

        const type = id < half ? 'positive' : 'negative';

        hiveLayers[layer].push({
            id,
            type,
            directoryPath,
            layer,
            signalSpeed: 0,
            lastSignal: {},
            pop : BASE_POPULATION * (layer + 1)
        });
    }

    layer++;
}

let totalPop = 0;
for (const l in hiveLayers) {
    const controllers = hiveLayers[l];
    const num = controllers.length;
    const popPerController = controllers[0].pop;
    const layerPop = num * popPerController;

    totalPop += layerPop;

    console.log(`Hive layer ${l} READY! [${num} controllers × ${popPerController} population] - Layer population = ${layerPop}`);
}

console.log(`Total Population: ${totalPop}`);
console.log('--------------------------------------------------');

let totalWeight = 0;

for (const layerIdx in hiveLayers) {
    const base = layerBaseWeights[layerIdx];
    const controllers = hiveLayers[layerIdx];
    for (const controller of controllers) {
        controller.weight = base;
    }
    totalWeight += controllers.length * base;
}

const runWorker = async (co, cache) => {
    return new Promise((resolve, reject) => {
        const worker = new Worker(new URL('./worker.js', import.meta.url), {
            workerData: {
                id: `L${co.layer}C${co.id}`,
                directoryPath: co.directoryPath,
                cacheSize: CACHE_SIZE,
                populationPerController: co.pop,
                cache,
                type : co.type
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
}

const processBatch = async (counter) => {
    const allControllers = Object.values(hiveLayers).flat();

    console.log(`New batch(${counter}): Processing controllers...`);

    const totalStart = performance.now();
    let progressTracker = 0;
    const results = [];

    for (let index = 0; index < allControllers.length; index += MAX_CONCURRENT_WORKERS) {
        const chunk = allControllers.slice(index, index + MAX_CONCURRENT_WORKERS);
        const promises = chunk.map(co => runWorker(co, cache));

        let chunkResults;
        try {
            chunkResults = await Promise.all(promises);
        } catch (err) {
            console.error('Error during parallel processing:', err);
            process.exit(1);
        }

        chunkResults.forEach(result => {
            if (progressTracker !== 0) {
                process.stdout.moveCursor(0, -1);
                process.stdout.clearScreenDown();
            }

            progressTracker++;

            console.log(`Processed ${progressTracker}/${allControllers.length} controllers (${((progressTracker / allControllers.length) * 100).toFixed(2)}%)...`);

            results.push(result);
        });
    }

    const totalEnd = performance.now();
    console.log(`Batch completed in ${((totalEnd - totalStart) / 1000).toFixed(2)} seconds`);

    for (const { co, signal, duration } of results) {
        co.lastSignal = signal;
        co.signalSpeed = duration;
    }
}

let counter = 0;
const Y = '\x1b[33m';
const X = '\x1b[0m';
const G = '\x1b[32m';
const R = '\x1b[31m';
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
        if (cache.length > CACHE_SIZE) {
            cache.shift();
        }

        if (cache.length === CACHE_SIZE) {
            counter++;

            await processBatch(counter);

            console.log('--');
            for (const layer in hiveLayers) {
                for (const cluster of hiveLayers[layer]) {
                    const contribution = (cluster.weight / totalWeight * 100).toFixed(3);
                    console.log(`Cluster ${Y}L${layer}C${cluster.id}P${cluster.pop}${X} [ Prob ${Y}${cluster.lastSignal.prob}${X} % - cont ${Y}${contribution}${X} % - type : ${cluster.type === 'positive' ? `${G}positive${X}` : `${R}negative${X}`}] => Signal speed(s) : ${Y}${(cluster.signalSpeed / 1000).toFixed(3)}${X} | Steps : ${Y}${cluster.lastSignal.lastTrainingStep}${X} | Skipped : ${Y}${cluster.lastSignal.skippedTraining}${X} | Simulations : ${Y}${cluster.lastSignal.openSimulations}${X} | Pending : ${Y}${cluster.lastSignal.pendingClosedTrades}${X}`)
                }
                console.log('--');
            }

            console.log('--------------------------------------------------');
        }
    }

    console.log('End of file reached. Processing complete.');
}

processCandles().catch(err => {
    console.error('Unexpected error during processing:', err);
});