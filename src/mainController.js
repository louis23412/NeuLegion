import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

const trainingFile = path.join(import.meta.dirname, 'candles.jsonl');

const hiveLayers = {};
const cache = [];

const NUM_CONTROLLERS_PER_LAYER = [2, 2, 4, 8];
const POPULATION_PER_CONTROLLER = 64;
const CACHE_SIZE = 250;

let layer = 0;

console.log('Initializing...');
for (const numControllers of NUM_CONTROLLERS_PER_LAYER) {
    hiveLayers[layer] = [];

    for (let id = 0; id < numControllers; id++) {
        const directoryPath = path.join(import.meta.dirname, '..', 'hive-clusters-states', `C${layer}ID${id}`);

        hiveLayers[layer].push({
            id,
            directoryPath,
            layer,
            signalSpeed: 0,
            lastSignal: {},
        });
    }

    layer++;
}

let totalPop = 0;
for (const l in hiveLayers) {
    const num = NUM_CONTROLLERS_PER_LAYER[l];
    const pop = num * POPULATION_PER_CONTROLLER;
    totalPop += pop;
    console.log(`Hive layer ${l} READY! [${num} controllers × ${POPULATION_PER_CONTROLLER} population] - Layer population = ${pop}`);
}

console.log(`Total Population: ${totalPop}`);
console.log('--------------------------------------------------');

const processCandles = () => {
    const rd = readline.createInterface({
        input: fs.createReadStream(trainingFile),
    });

    let hasProcessed = false;

    rd.on('line', (line) => {
        if (!line.trim()) return;

        const candle = JSON.parse(line);
        cache.push(candle);

        if (cache.length > CACHE_SIZE) {
            cache.shift();
        }

        if (cache.length >= CACHE_SIZE && !hasProcessed) {
            hasProcessed = true;

            const allControllers = Object.values(hiveLayers).flat();

            console.log(`Cache ready (${CACHE_SIZE} candles). Processing ${allControllers.length} controllers in parallel using worker threads...`);

            const totalStart = performance.now();

            const promises = allControllers.map((co) => {
                return new Promise((resolve, reject) => {
                    const worker = new Worker(new URL('./worker.js', import.meta.url), {
                        workerData: {
                            directoryPath: co.directoryPath,
                            cacheSize: CACHE_SIZE,
                            populationPerController: POPULATION_PER_CONTROLLER,
                            cache,
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
            });

            Promise.all(promises)
                .then((results) => {
                    const totalEnd = performance.now();
                    console.log(`All workers completed in ${((totalEnd - totalStart) / 1000).toFixed(2)} seconds`);

                    for (const { co, signal, duration } of results) {
                        co.lastSignal = signal;
                        co.signalSpeed = duration;
                    }

                    const aggregated = [];

                    for (const layerKey in hiveLayers) {
                        for (const co of hiveLayers[layerKey]) {
                            if (co.lastSignal && co.signalSpeed > 0) {
                                aggregated.push({
                                    confidence: co.lastSignal.confidence,
                                    speed: co.signalSpeed,
                                    layer: co.layer,
                                });
                            }
                        }
                    }

                    if (aggregated.length === 0) {
                        console.log('No valid signals received.');
                        process.exit(0);
                    }

                    const minSpeed = Math.min(...aggregated.map((c) => c.speed));

                    let totalWeightedConf = 0;
                    let totalWeight = 0;

                    for (const ctrl of aggregated) {
                        const speedScore = minSpeed / ctrl.speed;
                        const layerScore = ctrl.layer + 1;

                        const weight = speedScore + layerScore;

                        totalWeightedConf += ctrl.confidence * weight;
                        totalWeight += weight;
                    }

                    const finalConfidence = totalWeight > 0 ? totalWeightedConf / totalWeight : 0;

                    console.log(`Final confidence: ${finalConfidence.toFixed(3)}%`);
                    process.exit(0);
                })
                .catch((err) => {
                    console.error('Error during parallel processing:', err);
                    process.exit(1);
                });
        }
    });
};

processCandles();