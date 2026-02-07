import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';
import { availableParallelism } from 'node:os';

const cache = [];
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
    layerPriceBoost: 0.15
};

let structureMap = Array.from({ length: CONFIG.dims[0] }, (_, group) =>
    Array.from({ length: CONFIG.dims[1] }, (_, section) =>
        Array.from({ length: CONFIG.dims[2] }, (_, layer) =>
            Array.from({ length: CONFIG.dims[3] }, (_, cluster) => {
                const half = Math.floor(CONFIG.dims[3] / 2);
                const directoryPath = path.join(CONFIG.stateFolder, `Group${group}`, `Section${section}`, `Layer${layer}`, `G${group}S${section}L${layer}C${cluster}`);

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

const processBatch = async () => {
    console.log(`New batch(${candleCounter}):`);

    const totalStart = performance.now();
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

    const peerSharedMem = structureMap.flat(3).filter((c) => {
        const peerId = `${c.group}${c.section}${c.layer}${c.id}`;
        const peerCompat = c.lastSignal?.memoryBroadcast?.compatibility;

        if (
            mainCompat &&
            mainId !== peerId &&
            c.type === controller.type &&
            JSON.stringify(mainCompat ?? {}) === JSON.stringify(peerCompat ?? {})
        ) { return c }
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

const saveLegionState = async () => {
    fs.writeFileSync(path.join(CONFIG.stateFolder, 'legionState.json'), JSON.stringify({
        candleCounter,
        legionState : structureMap
    }));
}

const loadLegionState = () => {
    const fileName = path.join(CONFIG.stateFolder, 'legionState.json');

    if (!fs.existsSync(fileName)) {
        return;
    }

    const rawData = fs.readFileSync(fileName)
    const loadedData = JSON.parse(rawData)

    structureMap = loadedData.legionState;
    return loadedData.candleCounter;
}

const processCandles = async () => {
    const fileStream = fs.createReadStream(CONFIG.file);
    const rd = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity,
    });

    let rebuildCounter = loadLegionState();

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
            cache.shift();
        }

        if (cache.length === maxCache) {
            candleCounter++;
            if (candleCounter <= rebuildCounter) continue;

            await processBatch();
            await saveLegionState();

            console.log('-------------------------------------------------------');

            if (candleCounter % CONFIG.cutoff === 0) {
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