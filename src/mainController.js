import fs from 'fs';
import path from 'path';
import readline from 'readline';
import { Worker } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';
import { availableParallelism } from 'node:os';

const cache = [];
let candleCounter = 0;

const CONFIG = {
    cutoff: 1,
    baseProcessCount: 1,
    forceMin: true,
    maxWorkers: Math.max(1, Math.floor(availableParallelism() * 0.25)),
    file: path.join(import.meta.dirname, 'candles.jsonl'),
    stateFolder : path.join(import.meta.dirname, '..', 'state'),

    dims: [4, 4, 4, 4],

    groupInputBoost: 0.15,
    sectionInputBoost: 0.25,
    layerInputBoost: 0.35,

    groupPopBoost: 0.05,
    sectionPopBoost: 0.15,
    layerPopBoost: 0.25,

    groupCacheBoost: 0.15,
    sectionCacheBoost: 0.25,
    layerCacheBoost: 0.35,

    groupPriceBoost: 0.05,
    sectionPriceBoost: 0.10,
    layerPriceBoost: 0.15,

    basePop: 64,
    baseCache: 250,

    baseAtr: 1.5,
    baseStop: 0.75,
    minPriceMove: 0.0021,
    maxPriceMove: 0.05,

    scoreHistory : 100,
    signalHistory : 100
};

let structureMap = Array.from({ length: CONFIG.dims[0] }, (_, group) =>
    Array.from({ length: CONFIG.dims[1] }, (_, section) =>
        Array.from({ length: CONFIG.dims[2] }, (_, layer) =>
            Array.from({ length: CONFIG.dims[3] }, (_, cluster) => {
                const half = Math.floor(CONFIG.dims[3] / 2);
                const directoryPath = path.join(CONFIG.stateFolder, `Group${group}`, `Section${section}`, `Layer${layer}`, `G${group}S${section}L${layer}C${cluster}`);

                return {
                    id: cluster,
                    type: cluster < half ? 'positive' : 'negative',
                    directoryPath,
                    group,
                    section,
                    layer,
                    signalSpeed: 0,
                    lastSignal: {},
                    cont: 0,
                    scoreHistory : [],
                    signalHistory : []
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

    const inputMult = Number((CONFIG.groupInputBoost * group + CONFIG.sectionInputBoost * section + CONFIG.layerInputBoost * layer).toFixed(3));

    return {
        cacheSize: Math.round(CONFIG.baseCache * cacheFactor),
        atrFactor: Number((CONFIG.baseAtr * moneyFactor).toFixed(3)),
        stopFactor: Number((CONFIG.baseStop * moneyFactor).toFixed(3)),
        minPriceMovement: Number((CONFIG.minPriceMove * moneyFactor).toFixed(3)),
        maxPriceMovement: Number((CONFIG.maxPriceMove * moneyFactor).toFixed(3)),
        inputMult,
        pop: Math.round(CONFIG.basePop * popFactor)
    };
};

const adjustAllControllers = () => {
    const allControllers = structureMap.flat(3);

    allControllers.forEach(controller => {
        const params = getControllerParams(controller.group, controller.section, controller.layer);

        const score = controller.lastSignal.score || 0;

        const cacheBoost = params.cacheSize / CONFIG.baseCache;
        const moneyBoost = params.atrFactor / CONFIG.baseAtr;
        const inputBoost = 1 + params.inputMult;
        const popBoost = CONFIG.basePop / params.pop;

        const totalBoost = cacheBoost * moneyBoost * inputBoost * popBoost;

        controller.cont = Number((score * totalBoost).toFixed(4));

        controller.scoreHistory.push(score);
        controller.signalHistory.push(controller.signalSpeed);
        if (controller.scoreHistory.length > CONFIG.scoreHistory) controller.scoreHistory.shift();
        if (controller.signalHistory.length > CONFIG.signalHistory) controller.signalHistory.shift();
    });
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

    const totalEnd = performance.now();
    console.log(`Batch completed in ${((totalEnd - totalStart) / 1000).toFixed(3)} seconds`);

    for (const { controller, signal, duration } of results) {
        controller.lastSignal = signal;
        controller.signalSpeed = duration;
    }
};

const runWorker = async (controller) => {
    const params = getControllerParams(controller.group, controller.section, controller.layer);
    const { cacheSize, atrFactor, stopFactor, minPriceMovement, maxPriceMovement, inputMult, pop } = params;

    const recentCandles = cache.slice(-cacheSize);

    return new Promise((resolve, reject) => {
        const worker = new Worker(new URL('./worker.js', import.meta.url), {
            workerData: {
                id: `G${controller.group}S${controller.section}L${controller.layer}C${controller.id}`,
                directoryPath: controller.directoryPath,
                cacheSize,
                populationPerController: pop,
                cache: recentCandles,
                type: controller.type,
                priceObj: {
                    atrFactor,
                    stopFactor,
                    minPriceMovement,
                    maxPriceMovement,
                },
                inputMult,
                processCount: CONFIG.baseProcessCount,
                forceMin: CONFIG.forceMin
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
            adjustAllControllers();
            await saveLegionState();

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