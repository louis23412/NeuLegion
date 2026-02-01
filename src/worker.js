import { parentPort, workerData } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

import HiveMindController from './hivemind/hiveMindController.js';

const { directoryPath, cacheSize, populationPerController, cache, id, type, priceObj, inputMult, processCount, forceMin, memShareFreq, sharedMem } = workerData;

try {
    const controller = new HiveMindController(id, directoryPath, cacheSize, populationPerController, type, priceObj, inputMult, forceMin, memShareFreq);

    const start = performance.now();
    const signal = controller.getSignal(cache, processCount, sharedMem);
    const end = performance.now();
    const duration = end - start;

    parentPort.postMessage({ signal, duration });
} catch (err) {
    parentPort.postMessage({ error: err.message || String(err) });
}