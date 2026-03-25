import { parentPort, workerData } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';

import HiveMindController from './hivemind/hiveMindController.js';

const { directoryPath, cacheSize, pop, cache, id, type, tier, priceObj, processCount, forceMin, bcR, injR, sharedMem, childMem } = workerData;

try {
    const controller = new HiveMindController(id, directoryPath, cacheSize, pop, type, tier, priceObj, forceMin);

    const start = performance.now();
    const signal = controller.getSignal(cache, processCount, bcR, injR, sharedMem, childMem);
    const end = performance.now();
    const duration = end - start;

    parentPort.postMessage({ signal, duration });
} catch (err) {
    parentPort.postMessage({ error: err.message || String(err) });
}