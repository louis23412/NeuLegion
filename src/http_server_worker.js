import http from 'node:http';
import { parentPort, workerData } from 'node:worker_threads';

const { ip, port } = workerData;

const currentLegionState = {
    overview : {
        status : 'initializing',
        runtimeSeconds : 0
    },
    memoryVaultStats : {},
    consensus : {},
    lastCandles : [],
    controllers: []
};

parentPort.on('message', (msg) => {
    if (msg.type === 'UPDATE_FULL_STATE') {
        currentLegionState.overview = msg.overview
        currentLegionState.memoryVaultStats = msg.memoryVaultStats
        currentLegionState.consensus = msg.consensus
        currentLegionState.lastCandles = msg.lastCandles
        currentLegionState.controllers = msg.controllers
    }

    else if (msg.type === 'UPDATE_STATUS') {
        currentLegionState.overview.status = msg.status,
        currentLegionState.overview.runtimeSeconds = msg.runtimeSeconds
    }
});

const server = http.createServer((req, res) => {
    const trustedAddress = `http://${ip}:3001`

    res.setHeader('Access-Control-Allow-Origin', trustedAddress);
    res.setHeader('Access-Control-Allow-Methods', 'GET');
    res.setHeader('Content-Type', 'application/json');

    const origin = req.headers.origin;

    if (origin !== trustedAddress) {
        res.writeHead(403);
        res.end(JSON.stringify({ error : 'access denied' }));
        return;
    }

    if (req.url === '/') {
        res.writeHead(200);
        res.end(JSON.stringify(currentLegionState, null, 2));
        return;
    }
    
    else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Not Found' }));
    }
});

server.listen(port, ip);
setInterval(() => {}, 30000);