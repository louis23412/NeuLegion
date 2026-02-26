import { parentPort, workerData } from 'node:worker_threads';
import http from 'node:http';

const { ip, port } = workerData;

const currentLegionState = {
    status: 'initializing',
    candleCounter: 0,
    consensus : {},
    controllers: []
};

parentPort.on('message', (msg) => {
    if (msg.type === 'UPDATE_STATE') {
        currentLegionState.status = msg.status,
        currentLegionState.candleCounter = msg.candleCounter,
        currentLegionState.consensus = msg.consensus,
        currentLegionState.controllers = msg.controllers
    }
});

const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Content-Type', 'application/json');

    if (req.url === '/') {
        res.writeHead(200);
        res.end(JSON.stringify(currentLegionState, null, 2));
    } else {
        res.writeHead(404);
        res.end(JSON.stringify({ error: 'Not Found' }));
    }
});

server.listen(port, ip);
setInterval(() => {}, 30000);