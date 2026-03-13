import { parentPort, workerData } from 'node:worker_threads';
import http from 'node:http';
import os from 'os';

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

const getLocalIP = () => {
  const interfaces = os.networkInterfaces();
  let ipAddress;

  Object.keys(interfaces).forEach((ifaceName) => {
    interfaces[ifaceName].forEach((iface) => {
      if (iface.family === 'IPv4' && !iface.internal) {
        ipAddress = iface.address;
      }
    });
  });

  return ipAddress;
}

const server = http.createServer((req, res) => {
    const trustedIp = getLocalIP();
    const trustedPort = 3001;

    res.setHeader('Access-Control-Allow-Origin', `http://${trustedIp}:${trustedPort}`);
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