import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import Database from 'better-sqlite3';

import HiveMind from './hiveMind.js';
import IndicatorProcessor from './indicatorProcessor.js';

import { truncateToDecimals, isValidNumber } from './utils.js';

class HiveMindController {
    #hivemind;
    #indicators;
    #db;
    #config;

    #globalAccuracy = {
        trainingSteps : 0,
        skippedDuplicate : 0,
        prob : 0
    }

    #cacheSize;
    #inputSize;
    #trainingCandleSize;
    #trainingIndicators;

    #controllerID;
    #directoryPath;
    #ensembleSize;
    #type;

    constructor ( id, dp, cs, es, type, priceObj ) {
        this.#controllerID = id;

        try {
            this.#directoryPath = dp;
            fs.mkdirSync(this.#directoryPath, { recursive: true });
        } catch (err) {
            console.log(`Unable to create directory path "${this.#directoryPath}". ${err.message}`);
            process.exit(1);
        }

        this.#type = type;
        this.#cacheSize = cs;
        this.#ensembleSize = es;
        this.#chooseDimension(this.#ensembleSize);

        this.#config = priceObj;

        this.#indicators = new IndicatorProcessor();
        this.#db = new Database(path.join(this.#directoryPath, `${this.#type}-hivemind_controller_${this.#controllerID}.db`), { fileMustExist: false });

        this.#initDatabase();
        this.#loadGlobalAccuracy();
    }

    #initDatabase () {
        this.#db.exec(`
            CREATE TABLE IF NOT EXISTS open_trades (
                timestamp TEXT PRIMARY KEY,
                sellPrice REAL NOT NULL,
                stopLoss REAL NOT NULL,
                entryPrice REAL NOT NULL,
                features TEXT NOT NULL,
                confidence REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS closed_trades (
                timestamp TEXT PRIMARY KEY,
                entryPrice REAL NOT NULL,
                exitPrice REAL NOT NULL,
                outcome INTEGER NOT NULL,
                features TEXT NOT NULL,
                confidence REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS candles (
                timestamp TEXT PRIMARY KEY,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trained_features (
                encoding TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS global_stats (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_open_trades_sellPrice ON open_trades(sellPrice);
            CREATE INDEX IF NOT EXISTS idx_open_trades_stopLoss ON open_trades(stopLoss);
            CREATE INDEX IF NOT EXISTS idx_candles_timestamp ON candles(timestamp);
            CREATE INDEX IF NOT EXISTS idx_trained_features_encoding ON trained_features(encoding);
        `);
    }

    #loadGlobalAccuracy () {
        const selectStmt = this.#db.prepare(`
            SELECT value FROM global_stats WHERE key = ?
        `);

        const trainingStepsRaw = selectStmt.get('training_steps');
        const skippedRaw = selectStmt.get('skipped_duplicate');
        const probRaw = selectStmt.get('prob');

        if (trainingStepsRaw) {
            this.#globalAccuracy.trainingSteps = trainingStepsRaw.value;
        }
        if (skippedRaw) {
            this.#globalAccuracy.skippedDuplicate = skippedRaw.value;
        }
        if (probRaw) {
            this.#globalAccuracy.prob = probRaw.value;
        }
    }

    #saveGlobalAccuracy () {
        const upsertStmt = this.#db.prepare(`
            INSERT INTO global_stats (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        `);

        const transaction = this.#db.transaction(() => {
            upsertStmt.run('training_steps', this.#globalAccuracy.trainingSteps);
            upsertStmt.run('skipped_duplicate', this.#globalAccuracy.skippedDuplicate);
            upsertStmt.run('prob', this.#globalAccuracy.prob);
        });
        transaction();
    }

    #getRecentCandles (candles) {
        if (!Array.isArray(candles) || candles.length === 0) {
            return { error: 'Invalid candle array type or length', recentCandles: [], fullCandles: [] };
        }

        const newCandles = candles.filter(c =>
            isValidNumber(c.timestamp) &&
            isValidNumber(c.open) &&
            isValidNumber(c.high) &&
            isValidNumber(c.low) &&
            isValidNumber(c.close) &&
            isValidNumber(c.volume) &&
            c.volume >= 0
        );

        let recentCandles = [];
        let fullCandles = [];
        const transaction = this.#db.transaction(() => {
            if (newCandles.length > 0) {
                const insertCandleStmt = this.#db.prepare(`
                    INSERT OR IGNORE INTO candles (timestamp, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?)
                `);
                const insertedTimestamps = [];
                for (const candle of newCandles) {
                    const result = insertCandleStmt.run(
                        candle.timestamp,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume
                    );

                    if (result.changes > 0) {
                        insertedTimestamps.push(candle.timestamp);
                    }
                }

                if (insertedTimestamps.length > 0) {
                    const placeholders = insertedTimestamps.map(() => '?').join(',');
                    const fetchRecentStmt = this.#db.prepare(`
                        SELECT * FROM candles WHERE timestamp IN (${placeholders}) ORDER BY timestamp ASC
                    `);
                    recentCandles = fetchRecentStmt.all(...insertedTimestamps);
                }
            }

            const fetchCandlesStmt = this.#db.prepare(`
                SELECT * FROM candles ORDER BY timestamp ASC LIMIT ${this.#cacheSize}
            `);
            fullCandles = fetchCandlesStmt.all();

            const cleanupStmt = this.#db.prepare(`
                DELETE FROM candles WHERE timestamp NOT IN (
                    SELECT timestamp FROM candles ORDER BY timestamp DESC LIMIT ${this.#cacheSize}
                )
            `);
            cleanupStmt.run();
        });
        transaction();

        if (fullCandles.length === 0) {
            return { error: 'No valid candles available', recentCandles: [], fullCandles: [] };
        }

        return { error: null, recentCandles, fullCandles };
    }

    #robustNormalize (data, count = 1, lowerPercentile = 0.05, upperPercentile = 0.95) {
        if (!Array.isArray(data) || data.length < 2) return Array(count).fill(0);
        
        const actualCount = Math.min(count, data.length);
        const valuesToNormalize = data.slice(-actualCount);
        
        if (!valuesToNormalize.every(isValidNumber)) return Array(actualCount).fill(0);
        
        const sortedData = [...data].sort((a, b) => a - b);
        const lowerIdx = Math.floor(lowerPercentile * sortedData.length);
        const upperIdx = Math.ceil(upperPercentile * sortedData.length) - 1;
        let min_h = sortedData[lowerIdx];
        let max_h = sortedData[upperIdx];
        
        const recentMin = Math.min(...valuesToNormalize);
        const recentMax = Math.max(...valuesToNormalize);
        let min = Math.min(min_h, recentMin);
        let max = Math.max(max_h, recentMax);
        
        if (max === min) {
            const median = sortedData[Math.floor(sortedData.length / 2)];
            const mad = sortedData.reduce((sum, val) => {
                return sum + Math.abs(val - median);
            }, 0) / sortedData.length;
            
            const scale = mad > 0 ? mad : Number.EPSILON * 1e6;
            min = median - scale;
            max = median + scale;
        }
        
        const range = max - min;
        const epsilon = Number.EPSILON * Math.max(Math.abs(min), Math.abs(max));
        if (range < epsilon) {
            min -= epsilon;
            max += epsilon;
        }
        
        return valuesToNormalize.map(value => {
            const normalized = (value - min) / (max - min);
            return truncateToDecimals(Math.min(1, Math.max(0, normalized)), 4);
        });
    }

    #extractFeatures (data, candleCount, indicatorCount) {
        const indicators = [
            'rsi',
            'macdDiff',
            'atr',
            'ema100',
            'stochasticDiff',
            'bollingerPercentB',
            'obv',
            'adx',
            'cci',
            'williamsR'
        ];

        const count = Math.max(0, Math.min(indicatorCount, indicators.length));

        const normalized = [];
        for (let i = 0; i < count; i++) {
            const key = indicators[i];
            normalized.push(this.#robustNormalize(data[key], candleCount));
        }

        const result = Array.from({ length: candleCount }, (_, i) => {
            const row = [];
            for (let j = 0; j < count; j++) {
                row.push(normalized[j][i]);
            }
            return row;
        });

        return result;
    }

    #chooseDimension (es) {
        const MIN_SIZE = 10;
        const MAX_SIZE = 100;
        let desiredSize = Math.max(MIN_SIZE, MAX_SIZE - Math.floor((es - 1) / 10));

        const searchRange = 1;

        const getBestFactoring = (size) => {
            let bestMinv = 1;
            let bestInd = 1;
            let bestCand = size;
            for (let i = 2; i <= 10; i++) {
                if (size % i === 0) {
                    const cand = size / i;
                    const minv = Math.min(i, cand);
                    if (minv > bestMinv || (minv === bestMinv && i > bestInd)) {
                        bestMinv = minv;
                        bestInd = i;
                        bestCand = cand;
                    }
                }
            }
            return { minv: bestMinv, ind: bestInd, cand: bestCand };
        };

        let bestMinv = -1;
        let bestDiff = Infinity;
        let bestTarget = desiredSize;
        let bestInd = 1;
        let bestCand = desiredSize;

        for (let offset = -searchRange; offset <= searchRange; offset++) {
            const candidate = desiredSize + offset;
            if (candidate < MIN_SIZE || candidate > MAX_SIZE) continue;

            const current = getBestFactoring(candidate);
            const currentDiff = Math.abs(offset);

            let update = false;
            if (current.minv > bestMinv) {
                update = true;
            } else if (current.minv === bestMinv) {
                if (currentDiff < bestDiff) {
                    update = true;
                } else if (currentDiff === bestDiff) {
                    if (current.ind > bestInd) {
                        update = true;
                    } else if (current.ind === bestInd) {
                        if (current.cand > bestCand) {
                            update = true;
                        }
                    }
                }
            }

            if (update) {
                bestMinv = current.minv;
                bestDiff = currentDiff;
                bestTarget = candidate;
                bestInd = current.ind;
                bestCand = current.cand;
            }
        }

        this.#inputSize = bestTarget;
        this.#trainingCandleSize = bestCand;
        this.#trainingIndicators = bestInd;
    }

    #updateOpenTrades (candles) {
        if (!Array.isArray(candles) || candles.length === 0) return;

        const tradesStmt = this.#db.prepare(`
            SELECT timestamp, sellPrice, stopLoss, entryPrice, features, confidence
            FROM open_trades
        `);
        const trades = tradesStmt.all();

        const closedTrades = [];

        for (const trade of trades) {
            const features = JSON.parse(trade.features);
            for (const candle of candles) {
                if (!candle || !isValidNumber(candle.high) || !isValidNumber(candle.low)) continue;

                const isLong = trade.sellPrice > trade.entryPrice;

                const hitTakeProfit = isLong
                    ? candle.high >= trade.sellPrice
                    : candle.low <= trade.sellPrice;

                const hitStopLoss = isLong
                    ? candle.low <= trade.stopLoss
                    : candle.high >= trade.stopLoss;

                if (hitTakeProfit || hitStopLoss) {
                    const exitPrice = hitTakeProfit ? trade.sellPrice : trade.stopLoss;
                    const outcome = hitTakeProfit ? 1 : 0;

                    closedTrades.push({
                        timestamp: trade.timestamp,
                        entryPrice: trade.entryPrice,
                        exitPrice,
                        outcome,
                        features,
                        confidence: trade.confidence
                    });
                    break;
                }
            }
        }

        if (closedTrades.length > 0) {
            const transaction = this.#db.transaction(() => {
                const insertClosedStmt = this.#db.prepare(`
                    INSERT INTO closed_trades 
                    (timestamp, entryPrice, exitPrice, outcome, features, confidence) 
                    VALUES (?, ?, ?, ?, ?, ?)
                `);
                const deleteOpenStmt = this.#db.prepare(`DELETE FROM open_trades WHERE timestamp = ?`);

                for (const trade of closedTrades) {
                    insertClosedStmt.run(
                        trade.timestamp,
                        trade.entryPrice,
                        trade.exitPrice,
                        trade.outcome,
                        JSON.stringify(trade.features),
                        trade.confidence
                    );
                    deleteOpenStmt.run(trade.timestamp);
                }
            });

            transaction();
        }
    }

    #processClosedTrades (processCount) {
        const tradesStmt = this.#db.prepare(`
            SELECT timestamp, entryPrice, exitPrice, outcome, features, confidence
            FROM closed_trades
            ORDER BY timestamp ASC
            LIMIT ?
        `);

        const trades = tradesStmt.all(processCount);

        if (trades.length === 0) {
            return;
        }

        const checkEncodingStmt = this.#db.prepare(`SELECT encoding FROM trained_features WHERE encoding = ?`);
        const insertEncodingStmt = this.#db.prepare(`INSERT INTO trained_features (encoding) VALUES (?)`);
        const deleteTradeStmt = this.#db.prepare(`DELETE FROM closed_trades WHERE timestamp = ?`);

        for (const row of trades) {
            const trade = {
                timestamp: row.timestamp,
                entryPrice: row.entryPrice,
                exitPrice: row.exitPrice,
                outcome: row.outcome,
                features: JSON.parse(row.features),
                confidence: row.confidence
            };

            const flatFeatures = trade.features.flat();
            const encodingString = `${flatFeatures.join(',')}|${trade.outcome}`;
            const encodingHash = crypto.createHash('sha256').update(encodingString).digest('hex');

            this.#db.transaction(() => {
                const existingEncoding = checkEncodingStmt.get(encodingHash);
                if (existingEncoding) {
                    this.#globalAccuracy.skippedDuplicate++;
                    deleteTradeStmt.run(trade.timestamp);
                    return;
                }

                if (!this.#hivemind) {
                    this.#hivemind = new HiveMind(this.#directoryPath, this.#ensembleSize, this.#inputSize, this.#controllerID);
                }

                const result = this.#hivemind.train(flatFeatures, trade.outcome);
                this.#globalAccuracy.trainingSteps = result.step;
                this.#globalAccuracy.prob = result.prob;

                this.#hivemind.dumpState();

                insertEncodingStmt.run(encodingHash);
                deleteTradeStmt.run(trade.timestamp);
            })();
        }

        this.#saveGlobalAccuracy();
    }

    getSignal (candles) {
        const { error, recentCandles, fullCandles } = this.#getRecentCandles(candles);

        if (error) return { error };

        this.#updateOpenTrades(recentCandles);

        const indicators = this.#indicators.compute(fullCandles);

        if (indicators.error) return { error: 'Indicators error' };

        const features = this.#extractFeatures(indicators, this.#trainingCandleSize, this.#trainingIndicators);
        
        const entryPrice = indicators.lastClose;

        const direction = this.#type === 'positive' ? 1 : (this.#type === 'negative' ? -1 : 0);

        const atr = indicators.lastAtr;

        const tpRawDistance = this.#config.atrFactor * atr;
        const slRawDistance = this.#config.stopFactor * atr;

        const minDelta = entryPrice * this.#config.minPriceMovement;
        const maxDelta = entryPrice * this.#config.maxPriceMovement;

        const tpDistance = Math.min(Math.max(tpRawDistance, minDelta), maxDelta);
        const slDistance = Math.min(Math.max(slRawDistance, minDelta), maxDelta);

        const sellPrice = truncateToDecimals(entryPrice + direction * tpDistance, 2);
        const stopLoss = truncateToDecimals(entryPrice - direction * slDistance, 2);

        const insertTradeStmt = this.#db.prepare(`
            INSERT INTO open_trades (timestamp, sellPrice, stopLoss, entryPrice, features, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
        `);
        
        insertTradeStmt.run(
            recentCandles.at(-1).timestamp,
            sellPrice,
            stopLoss,
            entryPrice,
            JSON.stringify(features),
            -1
        );

        const tradesStmt = this.#db.prepare(`SELECT timestamp FROM open_trades`);
        const openSimulations = tradesStmt.all().length;

        const closedTradesStmt = this.#db.prepare(`SELECT timestamp FROM closed_trades`);
        const pendingClosedTrades = closedTradesStmt.all().length;

        this.#processClosedTrades(1);

        return {
            prob : truncateToDecimals(this.#globalAccuracy.prob * 100, 3),
            lastTrainingStep : this.#globalAccuracy.trainingSteps,
            skippedTraining : this.#globalAccuracy.skippedDuplicate,
            openSimulations,
            pendingClosedTrades
        };
    }
}

export default HiveMindController;