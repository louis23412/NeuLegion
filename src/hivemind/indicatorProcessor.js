import { isValidNumber } from "./utils.js";

class IndicatorProcessor {
  compute(candles) {
    if (!Array.isArray(candles) || candles.length < 11) return { error: true };

    const validCandles = candles.filter(c =>
      isValidNumber(c.close) && isValidNumber(c.high) && isValidNumber(c.low) &&
      isValidNumber(c.volume) && c.volume >= 0 && isValidNumber(c.timestamp)
    );

    if (validCandles.length < 11) {
      return { error: true };
    }
    
    const close = validCandles.map(c => Number(c.close));
    const high = validCandles.map(c => Number(c.high));
    const low = validCandles.map(c => Number(c.low));
    const volume = validCandles.map(c => Number(c.volume));
    const lastClose = close[close.length - 1];

    const rsi = this.#computeRSI(close, 14);
    const macd = this.#computeMACD(close, 8, 21, 5);
    const atr = this.#computeATR(high, low, close, 14);
    const ema100 = this.#computeEMA(close, 100);
    const stochastic = this.#computeStochastic(high, low, close, 14, 3, 3);
    const bollinger = this.#computeBollingerBands(close, 20, 2);
    const obv = this.#computeOBV(close, volume);
    const adx = this.#computeADX(high, low, close, 14);
    const cci = this.#computeCCI(high, low, close, 20);
    const williamsR = this.#computeWilliamsR(high, low, close, 14);

    const macdDiff = macd.map(m => m.MACD - m.signal);
    const stochasticDiff = stochastic.map(s => s.K - s.D);
    const bollingerPercentB = bollinger.map((b, i) => {
      const closePrice = close[i + 19];
      const denominator = b.upper - b.lower;
      return denominator > 0 && isValidNumber(closePrice) 
        ? (closePrice - b.lower) / denominator 
        : 0.5;
    });

    return {
      lastClose,
      lastAtr: atr.at(-1),
      rsi,
      macdDiff,
      atr,
      ema100,
      stochasticDiff,
      bollingerPercentB,
      obv,
      adx,
      cci,
      williamsR
    };
  }

  #computeRSI(values, period) {
    if (!Array.isArray(values) || values.length < period + 1 || !values.every(isValidNumber)) {
      return new Array(Math.max(0, values.length - period)).fill(50);
    }
    let gains = 0, losses = 0;
    const rsi = new Array(values.length - period);
    for (let i = 1; i <= period; i++) {
      const delta = values[i] - values[i - 1];
      if (!isValidNumber(delta)) return new Array(values.length - period).fill(50);
      gains += delta > 0 ? delta : 0;
      losses += delta < 0 ? -delta : 0;
    }
    let avgGain = gains / period;
    let avgLoss = losses / period;
    if (!isValidNumber(avgGain) || !isValidNumber(avgLoss)) return new Array(values.length - period).fill(50);
    for (let i = period, j = 0; j < rsi.length; i++, j++) {
      const delta = values[i] - values[i - 1];
      const gain = delta > 0 ? delta : 0;
      const loss = delta < 0 ? -delta : 0;
      avgGain = (avgGain * (period - 1) + gain) / period;
      avgLoss = (avgLoss * (period - 1) + loss) / period;
      if (!isValidNumber(avgGain) || !isValidNumber(avgLoss)) {
        rsi[j] = 50;
        continue;
      }
      const rs = avgLoss <= 0 ? 100 : avgGain / avgLoss;
      rsi[j] = isValidNumber(rs) ? 100 - (100 / (1 + rs)) : 50;
    }
    return rsi;
  }

  #computeMACD(values, fastPeriod, slowPeriod, signalPeriod) {
    if (!Array.isArray(values) || values.length < slowPeriod + signalPeriod || !values.every(isValidNumber)) {
      return new Array(Math.max(0, values.length - slowPeriod - signalPeriod + 1)).fill({ MACD: 0, signal: 0, histogram: 0 });
    }
    const median = values.slice().sort((a, b) => a - b)[Math.floor(values.length / 2)] || 1;
    const normalizedValues = values.map(v => v / median);
    const fastEMA = this.#computeEMA(normalizedValues, fastPeriod);
    const slowEMA = this.#computeEMA(normalizedValues, slowPeriod);
    const macdLine = new Array(normalizedValues.length - (slowPeriod - fastPeriod)).fill(0);
    for (let i = slowPeriod - fastPeriod, j = 0; i < normalizedValues.length; i++, j++) {
      const diff = fastEMA[i] - slowEMA[i - (slowPeriod - fastPeriod)];
      macdLine[j] = isValidNumber(diff) ? Math.min(Math.max(diff, -1000), 1000) : 0;
    }
    const signalLine = this.#computeEMA(macdLine.slice(0, macdLine.length - signalPeriod + 1), signalPeriod);
    const macd = new Array(macdLine.length - signalPeriod + 1).fill({ MACD: 0, signal: 0, histogram: 0 });
    for (let i = signalPeriod - 1, j = 0; i < macdLine.length; i++, j++) {
      const signal = signalLine[j] || 0;
      const macdVal = macdLine[i];
      const hist = macdVal - signal;
      macd[j] = {
        MACD: isValidNumber(macdVal) ? macdVal : 0,
        signal: isValidNumber(signal) ? signal : 0,
        histogram: isValidNumber(hist) ? hist : 0
      };
    }
    return macd;
  }

  #computeEMA(values, period) {
    if (!Array.isArray(values) || values.length < period || !values.every(isValidNumber)) return new Array(values.length).fill(0);
    const alpha = 2 / (period + 1);
    const ema = new Array(values.length);
    const initialValues = values.slice(0, period);
    const initialAvg = initialValues.reduce((sum, v) => sum + v, 0) / period;
    if (!isValidNumber(initialAvg)) return new Array(values.length).fill(0);
    ema[period - 1] = Math.min(Math.max(initialAvg, -5000), 5000);
    for (let i = period; i < values.length; i++) {
      const next = alpha * values[i] + (1 - alpha) * ema[i - 1];
      ema[i] = isValidNumber(next) ? Math.min(Math.max(next, -5000), 5000) : 0;
    }
    for (let i = 0; i < period - 1; i++) {
      ema[i] = 0;
    }
    return ema;
  }

  #computeATR(high, low, close, period) {
    if (
      !Array.isArray(high) || high.length < period + 1 ||
      high.length !== low.length || low.length !== close.length ||
      !high.every(isValidNumber) || !low.every(isValidNumber) || !close.every(isValidNumber)
    ) return new Array(Math.max(0, high.length - period)).fill(0);
    const tr = new Array(close.length - 1);
    for (let i = 1; i < close.length; i++) {
      const highLow = high[i] - low[i];
      const highClose = Math.abs(high[i] - close[i - 1]);
      const lowClose = Math.abs(low[i] - close[i - 1]);
      tr[i - 1] = isValidNumber(highLow) && isValidNumber(highClose) && isValidNumber(lowClose)
        ? Math.max(highLow, highClose, lowClose)
        : 0;
    }
    const atr = new Array(tr.length - period + 1);
    let sum = tr.slice(0, period).reduce((s, v) => s + (isValidNumber(v) ? v : 0), 0);
    atr[0] = isValidNumber(sum) ? Math.min(Math.max(sum / period, 0), 1000) : 0;
    for (let i = period, j = 1; i < tr.length; i++, j++) {
      const next = (atr[j - 1] * (period - 1) + (isValidNumber(tr[i]) ? tr[i] : 0)) / period;
      atr[j] = isValidNumber(next) ? Math.min(Math.max(next, 0), 1000) : 0;
    }
    return atr;
  }

  #computeStochastic(high, low, close, period, kSmoothing, dSmoothing) {
    if (
      !Array.isArray(high) || high.length < period + kSmoothing + dSmoothing - 1 ||
      high.length !== low.length || low.length !== close.length ||
      !high.every(isValidNumber) || !low.every(isValidNumber) || !close.every(isValidNumber)
    ) {
      return new Array(Math.max(0, high.length - period - kSmoothing - dSmoothing + 2)).fill({ K: 50, D: 50 });
    }
    const kValues = new Array(high.length - period + 1).fill(50);
    for (let i = period - 1; i < high.length; i++) {
      const periodHigh = Math.max(...high.slice(i - period + 1, i + 1));
      const periodLow = Math.min(...low.slice(i - period + 1, i + 1));
      const denominator = periodHigh - periodLow;
      const k = denominator > 0 ? ((close[i] - periodLow) / denominator) * 100 : 50;
      kValues[i - period + 1] = isValidNumber(k) ? Math.min(Math.max(k, 0), 100) : 50;
    }
    const kSmoothed = this.#computeEMA(kValues, kSmoothing);
    const dValues = this.#computeEMA(kSmoothed.slice(0, kSmoothed.length - dSmoothing + 1), dSmoothing);
    const stochastic = new Array(kSmoothed.length - dSmoothing + 1).fill({ K: 50, D: 50 });
    for (let i = dSmoothing - 1, j = 0; i < kSmoothed.length; i++, j++) {
      stochastic[j] = {
        K: isValidNumber(kSmoothed[i]) ? kSmoothed[i] : 50,
        D: isValidNumber(dValues[j]) ? dValues[j] : 50
      };
    }
    return stochastic;
  }

  #computeBollingerBands(values, period, stdDevMultiplier) {
    if (!Array.isArray(values) || values.length < period || !values.every(isValidNumber)) {
      return new Array(values.length - period + 1).fill({ middle: 0, upper: 0, lower: 0 });
    }
    const bands = new Array(values.length - period + 1);
    for (let i = period - 1, j = 0; i < values.length; i++, j++) {
      const periodValues = values.slice(i - period + 1, i + 1);
      const sma = periodValues.reduce((sum, v) => sum + v, 0) / period;
      const variance = periodValues.reduce((sum, v) => sum + Math.pow(v - sma, 2), 0) / period;
      const stdDev = Math.sqrt(variance);
      const upper = sma + stdDevMultiplier * stdDev;
      const lower = sma - stdDevMultiplier * stdDev;
      bands[j] = {
        middle: isValidNumber(sma) ? sma : 0,
        upper: isValidNumber(upper) ? upper : 0,
        lower: isValidNumber(lower) ? lower : 0
      };
    }
    return bands;
  }

  #computeOBV(close, volume) {
    if (
      !Array.isArray(close) || close.length < 2 ||
      close.length !== volume.length ||
      !close.every(isValidNumber) || !volume.every(isValidNumber)
    ) {
      return new Array(close.length).fill(0);
    }
    const obv = new Array(close.length).fill(0);
    obv[0] = 0; // Initial OBV is 0
    for (let i = 1; i < close.length; i++) {
      const prevClose = close[i - 1];
      const currClose = close[i];
      const currVolume = volume[i];
      if (!isValidNumber(prevClose) || !isValidNumber(currClose) || !isValidNumber(currVolume)) {
        obv[i] = obv[i - 1];
        continue;
      }
      obv[i] = obv[i - 1] + (currClose > prevClose ? currVolume : currClose < prevClose ? -currVolume : 0);
      obv[i] = isValidNumber(obv[i]) ? Math.min(Math.max(obv[i], -1e9), 1e9) : obv[i - 1];
    }
    return obv;
  }

  #computeADX(high, low, close, period) {
    if (
      !Array.isArray(high) || high.length < period + 1 ||
      high.length !== low.length || low.length !== close.length ||
      !high.every(isValidNumber) || !low.every(isValidNumber) || !close.every(isValidNumber)
    ) {
      return new Array(Math.max(0, high.length - period)).fill(50);
    }
    const dmPlus = new Array(high.length - 1).fill(0);
    const dmMinus = new Array(high.length - 1).fill(0);
    const tr = new Array(high.length - 1).fill(0);
    for (let i = 1; i < high.length; i++) {
      const highDiff = high[i] - high[i - 1];
      const lowDiff = low[i - 1] - low[i];
      dmPlus[i - 1] = highDiff > lowDiff && highDiff > 0 ? highDiff : 0;
      dmMinus[i - 1] = lowDiff > highDiff && lowDiff > 0 ? lowDiff : 0;
      const highLow = high[i] - low[i];
      const highClose = Math.abs(high[i] - close[i - 1]);
      const lowClose = Math.abs(low[i] - close[i - 1]);
      tr[i - 1] = Math.max(highLow, highClose, lowClose);
    }
    const atr = this.#computeEMA(tr, period);
    const smoothedDmPlus = this.#computeEMA(dmPlus, period);
    const smoothedDmMinus = this.#computeEMA(dmMinus, period);
    const diPlus = new Array(smoothedDmPlus.length).fill(0);
    const diMinus = new Array(smoothedDmMinus.length).fill(0);
    for (let i = 0; i < diPlus.length; i++) {
      diPlus[i] = atr[i] > 0 ? (smoothedDmPlus[i] / atr[i]) * 100 : 0;
      diMinus[i] = atr[i] > 0 ? (smoothedDmMinus[i] / atr[i]) * 100 : 0;
    }
    const adx = new Array(diPlus.length).fill(50);
    for (let i = 0; i < diPlus.length; i++) {
      const diDiff = Math.abs(diPlus[i] - diMinus[i]);
      const diSum = diPlus[i] + diMinus[i];
      const dx = diSum > 0 ? (diDiff / diSum) * 100 : 50;
      adx[i] = isValidNumber(dx) ? dx : 50;
    }
    const smoothedAdx = this.#computeEMA(adx, period);
    return smoothedAdx.slice(period - 1).map(v => isValidNumber(v) ? Math.min(Math.max(v, 0), 100) : 50);
  }

  #computeCCI(high, low, close, period) {
    if (
      !Array.isArray(high) || high.length < period ||
      high.length !== low.length || low.length !== close.length ||
      !high.every(isValidNumber) || !low.every(isValidNumber) || !close.every(isValidNumber)
    ) {
      return new Array(Math.max(0, high.length - period + 1)).fill(0);
    }
    const cci = new Array(high.length - period + 1);
    for (let i = period - 1, j = 0; i < high.length; i++, j++) {
      const typicalPrices = high.slice(i - period + 1, i + 1).map((h, k) => 
        (h + low[i - period + 1 + k] + close[i - period + 1 + k]) / 3
      );
      const sma = typicalPrices.reduce((sum, v) => sum + v, 0) / period;
      const meanDeviation = typicalPrices.reduce((sum, v) => sum + Math.abs(v - sma), 0) / period;
      const currentTypicalPrice = (high[i] + low[i] + close[i]) / 3;
      cci[j] = meanDeviation > 0 
        ? (currentTypicalPrice - sma) / (0.015 * meanDeviation)
        : 0;
      cci[j] = isValidNumber(cci[j]) ? Math.min(Math.max(cci[j], -1000), 1000) : 0;
    }
    return cci;
  }

  #computeWilliamsR(high, low, close, period) {
    if (
      !Array.isArray(high) || high.length < period ||
      high.length !== low.length || low.length !== close.length ||
      !high.every(isValidNumber) || !low.every(isValidNumber) || !close.every(isValidNumber)
    ) {
      return new Array(Math.max(0, high.length - period + 1)).fill(-50);
    }
    const williamsR = new Array(high.length - period + 1);
    for (let i = period - 1, j = 0; i < high.length; i++, j++) {
      const periodHigh = Math.max(...high.slice(i - period + 1, i + 1));
      const periodLow = Math.min(...low.slice(i - period + 1, i + 1));
      const denominator = periodHigh - periodLow;
      const r = denominator > 0 
        ? ((periodHigh - close[i]) / denominator) * -100 
        : -50;
      williamsR[j] = isValidNumber(r) ? Math.min(Math.max(r, -100), 0) : -50;
    }
    return williamsR;
  }
}

export default IndicatorProcessor;