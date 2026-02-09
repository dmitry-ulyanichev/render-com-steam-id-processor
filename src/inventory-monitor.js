// steam-id-processor/src/inventory-monitor.js
const fs = require('fs-extra');
const path = require('path');
const logger = require('./utils/logger');

const PERSIST_PATH = path.join(__dirname, '../inventory_requests.json');
const MS_24H = 24 * 60 * 60 * 1000;
const MS_48H = 48 * 60 * 60 * 1000;

class InventoryMonitor {
  constructor() {
    this.timestamps = [];
    this._persistTimer = null;
    this._load();
  }

  _load() {
    try {
      if (fs.existsSync(PERSIST_PATH)) {
        const data = fs.readJsonSync(PERSIST_PATH);
        if (Array.isArray(data)) {
          this.timestamps = data;
          this._prune();
          logger.info(`InventoryMonitor: loaded ${this.timestamps.length} entries from disk`);
        }
      }
    } catch (err) {
      logger.warn(`InventoryMonitor: failed to load persisted data: ${err.message}`);
    }
  }

  _prune() {
    const cutoff = Date.now() - MS_48H;
    this.timestamps = this.timestamps.filter(e => e.ts > cutoff);
  }

  _schedulePersist() {
    if (this._persistTimer) return;
    this._persistTimer = setTimeout(() => {
      this._persistTimer = null;
      this._persist();
    }, 2000);
  }

  _persist() {
    this._prune();
    try {
      fs.writeJsonSync(PERSIST_PATH, this.timestamps);
    } catch (err) {
      logger.warn(`InventoryMonitor: failed to persist data: ${err.message}`);
    }
  }

  recordRequest(statusCode) {
    this.timestamps.push({ ts: Date.now(), status: statusCode });
    this._schedulePersist();
  }

  getRolling24hCount() {
    const cutoff = Date.now() - MS_24H;
    return this.timestamps.filter(e => e.ts > cutoff).length;
  }

  getCountsByDate() {
    const utc = {};
    const pacific = {};

    for (const entry of this.timestamps) {
      const d = new Date(entry.ts);

      // UTC date key
      const utcKey = d.toISOString().slice(0, 10);
      utc[utcKey] = (utc[utcKey] || 0) + 1;

      // Pacific time (UTC-8 standard, UTC-7 DST)
      const pacificDate = new Date(d.toLocaleString('en-US', { timeZone: 'America/Los_Angeles' }));
      const pYear = pacificDate.getFullYear();
      const pMonth = String(pacificDate.getMonth() + 1).padStart(2, '0');
      const pDay = String(pacificDate.getDate()).padStart(2, '0');
      const pacificKey = `${pYear}-${pMonth}-${pDay}`;
      pacific[pacificKey] = (pacific[pacificKey] || 0) + 1;
    }

    return { utc, pacific };
  }

  getStats() {
    const cutoff24h = Date.now() - MS_24H;
    const recent = this.timestamps.filter(e => e.ts > cutoff24h);

    return {
      rolling24h: recent.length,
      countsByDate: this.getCountsByDate(),
      rateLimited24h: recent.filter(e => e.status === 429).length,
      lastRequestTs: this.timestamps.length > 0
        ? this.timestamps[this.timestamps.length - 1].ts
        : null,
      totalTracked: this.timestamps.length
    };
  }

  getRecentTimestamps(n = 20) {
    return this.timestamps.slice(-n);
  }
}

module.exports = InventoryMonitor;
