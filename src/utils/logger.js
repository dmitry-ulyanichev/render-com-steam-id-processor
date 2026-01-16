// steam-id-processor/src/utils/logger.js
const fs = require('fs');
const path = require('path');
const CONFIG = require('../../config/config');

// Ensure logs directory exists
fs.mkdirSync(CONFIG.LOG_DIR, { recursive: true });

// Log file paths
const MAIN_LOG = path.resolve(CONFIG.LOG_DIR, 'steam_id_processor.log');
const ERROR_LOG = path.resolve(CONFIG.LOG_DIR, 'error.log');

// Log level hierarchy
const LOG_LEVELS = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3
};

// Simple, reliable logger using fs.appendFileSync (no Winston complexity)
class SimpleLogger {
  constructor() {
    // Get log level from config (which reads from STEAM_ID_PROCESSOR_LOG_LEVEL env var)
    const configLevel = CONFIG.LOG_LEVEL || 'info';
    this.level = configLevel.toLowerCase();
    console.log(`Log directory path: ${CONFIG.LOG_DIR}`);
    console.log(`Log level: ${this.level}`);
  }

  _shouldLog(level) {
    const currentLevel = LOG_LEVELS[this.level] !== undefined ? LOG_LEVELS[this.level] : LOG_LEVELS.info;
    const messageLevel = LOG_LEVELS[level] !== undefined ? LOG_LEVELS[level] : LOG_LEVELS.info;
    return messageLevel >= currentLevel;
  }

  _write(level, message, logFile = MAIN_LOG) {
    // Check if this message should be logged based on current level
    if (!this._shouldLog(level)) {
      return;
    }

    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] ${level.toUpperCase()}: ${message}\n`;

    // Always write to console
    console.log(logMessage.trim());

    // Write to file (synchronous to prevent race conditions)
    try {
      fs.appendFileSync(logFile, logMessage, 'utf8');
    } catch (error) {
      // If file write fails, at least log to console
      console.error(`Failed to write to log file: ${error.message}`);
    }
  }

  info(message) {
    this._write('info', message, MAIN_LOG);
  }

  error(message) {
    this._write('error', message, MAIN_LOG);
    // Also write errors to error.log
    if (this._shouldLog('error')) {
      try {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ERROR: ${message}\n`;
        fs.appendFileSync(ERROR_LOG, logMessage, 'utf8');
      } catch (e) {
        // Ignore if error log fails
      }
    }
  }

  warn(message) {
    this._write('warn', message, MAIN_LOG);
  }

  debug(message) {
    this._write('debug', message, MAIN_LOG);
  }

  // Alias for compatibility
  log(level, message) {
    if (this[level]) {
      this[level](message);
    } else {
      this.info(message);
    }
  }
}

// Export singleton instance
const logger = new SimpleLogger();
module.exports = logger;
