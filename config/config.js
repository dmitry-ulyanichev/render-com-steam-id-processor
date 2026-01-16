// steam-id-processor/config/config.js
const path = require('path');
const fs = require('fs-extra');

// Load environment variables
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });

console.log('🔍 [DEBUG] Environment variables loaded');
console.log('🔍 [DEBUG] BACKOFF_SEQUENCE_MINUTES env var:', process.env.BACKOFF_SEQUENCE_MINUTES);

// Determine the parent directory (project root)
const parentDir = path.resolve(__dirname, '../..');

// Helper function to parse duration with fallback
function parseDuration(envVar, defaultValue) {
  const value = parseInt(process.env[envVar]);
  return isNaN(value) ? defaultValue : value;
}

// Helper function to parse backoff sequence from environment
function parseBackoffSequence(envVar, defaultSequence) {
  const envValue = process.env[envVar];
  if (!envValue) return defaultSequence;
  
  try {
    const parsed = envValue.split(',').map(val => parseInt(val.trim()));
    // Validate that all values are positive numbers
    if (parsed.every(val => !isNaN(val) && val > 0)) {
      return parsed;
    } else {
      console.warn(`Invalid BACKOFF_SEQUENCE_MINUTES format, using default: ${envValue}`);
      return defaultSequence;
    }
  } catch (error) {
    console.warn(`Error parsing BACKOFF_SEQUENCE_MINUTES, using default: ${error.message}`);
    return defaultSequence;
  }
}

// Define configuration
const CONFIG = {
  // File paths - updated for new structure
  QUEUE_PATH: path.join(__dirname, '../profiles_queue.json'), // Queue file inside steam-id-processor
  LOG_DIR: path.join(parentDir, 'logs'),
  LOG_LEVEL: process.env.STEAM_ID_PROCESSOR_LOG_LEVEL || 'info', // debug, info, warn, error

  // Detect cloud environment (Render, Heroku, etc.) - skip file logging on ephemeral filesystems
  ENABLE_FILE_LOGGING: !process.env.RENDER && !process.env.DYNO,

  // API settings
  API_ENDPOINT: 'https://kuchababok.online/en/links/api/add-link/',
  
  // NEW: API Server settings (PORT is set by Render, STEAM_PROCESSOR_API_PORT for local dev)
  API_PORT: parseInt(process.env.PORT) || parseInt(process.env.STEAM_PROCESSOR_API_PORT) || 3002,
  API_HOST: process.env.STEAM_PROCESSOR_API_HOST || '0.0.0.0',
  
  // Processing settings
  BATCH_SIZE: 20,
  CHECK_INTERVAL: 15000, // Check for new queue items every 15 seconds
  PROCESSING_DELAY: 350, // Delay between processing items
  EMPTY_QUEUE_DELAY: 5000, // Delay when queue is empty
  ERROR_DELAY: 30000, // Delay after errors
  MAX_RETRIES: 3, // Max retries for a single API call
  REQUEST_DELAY: 2000, // Delay between Steam API requests
  
  // NEW: Exponential backoff sequence for 429 errors (in minutes)
  // Can be overridden via BACKOFF_SEQUENCE_MINUTES environment variable
  BACKOFF_SEQUENCE_MINUTES: parseBackoffSequence('BACKOFF_SEQUENCE_MINUTES', [1, 2, 4, 8, 16, 32, 60, 120, 240, 480]),
  
  // Cooldown durations in milliseconds (from environment variables)
  // NOTE: inventory cooldown is now managed by exponential backoff for 429 errors
  COOLDOWN_DURATIONS: {
    // Connection error cooldowns (non-429 errors)
    'connection_reset': parseDuration('COOLDOWN_CONNECTION_RESET_MS', 10 * 60 * 1000), // Default: 10 minutes
    'timeout': parseDuration('COOLDOWN_TIMEOUT_MS', 5 * 60 * 1000), // Default: 5 minutes
    'dns_failure': parseDuration('COOLDOWN_DNS_FAILURE_MS', 15 * 60 * 1000), // Default: 15 minutes
    'socks_error': parseDuration('COOLDOWN_SOCKS_ERROR_MS', 15 * 60 * 1000), // Default: 15 minutes
    'permanent': parseDuration('COOLDOWN_PERMANENT_MS', 24 * 60 * 60 * 1000) // Default: 24 hours
  },
  
  // Environment variables validation
  STEAM_API_KEY: process.env.STEAM_API_KEY,
  LINK_HARVESTER_API_KEY: process.env.LINK_HARVESTER_API_KEY,
};

// Validate that required environment variables are present
const requiredEnvVars = {
  STEAM_API_KEY: CONFIG.STEAM_API_KEY,
  LINK_HARVESTER_API_KEY: CONFIG.LINK_HARVESTER_API_KEY
};

for (const [varName, value] of Object.entries(requiredEnvVars)) {
  if (!value) {
    console.error(`❌ Missing required environment variable: ${varName}`);
    console.error(`Please add ${varName}=your_value to your .env file`);
  } else {
    console.log(`✅ Environment variable ${varName} loaded`);
  }
}

// Log configuration
console.log('📝 Logging configuration:');
console.log(`   Log directory: ${CONFIG.LOG_DIR}`);
console.log(`   Log level: ${CONFIG.LOG_LEVEL} (set via LOG_LEVEL env var)`);
console.log(`   File logging: ${CONFIG.ENABLE_FILE_LOGGING ? 'enabled' : 'disabled (cloud environment detected)'}`);

// Log API server configuration
console.log('🌐 API Server configuration:');
console.log(`   Host: ${CONFIG.API_HOST}`);
console.log(`   Port: ${CONFIG.API_PORT}`);
console.log(`   Environment override: STEAM_PROCESSOR_API_PORT`);

// Log backoff sequence configuration
console.log('🔄 Exponential backoff sequence for 429 errors:');
console.log(`   Sequence: ${CONFIG.BACKOFF_SEQUENCE_MINUTES.join('min → ')}min → reset`);
console.log(`   Max cooldown: ${Math.max(...CONFIG.BACKOFF_SEQUENCE_MINUTES)} minutes`);

// Log cooldown duration configuration for other errors
console.log('📅 Other error cooldown durations:');
Object.entries(CONFIG.COOLDOWN_DURATIONS).forEach(([key, duration]) => {
  const minutes = Math.round(duration / 60000);
  const hours = Math.round(duration / 3600000);
  const display = hours >= 1 ? `${hours}h` : `${minutes}m`;
  console.log(`   ${key}: ${display} (${duration}ms)`);
});

// Validate paths exist
if (!fs.existsSync(parentDir)) {
  console.error(`Parent directory not found: ${parentDir}`);
}

// Ensure log directory exists (only if file logging is enabled)
if (CONFIG.ENABLE_FILE_LOGGING) {
  fs.ensureDirSync(CONFIG.LOG_DIR);
}

module.exports = CONFIG;