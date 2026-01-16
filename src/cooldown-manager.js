// steam-id-processor/src/cooldown-manager.js
const fs = require('fs-extra');
const path = require('path');
const logger = require('./utils/logger');
const axios = require('axios');

/**
 * Manages rate limit cooldowns for Steam API endpoints
 * Handles exponential backoff for 429 errors and fixed cooldowns for connection errors
 */
class CooldownManager {
  constructor(configDir, cooldownDurations, backoffSequence) {
    logger.debug(`🔍 [DEBUG] CooldownManager constructor called`);
    logger.debug(`🔍 [DEBUG]   configDir: ${configDir}`);
    logger.debug(`🔍 [DEBUG]   cooldownDurations: ${JSON.stringify(cooldownDurations)}`);
    logger.debug(`🔍 [DEBUG]   backoffSequence: ${JSON.stringify(backoffSequence)}`);

    this.cooldownPath = path.join(configDir, 'endpoint_cooldowns.json');
    this.cooldowns = null;
    this.cooldownDurations = cooldownDurations;
    this.backoffSequence = backoffSequence;
    this.backoffLevels = new Map(); // key: endpoint name, value: backoff level

    // Validate backoffSequence
    if (!this.backoffSequence || !Array.isArray(this.backoffSequence) || this.backoffSequence.length === 0) {
      logger.warn(`Invalid backoffSequence, using fallback: [1, 2, 4, 8, 16, 32, 60, 120, 240, 480]`);
      this.backoffSequence = [1, 2, 4, 8, 16, 32, 60, 120, 240, 480];
    }

    logger.debug(`🔍 [DEBUG] Final backoffSequence: ${JSON.stringify(this.backoffSequence)}`);

    this.initializeCooldowns();
    this.initializeBackoffLevelsFromFile();
  }

  initializeCooldowns() {
    try {
      if (fs.existsSync(this.cooldownPath)) {
        this.cooldowns = JSON.parse(fs.readFileSync(this.cooldownPath, 'utf8'));
        logger.info('Endpoint cooldowns loaded');
        this.ensureCooldownStructure();
      } else {
        this.createDefaultCooldowns();
        logger.info('Created default endpoint cooldowns file');
      }
    } catch (error) {
      logger.error(`Error initializing endpoint cooldowns: ${error.message}`);
      this.createDefaultCooldowns();
    }
  }

  createDefaultCooldowns() {
    this.cooldowns = {
      endpoint_cooldowns: {}
    };
    this.saveCooldowns();
  }

  ensureCooldownStructure() {
    if (!this.cooldowns.endpoint_cooldowns) {
      this.cooldowns.endpoint_cooldowns = {};
    }
    this.saveCooldowns();
  }

  saveCooldowns() {
    try {
      fs.writeFileSync(this.cooldownPath, JSON.stringify(this.cooldowns, null, 2));
    } catch (error) {
      logger.error(`Error saving endpoint cooldowns: ${error.message}`);
    }
  }

  /**
   * Clean up expired cooldowns
   * @returns {number} Number of cooldowns cleaned up
   */
  cleanupExpiredCooldowns() {
    const now = Date.now();
    let cleanupCount = 0;

    const endpointNames = Object.keys(this.cooldowns.endpoint_cooldowns);

    for (const endpoint of endpointNames) {
      const cooldown = this.cooldowns.endpoint_cooldowns[endpoint];

      if (cooldown.cooldown_until <= now) {
        if (cooldown.reason === '429') {
          logger.debug(`🔓 Cooldown expired for endpoint ${endpoint} (preserving backoff level in memory)`);
        } else {
          logger.debug(`🔓 Cooldown expired for endpoint ${endpoint}`);
        }

        delete this.cooldowns.endpoint_cooldowns[endpoint];
        cleanupCount++;
      }
    }

    if (cleanupCount > 0) {
      this.saveCooldowns();
      logger.info(`🧹 Cleaned up ${cleanupCount} expired endpoint cooldowns`);
    }

    return cleanupCount;
  }

  /**
   * Check if a specific endpoint is available (not in cooldown)
   * @param {string} endpoint - Endpoint name
   * @returns {boolean}
   */
  isEndpointAvailable(endpoint) {
    const cooldown = this.cooldowns.endpoint_cooldowns[endpoint];
    if (!cooldown) {
      return true;
    }

    const isAvailable = cooldown.cooldown_until <= Date.now();
    return isAvailable;
  }

  /**
   * Mark an endpoint as being in cooldown
   * @param {string} endpoint - Endpoint name
   * @param {string} errorType - Type of error (429, connection_error, timeout, dns_failure)
   * @param {string} errorMessage - Error message
   */
  markEndpointCooldown(endpoint, errorType, errorMessage) {
    logger.debug(`🔍 [DEBUG] markEndpointCooldown called:`);
    logger.debug(`🔍 [DEBUG]   endpoint: ${endpoint}`);
    logger.debug(`🔍 [DEBUG]   errorType: ${errorType}`);
    logger.debug(`🔍 [DEBUG]   errorMessage: ${errorMessage}`);

    // Handle 429 errors with exponential backoff
    if (errorType === '429') {
      const currentLevel = this.backoffLevels.get(endpoint) || 0;
      const newLevel = Math.min(currentLevel + 1, this.backoffSequence.length - 1);

      // Update in-memory backoff level
      this.backoffLevels.set(endpoint, newLevel);

      const cooldownMinutes = this.backoffSequence[newLevel];
      const cooldownDuration = cooldownMinutes * 60 * 1000;
      const cooldownUntil = Date.now() + cooldownDuration;

      this.cooldowns.endpoint_cooldowns[endpoint] = {
        cooldown_until: cooldownUntil,
        reason: '429',
        backoff_level: newLevel,
        applied_at: Date.now(),
        error_message: errorMessage,
        duration_minutes: cooldownMinutes
      };

      this.saveCooldowns();

      const cooldownUntilDate = new Date(cooldownUntil);
      const sequencePosition = `${newLevel + 1}/${this.backoffSequence.length}`;

      logger.warn(`🔒 Rate limit (429) cooldown applied to ${endpoint} endpoint`);
      logger.warn(`    Backoff level: ${currentLevel} → ${newLevel} (${sequencePosition}) → ${cooldownMinutes} minutes`);
      logger.warn(`    Available again at: ${cooldownUntilDate.toLocaleString()}`);
      logger.warn(`    Next level would be: ${this.getNextBackoffDuration(newLevel)} minutes`);

      return;
    }

    // Handle other error types with fixed durations
    let cooldownDuration;
    let description;

    if (errorType === 'connection_error') {
      cooldownDuration = this.cooldownDurations.connection_reset;
      description = `Connection error on ${endpoint} endpoint`;
    } else if (errorType === 'timeout') {
      cooldownDuration = this.cooldownDurations.timeout;
      description = `Timeout error on ${endpoint} endpoint`;
    } else if (errorType === 'dns_failure') {
      cooldownDuration = this.cooldownDurations.dns_failure;
      description = `DNS failure on ${endpoint} endpoint`;
    } else {
      cooldownDuration = 60000; // 1 minute fallback
      description = `Unknown error on ${endpoint} endpoint`;
    }

    const cooldownUntil = Date.now() + cooldownDuration;

    this.cooldowns.endpoint_cooldowns[endpoint] = {
      cooldown_until: cooldownUntil,
      reason: errorType,
      duration_used: cooldownDuration,
      applied_at: Date.now(),
      error_message: errorMessage
    };

    this.saveCooldowns();

    const cooldownUntilDate = new Date(cooldownUntil);
    const cooldownMinutes = Math.ceil(cooldownDuration / 60000);

    logger.warn(`🔒 Marked ${endpoint} endpoint as in cooldown for ${cooldownMinutes} minutes until ${cooldownUntilDate.toLocaleString()}`);
    logger.warn(`    Reason: ${description} - ${errorMessage}`);
  }

  /**
   * Get the next backoff duration for logging purposes
   * @param {number} currentLevel - Current backoff level
   * @returns {number} Next backoff duration in minutes
   */
  getNextBackoffDuration(currentLevel) {
    const nextLevel = currentLevel + 1;
    if (nextLevel >= this.backoffSequence.length) {
      return this.backoffSequence[0]; // Reset to first level
    }
    return this.backoffSequence[nextLevel];
  }

  /**
   * Reset backoff level on successful request
   * @param {string} endpoint - Endpoint name
   */
  resetBackoffOnSuccess(endpoint) {
    const hadBackoffLevel = this.backoffLevels.has(endpoint);
    const previousLevel = this.backoffLevels.get(endpoint) || 0;

    if (hadBackoffLevel) {
      this.backoffLevels.delete(endpoint);
      logger.info(`✅ Reset 429 backoff for ${endpoint} endpoint (was level ${previousLevel})`);
    }

    // Also clean up file state if it exists and was a 429 cooldown
    const cooldown = this.cooldowns.endpoint_cooldowns[endpoint];
    if (cooldown && cooldown.reason === '429') {
      delete this.cooldowns.endpoint_cooldowns[endpoint];
      this.saveCooldowns();
    }
  }

  /**
   * Check if endpoint is in cooldown
   * @param {string} endpoint - Endpoint name
   * @returns {boolean}
   */
  isEndpointInCooldown(endpoint) {
    this.cleanupExpiredCooldowns();
    return !this.isEndpointAvailable(endpoint);
  }

  /**
   * Alias for compatibility with old proxy-manager code
   * Check if endpoint is in cooldown
   * @param {string} endpoint - Endpoint name
   * @returns {boolean}
   */
  areAllConnectionsInCooldownForEndpoint(endpoint) {
    return this.isEndpointInCooldown(endpoint);
  }

  /**
   * Get next available time for an endpoint
   * @param {string} endpoint - Endpoint name
   * @returns {number} Milliseconds until endpoint is available (0 if available now)
   */
  getNextAvailableTimeForEndpoint(endpoint) {
    const cooldown = this.cooldowns.endpoint_cooldowns[endpoint];
    if (!cooldown) {
      return 0;
    }

    return Math.max(0, cooldown.cooldown_until - Date.now());
  }

  /**
   * Create axios instance for a specific endpoint
   * @param {string} endpoint - Full URL of the endpoint
   * @returns {Object} Axios instance or error object
   */
  createAxiosInstance(endpoint) {
    const endpointName = this.getEndpointName(endpoint);

    if (!this.isEndpointAvailable(endpointName)) {
      const nextAvailableIn = this.getNextAvailableTimeForEndpoint(endpointName);
      const waitTimeMin = Math.ceil(nextAvailableIn / 60000);

      logger.warn(`⏳ Endpoint ${endpointName} in cooldown. Next available in ~${waitTimeMin} minutes.`);
      return {
        allInCooldown: true,
        nextAvailableIn: nextAvailableIn,
        endpointName: endpointName
      };
    }

    const axiosConfig = {
      timeout: this.getTimeoutForEndpoint(endpointName),
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
      },
      _connectionInfo: {
        endpointName: endpointName
      }
    };

    return axios.create(axiosConfig);
  }

  /**
   * Extract endpoint name from URL
   * @param {string} url - Full endpoint URL
   * @returns {string} Endpoint name
   */
  getEndpointName(url) {
    if (url.includes('GetFriendList')) return 'friends';
    if (url.includes('inventory')) return 'inventory';
    if (url.includes('GetSteamLevel')) return 'steam_level';
    if (url.includes('GetAnimatedAvatar')) return 'animated_avatar';
    if (url.includes('GetAvatarFrame')) return 'avatar_frame';
    if (url.includes('GetMiniProfileBackground')) return 'mini_profile_background';
    if (url.includes('GetProfileBackground')) return 'profile_background';

    return 'other';
  }

  /**
   * Get timeout duration for specific endpoint
   * @param {string} endpointName - Endpoint name
   * @returns {number} Timeout in milliseconds
   */
  getTimeoutForEndpoint(endpointName) {
    if (endpointName === 'inventory') return 25000;
    return 15000;
  }

  /**
   * Handle request errors and mark cooldowns appropriately
   * @param {Error} error - The error object
   * @param {Object} axiosConfig - Axios configuration object
   * @returns {Object} Error details
   */
  handleRequestError(error, axiosConfig) {
    const connectionInfo = axiosConfig._connectionInfo;
    if (!connectionInfo) {
      logger.error('No connection info available for error handling');
      return { error };
    }

    const { endpointName } = connectionInfo;

    // Check if rate limited
    if (error.response && error.response.status === 429) {
      logger.warn(`Rate limit (429) hit for ${endpointName}`);
      this.markEndpointCooldown(endpointName, '429', error.message);
      return { rateLimited: true, error, endpointName };
    }

    // Handle connection errors
    if (this.isConnectionError(error)) {
      const errorType = this.categorizeConnectionError(error);
      logger.warn(`${errorType} for ${endpointName}: ${error.message}`);
      this.markEndpointCooldown(endpointName, errorType, error.message);
      return { connectionError: true, error, endpointName, errorType };
    }

    // Handle other errors normally
    return { error };
  }

  /**
   * Detect if error is a connection error
   * @param {Error} error - The error object
   * @returns {boolean}
   */
  isConnectionError(error) {
    const errorMsg = error.message || '';

    return (
      errorMsg.includes('socket disconnected') ||
      errorMsg.includes('socket hang up') ||
      errorMsg.includes('ECONNRESET') ||
      errorMsg.includes('ECONNREFUSED') ||
      errorMsg.includes('ETIMEDOUT') ||
      errorMsg.includes('EHOSTUNREACH') ||
      errorMsg.includes('timeout') ||
      errorMsg.includes('certificate') ||
      errorMsg.includes('SSL') ||
      errorMsg.includes('TLS') ||
      errorMsg.includes('ENOTFOUND')
    );
  }

  /**
   * Categorize connection errors for appropriate cooldown durations
   * @param {Error} error - The error object
   * @returns {string} Error category
   */
  categorizeConnectionError(error) {
    const errorMsg = error.message || '';

    if (errorMsg.includes('ENOTFOUND') || errorMsg.includes('EHOSTUNREACH')) {
      return 'dns_failure';
    }

    if (errorMsg.includes('timeout') || errorMsg.includes('ETIMEDOUT')) {
      return 'timeout';
    }

    return 'connection_error';
  }

  /**
   * Get connection status for monitoring
   * @returns {Object} Status object with endpoint information
   */
  getConnectionStatus() {
    this.cleanupExpiredCooldowns();

    const allEndpoints = ['friends', 'inventory', 'steam_level', 'animated_avatar',
                          'avatar_frame', 'mini_profile_background', 'profile_background'];

    const status = {
      connections: [],
      endpointSummary: {}
    };

    const now = Date.now();

    // Single connection status
    const connStatus = {
      index: 0,
      type: 'direct',
      url: null,
      availableEndpoints: 0,
      totalEndpoints: allEndpoints.length,
      endpointCooldowns: {}
    };

    // Check each endpoint
    for (const endpoint of allEndpoints) {
      const cooldown = this.cooldowns.endpoint_cooldowns[endpoint];

      if (!cooldown || cooldown.cooldown_until <= now) {
        connStatus.availableEndpoints++;
        connStatus.endpointCooldowns[endpoint] = 'available';
      } else {
        const remainingMs = Math.max(0, cooldown.cooldown_until - now);
        connStatus.endpointCooldowns[endpoint] = {
          status: 'cooldown',
          remainingMs: remainingMs,
          reason: cooldown.reason,
          until: new Date(cooldown.cooldown_until).toLocaleString()
        };
      }
    }

    status.connections.push(connStatus);

    // Create endpoint summary
    for (const endpoint of allEndpoints) {
      const isAvailable = this.isEndpointAvailable(endpoint);

      status.endpointSummary[endpoint] = {
        availableConnections: isAvailable ? 1 : 0,
        totalConnections: 1,
        nextAvailableIn: isAvailable ? 0 : this.getNextAvailableTimeForEndpoint(endpoint)
      };
    }

    return status;
  }

  /**
   * Initialize backoff levels from file on startup
   */
  initializeBackoffLevelsFromFile() {
    try {
      if (!this.cooldowns || !this.cooldowns.endpoint_cooldowns) {
        logger.debug('No cooldowns data available for backoff level initialization');
        return;
      }

      let initializedCount = 0;

      for (const [endpoint, cooldown] of Object.entries(this.cooldowns.endpoint_cooldowns)) {
        if (cooldown.reason === '429' && typeof cooldown.backoff_level === 'number') {
          this.backoffLevels.set(endpoint, cooldown.backoff_level);
          initializedCount++;
          logger.debug(`Initialized backoff level ${cooldown.backoff_level} for endpoint ${endpoint}`);
        }
      }

      if (initializedCount > 0) {
        logger.info(`✅ Initialized ${initializedCount} backoff levels from file on startup`);
      } else {
        logger.debug('No backoff levels found in file to initialize');
      }

    } catch (error) {
      logger.error(`Error initializing backoff levels from file: ${error.message}`);
    }
  }
}

module.exports = CooldownManager;
