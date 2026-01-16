// steam-id-processor/src/steam-validator.js
const fs = require('fs-extra');
const path = require('path');
const logger = require('./utils/logger');
const CooldownManager = require('./cooldown-manager');

// Load environment variables
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });

class SteamValidator {
  constructor(config) {
    this.config = config;
    this.apiKey = null;
    this.lastApiCallTime = 0;
    this.minApiCallInterval = 1000; // 1 second between calls
    this.loadApiKey();
    
    // DEBUG: Verificar que config tiene la secuencia de backoff
    logger.debug(`🔍 [DEBUG] CONFIG object keys: ${Object.keys(config).join(', ')}`);
    logger.debug(`🔍 [DEBUG] CONFIG.BACKOFF_SEQUENCE_MINUTES: ${JSON.stringify(config.BACKOFF_SEQUENCE_MINUTES)}`);
    logger.debug(`🔍 [DEBUG] CONFIG.BACKOFF_SEQUENCE_MINUTES type: ${typeof config.BACKOFF_SEQUENCE_MINUTES}`);
    
    if (!config.BACKOFF_SEQUENCE_MINUTES) {
      logger.debug(`🔍 [DEBUG] CRITICAL: BACKOFF_SEQUENCE_MINUTES is missing from config!`);
      logger.debug(`🔍 [DEBUG] Full config: ${JSON.stringify(config, null, 2)}`);
    }
    
    // Initialize the cooldown manager with cooldown durations AND backoff sequence
    const configDir = path.dirname(path.join(__dirname, '../config'));
    logger.debug(`🔍 [DEBUG] Initializing CooldownManager with:`);
    logger.debug(`🔍 [DEBUG]   configDir: ${configDir}`);
    logger.debug(`🔍 [DEBUG]   cooldownDurations: ${JSON.stringify(config.COOLDOWN_DURATIONS)}`);
    logger.debug(`🔍 [DEBUG]   backoffSequence: ${JSON.stringify(config.BACKOFF_SEQUENCE_MINUTES)}`);

    this.cooldownManager = new CooldownManager(
      configDir,
      config.COOLDOWN_DURATIONS,
      config.BACKOFF_SEQUENCE_MINUTES
    );

    logger.debug(`🔍 [DEBUG] CooldownManager initialized`);
    
    // Add a property to track deferred checks
    this.deferredChecks = new Map();
  
    // NEW: Initialize deferred checks from queue file on startup
    this.initializeDeferredChecksFromQueue();
  }

  async initializeDeferredChecksFromQueue() {
    try {
      logger.debug(`🔍 [DEBUG] Initializing deferred checks from queue file...`);
      
      // We need access to the queue manager, but it's not available in constructor
      // So we'll delay this initialization
      setTimeout(async () => {
        try {
          // Read queue file directly
          const queuePath = path.join(__dirname, '../profiles_queue.json');
          if (!fs.existsSync(queuePath)) {
            logger.debug(`🔍 [DEBUG] Queue file doesn't exist yet: ${queuePath}`);
            return;
          }
          
          const queueData = JSON.parse(fs.readFileSync(queuePath, 'utf8'));
          if (!Array.isArray(queueData)) {
            logger.debug(`🔍 [DEBUG] Invalid queue data format`);
            return;
          }
          
          let totalDeferredFound = 0;
          let profilesWithDeferred = 0;
          
          for (const profile of queueData) {
            const steamId = profile.steam_id;
            const deferredChecks = Object.entries(profile.checks)
              .filter(([_, status]) => status === "deferred")
              .map(([checkName, _]) => checkName);
            
            if (deferredChecks.length > 0) {
              profilesWithDeferred++;
              totalDeferredFound += deferredChecks.length;
              
              // Add to in-memory deferred checks Map
              this.deferredChecks.set(steamId, new Set(deferredChecks));
              logger.debug(`🔍 [DEBUG] Loaded ${deferredChecks.length} deferred checks for ${steamId}: ${deferredChecks.join(', ')}`);
            }
          }
          
          logger.info(`✅ Initialized ${totalDeferredFound} deferred checks from ${profilesWithDeferred} profiles`);
          
        } catch (error) {
          logger.error(`Error initializing deferred checks from queue: ${error.message}`);
        }
      }, 1000); // Wait 1 second for other components to initialize
      
    } catch (error) {
      logger.error(`Error in initializeDeferredChecksFromQueue: ${error.message}`);
    }
  }

  loadApiKey() {
    // Load Steam API key from environment variables
    this.apiKey = process.env.STEAM_API_KEY;
    
    if (!this.apiKey) {
      logger.warn('Steam API key not found in environment variables (STEAM_API_KEY)');
      logger.warn('Some Steam API calls (level, friends) will fail without this key');
    } else {
      logger.info('Steam API key loaded successfully from environment');
    }
  }

  async respectRateLimit() {
    const currentTime = Date.now();
    const timeSinceLast = currentTime - this.lastApiCallTime;
    
    if (timeSinceLast < this.minApiCallInterval) {
      const waitTime = this.minApiCallInterval - timeSinceLast;
      logger.debug(`Rate limiting: Waiting ${waitTime}ms before next API call`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
    
    this.lastApiCallTime = Date.now();
  }

  async makeApiRequest(url) {
    await this.respectRateLimit();
    
    const endpointName = this.cooldownManager.getEndpointName(url);
    logger.debug(`🔍 [DEBUG] Starting makeApiRequest for ${endpointName} endpoint: ${url}`);
    
    try {
      // Get axios instance for this endpoint
      logger.debug(`🔍 [DEBUG] Getting axios instance for ${endpointName}`);
      const axiosInstance = this.cooldownManager.createAxiosInstance(url);
      
      // Check if all connections are in cooldown for this endpoint
      if (axiosInstance && axiosInstance.allInCooldown) {
        logger.debug(`🔍 [DEBUG] All connections in cooldown for ${endpointName}`);
        return {
          allInCooldown: true,
          nextAvailableIn: axiosInstance.nextAvailableIn,
          endpointName: axiosInstance.endpointName
        };
      }
      
      // Validate that we got a proper axios instance
      if (!axiosInstance || typeof axiosInstance.get !== 'function') {
        logger.debug(`🔍 [DEBUG] Invalid axios instance for ${endpointName}: ${typeof axiosInstance}`);
        logger.debug(`🔍 [DEBUG] axiosInstance properties: ${Object.keys(axiosInstance || {}).join(', ')}`);
        return { 
          success: false, 
          error: `Invalid axios instance: ${typeof axiosInstance}` 
        };
      }
      
      logger.debug(`🔍 [DEBUG] Connection info: ${JSON.stringify(axiosInstance.defaults._connectionInfo)}`);

      logger.debug(`🔍 [DEBUG] Making HTTP request to ${endpointName} endpoint`);

      const response = await axiosInstance.get(url);

      logger.debug(`🔍 [DEBUG] HTTP request successful for ${endpointName}, status: ${response.status}`);

      // Reset backoff on successful request
      logger.debug(`🔍 [DEBUG] Resetting backoff for endpoint ${endpointName}`);
      this.cooldownManager.resetBackoffOnSuccess(endpointName);
      
      logger.debug(`✅ ${endpointName} request successful`);
      return { success: true, data: response.data };
      
    } catch (error) {
      const errorStatus = error.response ? error.response.status : 'no status';
      const errorMessage = error.message || 'Unknown error';
      
    if (!(errorStatus === 403 && endpointName === 'inventory')) {
      logger.debug(`🔍 [DEBUG] HTTP request failed for ${endpointName}`);
    }
    logger.debug(`🔍 [DEBUG] Error status: ${errorStatus}`);
    logger.debug(`🔍 [DEBUG] Error message: ${errorMessage}`);
    if (!(errorStatus === 403 && endpointName === 'inventory')) {
      logger.debug(`🔍 [DEBUG] Error stack: ${error.stack}`);
    }
      
      // Special handling for inventory 403 errors (private inventories are desirable)
      if (endpointName === 'inventory' && error.response && error.response.status === 403) {
        logger.info(`✅ Private inventory detected for ${endpointName} endpoint (403) - this is good!`);
        return { success: false, error: errorMessage, errorObj: error, isPrivateInventory: true };
      }
      
      // Special handling for friends 401 errors (private profiles)
      if (endpointName === 'friends' && error.response && error.response.status === 401) {
        logger.info(`✅ Private profile detected for ${endpointName} endpoint (401) - this is expected`);
        return { success: false, error: errorMessage, errorObj: error, isPrivateProfile: true };
      }
      
      logger.debug(`🔍 [DEBUG] Getting axios instance for error handling`);
      // Get the axios instance again to ensure we have a valid one for error handling
      const axiosInstanceForError = this.cooldownManager.createAxiosInstance(url);
      
      logger.debug(`🔍 [DEBUG] axiosInstanceForError type: ${typeof axiosInstanceForError}`);
      if (axiosInstanceForError) {
        logger.debug(`🔍 [DEBUG] axiosInstanceForError.allInCooldown: ${axiosInstanceForError.allInCooldown}`);
        if (axiosInstanceForError.defaults) {
          logger.debug(`🔍 [DEBUG] axiosInstanceForError.defaults exists`);
        } else {
          logger.debug(`🔍 [DEBUG] axiosInstanceForError.defaults is missing`);
        }
      }
      
      // Only try to handle the error if we have a valid axios instance with defaults
      if (axiosInstanceForError && 
          !axiosInstanceForError.allInCooldown && 
          axiosInstanceForError.defaults) {
        
        logger.debug(`🔍 [DEBUG] Calling handleRequestError for ${endpointName}`);
        
        // Let ProxyManager handle the error and mark cooldowns
        const errorResult = this.cooldownManager.handleRequestError(error, axiosInstanceForError.defaults);
        
        logger.debug(`🔍 [DEBUG] handleRequestError result: ${JSON.stringify({
          rateLimited: errorResult.rateLimited,
          socksError: errorResult.socksError,
          connectionError: errorResult.connectionError
        })}`);
        
        if (errorResult.rateLimited || errorResult.socksError || errorResult.connectionError) {
          logger.warn(`⚠️ Error handled by ProxyManager for ${endpointName}: ${errorResult.error.message}`);
          
          logger.debug(`🔍 [DEBUG] Calling retryWithNextConnection for ${endpointName}`);
          // Try to get another connection immediately
          return this.retryWithNextConnection(url, endpointName);
        }
      } else {
        logger.debug(`🔍 [DEBUG] Cannot handle error properly for ${endpointName}`);
        logger.debug(`🔍 [DEBUG] axiosInstanceForError: ${typeof axiosInstanceForError}`);
        logger.debug(`🔍 [DEBUG] allInCooldown: ${axiosInstanceForError?.allInCooldown}`);
        logger.debug(`🔍 [DEBUG] has defaults: ${!!axiosInstanceForError?.defaults}`);
      }
      
      // Handle other errors (these are the actual problems)
      logger.error(`❌ Request error (${errorStatus}) on ${endpointName}: ${errorMessage}`);
      return { success: false, error: errorMessage, errorObj: error };
    }
  }

  async retryWithNextConnection(url, endpointName) {
    // Check if we have another connection available for this endpoint
    if (!this.cooldownManager.areAllConnectionsInCooldownForEndpoint(endpointName)) {
      logger.info(`🔄 Retrying ${endpointName} with next available connection...`);
      return this.makeApiRequest(url);
    }
    
    // All connections in cooldown for this endpoint
    const nextAvailableIn = this.cooldownManager.getNextAvailableTimeForEndpoint(endpointName);
    const waitTimeMin = Math.ceil(nextAvailableIn / 60000);
    
    logger.warn(`❌ All connections in cooldown for ${endpointName}, deferring request (~${waitTimeMin} minutes)`);
    return {
      allInCooldown: true,
      nextAvailableIn: nextAvailableIn,
      endpointName: endpointName
    };
  }

  async checkAnimatedAvatar(steamId) {
    try {
      const url = `https://api.steampowered.com/IPlayerService/GetAnimatedAvatar/v1/?steamid=${steamId}`;
      const result = await this.makeApiRequest(url);
      
      // Check if all connections are in cooldown
      if (result.allInCooldown) {
        logger.warn(`Animated avatar check for ${steamId} deferred - all connections in cooldown`);
        this.addToDeferredChecks(steamId, 'animated_avatar');
        return { 
          success: false, 
          deferred: true,
          error: "All connections in cooldown", 
          nextAvailableIn: result.nextAvailableIn 
        };
      }
      
      if (!result.success) {
        logger.error(`Animated avatar check failed for ${steamId}: ${result.error}`);
        return { success: false, error: result.error };
      }
      
      const data = result.data;
      
      if (data.response && 'avatar' in data.response) {
        // Check if avatar is empty object or empty array
        const hasAnimatedAvatar = data.response.avatar && 
                                Object.keys(data.response.avatar).length > 0;
        return {
          success: true,
          passed: !hasAnimatedAvatar,
          details: hasAnimatedAvatar ? data.response : {}
        };
      }
      
      return { success: false, error: "Unexpected API response" };
    } catch (error) {
      logger.error(`Animated avatar check failed for ${steamId}: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async checkAvatarFrame(steamId) {
    try {
      const url = `https://api.steampowered.com/IPlayerService/GetAvatarFrame/v1/?steamid=${steamId}`;
      const result = await this.makeApiRequest(url);
      
      // Check if all connections are in cooldown
      if (result.allInCooldown) {
        logger.warn(`Avatar frame check for ${steamId} deferred - all connections in cooldown`);
        this.addToDeferredChecks(steamId, 'avatar_frame');
        return { 
          success: false, 
          deferred: true,
          error: "All connections in cooldown", 
          nextAvailableIn: result.nextAvailableIn 
        };
      }
      
      if (!result.success) {
        logger.error(`Avatar frame check failed for ${steamId}: ${result.error}`);
        return { success: false, error: result.error };
      }
      
      const data = result.data;
      
      if (data.response && 'avatar_frame' in data.response) {
        // Check if avatar_frame is empty object
        const hasFrame = data.response.avatar_frame && 
                        Object.keys(data.response.avatar_frame).length > 0;
        return {
          success: true,
          passed: !hasFrame,
          details: hasFrame ? data.response : {}
        };
      }
      
      return { success: false, error: "Unexpected API response" };
    } catch (error) {
      logger.error(`Avatar frame check failed for ${steamId}: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async checkMiniProfileBackground(steamId) {
    try {
      const url = `https://api.steampowered.com/IPlayerService/GetMiniProfileBackground/v1/?steamid=${steamId}`;
      const result = await this.makeApiRequest(url);
      
      // Check if all connections are in cooldown
      if (result.allInCooldown) {
        logger.warn(`Mini profile background check for ${steamId} deferred - all connections in cooldown`);
        this.addToDeferredChecks(steamId, 'mini_profile_background');
        return { 
          success: false, 
          deferred: true,
          error: "All connections in cooldown", 
          nextAvailableIn: result.nextAvailableIn 
        };
      }
      
      if (!result.success) {
        logger.error(`Mini profile background check failed for ${steamId}: ${result.error}`);
        return { success: false, error: result.error };
      }
      
      const data = result.data;
      
      if (data.response && 'profile_background' in data.response) {
        // Check if profile_background is empty object
        const hasBackground = data.response.profile_background && 
                            Object.keys(data.response.profile_background).length > 0;
        return {
          success: true,
          passed: !hasBackground,
          details: hasBackground ? data.response : {}
        };
      }
      
      return { success: false, error: "Unexpected API response" };
    } catch (error) {
      logger.error(`Mini profile background check failed for ${steamId}: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async checkProfileBackground(steamId) {
    try {
      const url = `https://api.steampowered.com/IPlayerService/GetProfileBackground/v1/?steamid=${steamId}`;
      const result = await this.makeApiRequest(url);
      
      // Check if all connections are in cooldown
      if (result.allInCooldown) {
        logger.warn(`Profile background check for ${steamId} deferred - all connections in cooldown`);
        this.addToDeferredChecks(steamId, 'profile_background');
        return { 
          success: false, 
          deferred: true,
          error: "All connections in cooldown", 
          nextAvailableIn: result.nextAvailableIn 
        };
      }
      
      if (!result.success) {
        logger.error(`Profile background check failed for ${steamId}: ${result.error}`);
        return { success: false, error: result.error };
      }
      
      const data = result.data;
      
      if (data.response && 'profile_background' in data.response) {
        // Check if profile_background is empty object
        const hasBackground = data.response.profile_background && 
                            Object.keys(data.response.profile_background).length > 0;
        return {
          success: true,
          passed: !hasBackground,
          details: hasBackground ? data.response : {}
        };
      }
      
      return { success: false, error: "Unexpected API response" };
    } catch (error) {
      logger.error(`Profile background check failed for ${steamId}: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async checkSteamLevel(steamId) {
    try {
      if (!this.apiKey) {
        return { 
          success: false, 
          error: "Steam API key not available (check STEAM_API_KEY environment variable)" 
        };
      }
      
      const url = `https://api.steampowered.com/IPlayerService/GetSteamLevel/v1/?key=${this.apiKey}&steamid=${steamId}`;
      const result = await this.makeApiRequest(url);
      
      // Check if all connections are in cooldown
      if (result.allInCooldown) {
        logger.warn(`Steam level check for ${steamId} deferred - all connections in cooldown`);
        this.addToDeferredChecks(steamId, 'steam_level');
        return { 
          success: false, 
          deferred: true,
          error: "All connections in cooldown", 
          nextAvailableIn: result.nextAvailableIn 
        };
      }
      
      if (!result.success) {
        logger.error(`Steam level check failed for ${steamId}: ${result.error}`);
        return { success: false, error: result.error };
      }
      
      const data = result.data;
      
      if (data.response) {
        // If response is empty, this is a private profile
        if (Object.keys(data.response).length === 0) {
          logger.info(`Private profile detected for ${steamId} (empty GetSteamLevel response)`);
          return {
            success: true,
            passed: true,
            details: { note: "Empty response from API - private profile detected" },
            level: 0,
            isPrivateProfile: true
          };
        }
        
        // Regular case - response contains player_level
        if ('player_level' in data.response) {
          const playerLevel = data.response.player_level;
          return {
            success: true,
            passed: playerLevel <= 13,
            details: { player_level: playerLevel },
            level: playerLevel,
            isPrivateProfile: false
          };
        }
      }
      
      logger.error(`Unexpected API response format for Steam level check: ${JSON.stringify(data)}`);
      return { success: false, error: "Unexpected API response" };
    } catch (error) {
      logger.error(`Steam level check failed for ${steamId}: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async checkFriends(steamId) {
    try {
      if (!this.apiKey) {
        return { 
          success: false, 
          error: "Steam API key not available (check STEAM_API_KEY environment variable)" 
        };
      }
      
      const url = `https://api.steampowered.com/ISteamUser/GetFriendList/v0001/?key=${this.apiKey}&steamid=${steamId}&relationship=friend`;
      const result = await this.makeApiRequest(url);
      
      // Check if all connections are in cooldown
      if (result.allInCooldown) {
        logger.warn(`Friends check for ${steamId} deferred - all connections in cooldown`);
        this.addToDeferredChecks(steamId, 'friends');
        return { 
          success: false, 
          deferred: true,
          error: "All connections in cooldown", 
          nextAvailableIn: result.nextAvailableIn 
        };
      }
      
      if (!result.success) {
        // Special case for private profiles (401 error)
        if (result.errorObj && result.errorObj.response && result.errorObj.response.status === 401) {
          return {
            success: true,
            passed: true,
            details: { error: "Private profile - cannot check friends" },
            count: 0
          };
        }
        
        logger.error(`Friends check failed for ${steamId}: ${result.error}`);
        return { success: false, error: result.error };
      }
      
      const data = result.data;
      
      if (data.friendslist && data.friendslist.friends) {
        const friendsCount = data.friendslist.friends.length;
        return {
          success: true,
          passed: friendsCount <= 60,
          details: {
            friends_count: friendsCount,
            sample_friends: data.friendslist.friends.slice(0, 3)
          },
          count: friendsCount
        };
      }
      
      logger.error(`Unexpected API response format for friends check: ${JSON.stringify(data)}`);
      return { success: false, error: "Unexpected API response" };
    } catch (error) {
      // Special case for private profiles (401 error)
      if (error.response && error.response.status === 401) {
        return {
          success: true,
          passed: true,
          details: { error: "Private profile - cannot check friends" },
          count: 0
        };
      }
      
      logger.error(`Friends check failed for ${steamId}: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  async checkCsgoInventory(steamId) {
    try {
      const url = `https://steamcommunity.com/inventory/${steamId}/730/2`;
      const result = await this.makeApiRequest(url);
      
      // Check if all connections are in cooldown
      if (result.allInCooldown) {
        logger.warn(`CS:GO inventory check for ${steamId} deferred - all connections in cooldown`);
        this.addToDeferredChecks(steamId, 'csgo_inventory');
        return { 
          success: false, 
          deferred: true,
          error: "All connections in cooldown", 
          nextAvailableIn: result.nextAvailableIn 
        };
      }
      
      if (!result.success) {
        // Special case for private inventories (403) - this is actually good!
        if (result.isPrivateInventory || 
            (result.errorObj && result.errorObj.response && result.errorObj.response.status === 403)) {
          logger.info(`CS:GO inventory check passed for ${steamId} (private inventory - this is good!)`);
          return {
            success: true,
            passed: true,
            details: { note: "Private inventory - cannot check (this is desirable)" }
          };
        }
        
        // Special case for unauthorized access (401)
        if (result.errorObj && result.errorObj.response && result.errorObj.response.status === 401) {
          logger.info(`CS:GO inventory check passed for ${steamId} (unauthorized - this is good!)`);
          return {
            success: true,
            passed: true,
            details: { note: "Unauthorized access - cannot check (this is desirable)" }
          };
        }
        
        logger.error(`CS:GO inventory check failed for ${steamId}: ${result.error}`);
        return { success: false, error: result.error };
      }
      
      const data = result.data;
      
      // Process results - pass if response is null or empty
      if (data === null || Object.keys(data).length === 0) {
        logger.info(`CS:GO inventory check passed for ${steamId} (empty inventory)`);
        return {
          success: true,
          passed: true,
          details: { note: "Empty inventory response" }
        };
      }
      
      // Check if inventory is actually empty
      if (typeof data === 'object' && 
          (!data.assets || data.assets.length === 0) && 
          (!data.descriptions || data.descriptions.length === 0)) {
        logger.info(`CS:GO inventory check passed for ${steamId} (empty inventory structure)`);
        return {
          success: true,
          passed: true,
          details: { note: "Empty inventory structure" }
        };
      }
      
      // Inventory exists - this is bad for your filtering criteria
      const itemCount = data.assets ? data.assets.length : 0;
      logger.info(`CS:GO inventory check failed for ${steamId} (found ${itemCount} items - will be filtered out)`);
      return {
        success: true,
        passed: false,
        details: {
          note: "Public inventory with items found",
          item_count: itemCount,
          sample_items: data.assets ? data.assets.slice(0, 3) : []
        }
      };
    } catch (error) {
      // Special case for private inventories (these are the good ones!)
      if (error.response && (error.response.status === 401 || error.response.status === 403)) {
        const errorType = error.response.status === 401 ? "Unauthorized" : "Private inventory";
        logger.info(`CS:GO inventory check passed for ${steamId}: ${errorType} - this is good!`);
        return {
          success: true,
          passed: true,
          details: { note: `${errorType} - cannot check (this is desirable)` }
        };
      }
      
      logger.error(`CS:GO inventory check failed for ${steamId}: ${error.message}`);
      return { success: false, error: error.message };
    }
  }

  // Helper method to add checks to deferred list
  addToDeferredChecks(steamId, checkType) {
    if (!this.deferredChecks.has(steamId)) {
      this.deferredChecks.set(steamId, new Set());
    }
    
    this.deferredChecks.get(steamId).add(checkType);
    
    // Log the number of deferred checks
    const totalDeferred = Array.from(this.deferredChecks.keys()).length;
    logger.info(`Added ${steamId} ${checkType} check to deferred list (total deferred profiles: ${totalDeferred})`);
  }

  // Get all deferred checks
  getDeferredChecks() {
    return this.deferredChecks;
  }

  // Clear a specific deferred check
  clearDeferredCheck(steamId, checkType) {
    if (this.deferredChecks.has(steamId)) {
      const checks = this.deferredChecks.get(steamId);
      checks.delete(checkType);
      
      if (checks.size === 0) {
        this.deferredChecks.delete(steamId);
      }
      
      logger.debug(`Cleared deferred check ${checkType} for ${steamId}`);
    }
  }

  // Method to process deferred checks when connections become available
  async processDeferredChecks(queueManager) {
    let processed = 0;
    
    // Get a snapshot of current deferred checks (to avoid concurrent modification issues)
    const deferredEntries = Array.from(this.deferredChecks.entries());
    
    logger.debug(`🔍 [DEBUG] processDeferredChecks: ${deferredEntries.length} deferred entries found`);
    
    if (deferredEntries.length === 0) {
      logger.debug(`🔍 [DEBUG] processDeferredChecks: No deferred entries, returning`);
      return {
        processed: 0,
        remaining: 0
      };
    }
    
    for (const [steamId, checkTypes] of deferredEntries) {
      const checkTypesToProcess = Array.from(checkTypes);
      logger.debug(`🔍 [DEBUG] processDeferredChecks: Processing ${steamId} with checks: ${checkTypesToProcess.join(', ')}`);
      
      for (const checkType of checkTypesToProcess) {
        // Map check type to endpoint name for cooldown checking
        const endpointName = this.mapCheckTypeToEndpoint(checkType);
        logger.debug(`🔍 [DEBUG] processDeferredChecks: ${checkType} maps to endpoint ${endpointName}`);
        
        // Check if connections are available for this specific endpoint BEFORE trying
        const allInCooldown = this.cooldownManager.areAllConnectionsInCooldownForEndpoint(endpointName);
        logger.debug(`🔍 [DEBUG] processDeferredChecks: All connections in cooldown for ${endpointName}: ${allInCooldown}`);
        
        if (allInCooldown) {
          // logger.debug(`Skipping deferred check ${checkType} for ${steamId} - all connections still in cooldown`);
          continue; // Skip this check, still in cooldown
        }
        
        logger.info(`🔄 Processing deferred check ${checkType} for ${steamId} - connections now available`);
        
        let result;
        
        // Run the appropriate check
        if (checkType === 'friends') {
          result = await this.checkFriends(steamId);
        } else if (checkType === 'csgo_inventory') {
          result = await this.checkCsgoInventory(steamId);
        } else if (checkType === 'animated_avatar') {
          result = await this.checkAnimatedAvatar(steamId);
        } else if (checkType === 'avatar_frame') {
          result = await this.checkAvatarFrame(steamId);
        } else if (checkType === 'mini_profile_background') {
          result = await this.checkMiniProfileBackground(steamId);
        } else if (checkType === 'profile_background') {
          result = await this.checkProfileBackground(steamId);
        } else if (checkType === 'steam_level') {
          result = await this.checkSteamLevel(steamId);
        }
        
        logger.debug(`🔍 [DEBUG] processDeferredChecks: ${checkType} result: ${JSON.stringify(result)}`);
        
        // If check was successful, update queue and remove from deferred
        if (result.success) {
          await queueManager.updateProfileCheck(steamId, checkType, result.passed ? "passed" : "failed");
          this.clearDeferredCheck(steamId, checkType);
          processed++;
          logger.info(`✅ Deferred check ${checkType} for ${steamId} completed: ${result.passed ? 'PASSED' : 'FAILED'}`);
        } else if (result.deferred) {
          // Still in cooldown, keep in deferred list
          // logger.debug(`Deferred check ${checkType} for ${steamId} still in cooldown`);
        } else {
          // Other error occurred, log it but remove from deferred list
          logger.error(`Error processing deferred check ${checkType} for ${steamId}: ${result.error}`);
          await queueManager.updateProfileCheck(steamId, checkType, "deferred");
          this.clearDeferredCheck(steamId, checkType);
        }
        
        // If we've hit the cooldown again, stop processing for now
        if (result.deferred) {
          return {
            processed,
            remaining: this.deferredChecks.size,
            nextTryIn: result.nextAvailableIn
          };
        }
      }
    }
    
    logger.debug(`🔍 [DEBUG] processDeferredChecks: Finished processing, processed: ${processed}, remaining: ${this.deferredChecks.size}`);
    
    return {
      processed,
      remaining: this.deferredChecks.size
    };
  }

  // Helper method to map check type to endpoint name
  mapCheckTypeToEndpoint(checkType) {
    const mapping = {
      'friends': 'friends',
      'csgo_inventory': 'inventory', // This is the key mapping!
      'steam_level': 'steam_level',
      'animated_avatar': 'animated_avatar',
      'avatar_frame': 'avatar_frame',
      'mini_profile_background': 'mini_profile_background',
      'profile_background': 'profile_background'
    };
    
    return mapping[checkType] || checkType;
  }

  // Helper method to calculate final results
  calculateResults(steamId, checks) {
    // Calculate final results
    const checkResults = Object.values(checks);
    const allSuccessful = checkResults.every(result => result.success);
    const allPassed = checkResults.every(result => result.success && result.passed);
    
    // Create detailed log of check results
    const checkSummary = Object.entries(checks).map(([name, result]) => {
      return `${name}: ${result.success ? (result.passed ? 'PASS' : 'FAIL') : 'ERROR'}`;
    }).join(', ');
    
    logger.info(`Validation summary for ${steamId}: ${checkSummary}`);
    
    // Collect failed checks for detailed reporting
    const failedChecks = Object.entries(checks)
      .filter(([_, result]) => !result.success || !result.passed)
      .map(([name, result]) => ({
        name,
        success: result.success,
        passed: result.passed,
        error: result.error || null
      }));
    
    return {
      steamId,
      allSuccessful,
      allPassed,
      checks,
      failedChecks,
      checkSummary,
      firstFailedCheck: failedChecks.length > 0 ? failedChecks[0].name : null
    };
  }

  // Get the proxy manager status
  getCooldownStatus() {
    return this.cooldownManager.getConnectionStatus();
  }
  
  async testProxyConnection() {
    try {
      logger.info('Testing connection with inventory endpoint...');
      
      // Test with a real inventory endpoint to match actual usage
      // Using a known public Steam ID for testing
      const testSteamId = '76561197960434622'; // Valve's official test account
      const testUrl = `https://steamcommunity.com/inventory/${testSteamId}/730/2`;
      
      const result = await this.makeApiRequest(testUrl);
      
      if (result.success || result.isPrivateInventory) {
        logger.info('✅ Inventory endpoint test successful!');
        return true;
      } else if (result.allInCooldown) {
        logger.warn(`⚠️ All connections in cooldown for inventory endpoint`);
        return false;
      } else {
        logger.warn(`⚠️ Inventory endpoint test failed: ${result.error}`);
        return false;
      }
      
    } catch (error) {
      // Handle 401/403 errors as success (means API is working, just private profile/inventory)
      if (error.response && (error.response.status === 401 || error.response.status === 403)) {
        logger.info('✅ Inventory endpoint test successful (private inventory expected)!');
        return true;
      }
      
      logger.error(`❌ Inventory endpoint test failed: ${error.message}`);
      return false;
    }
  }

  // Test method for friends endpoint (fallback test)
  async testProxyConnectionFallback() {
    try {
      logger.info('Testing connection with friends endpoint...');
      
      if (!this.apiKey) {
        logger.warn('Cannot test friends endpoint - no API key available');
        return false;
      }
      
      // Test with friends API (requires API key but is simpler)
      const testSteamId = '76561197960434622'; // Valve's official test account
      const testUrl = `https://api.steampowered.com/ISteamUser/GetFriendList/v0001/?key=${this.apiKey}&steamid=${testSteamId}&relationship=friend`;
      
      const result = await this.makeApiRequest(testUrl);
      
      if (result.success || result.isPrivateProfile) {
        logger.info('✅ Friends endpoint test successful!');
        return true;
      } else if (result.allInCooldown) {
        logger.warn(`⚠️ All connections in cooldown for friends endpoint`);
        return false;
      } else {
        logger.warn(`⚠️ Friends endpoint test failed: ${result.error}`);
        return false;
      }
      
    } catch (error) {
      // Handle 401 errors as success (means API is working, just private profile)
      if (error.response && error.response.status === 401) {
        logger.info('✅ Friends endpoint test successful (401 expected for test account)!');
        return true;
      }
      
      logger.error(`❌ Friends endpoint test failed: ${error.message}`);
      return false;
    }
  }
}

module.exports = SteamValidator;