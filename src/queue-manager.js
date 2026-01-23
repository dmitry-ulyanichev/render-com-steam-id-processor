// steam-id-processor/src/queue-manager.js
const fs = require('fs-extra');
const path = require('path');
const logger = require('./utils/logger');

class QueueManager {
  constructor(config, redisQueueClient = null, steamValidator = null) {
    this.config = config;
    this.queuePath = path.join(__dirname, '../profiles_queue.json');

    // Redis queue client (optional - for pulling from shared queue)
    this.redisQueueClient = redisQueueClient;

    // SteamValidator reference (for health checks before claiming from Redis)
    this.steamValidator = steamValidator;

    if (redisQueueClient) {
      logger.info('Queue Manager: Redis queue client enabled - will pull from shared validator queue');
      if (steamValidator) {
        logger.info('Queue Manager: Health checks enabled - will only claim from Redis when healthy');
      } else {
        logger.warn('Queue Manager: No steamValidator provided - health checks disabled!');
      }
    } else {
      logger.info('Queue Manager: Using file-based queue only (legacy mode)');
    }

    // Ensure queue file exists
    this.ensureQueueFileExists();
  }

  ensureQueueFileExists() {
    if (!fs.existsSync(this.queuePath)) {
      fs.ensureDirSync(path.dirname(this.queuePath));
      fs.writeFileSync(this.queuePath, '[]', 'utf8');
      logger.info(`Created empty queue file at: ${this.queuePath}`);
    }
  }

  async readQueueProfiles() {
    try {
      const data = await fs.readFile(this.queuePath, 'utf8');
      const parsed = JSON.parse(data);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      logger.error(`Error reading queue file: ${error.message}`);
      return [];
    }
  }

  async writeQueueProfiles(profiles) {
    try {
      if (!Array.isArray(profiles)) {
        throw new Error(`Invalid profiles data: expected array, got ${typeof profiles}`);
      }

      const jsonData = JSON.stringify(profiles, null, 2);
      await fs.writeFile(this.queuePath, jsonData, 'utf8');
      return true;
    } catch (error) {
      logger.error(`Error writing queue file: ${error.message}`);
      return false;
    }
  }

  async getQueuedProfiles() {
    return await this.readQueueProfiles();
  }

  async saveQueuedProfiles(profiles) {
    return await this.writeQueueProfiles(profiles);
  }

  async addProfileToQueue(steamId, username, apiService = null) {
    try {
      const profiles = await this.readQueueProfiles();

      // Check if already in queue
      const existing = profiles.find(p => p.steam_id === steamId);
      if (existing) {
        logger.info(`Profile ${steamId} (user: ${username}) already in queue`);
        return existing;
      }

      // Check if ID already exists in database (if apiService provided)
      if (apiService) {
        const existsCheckResult = await apiService.checkSteamIdExists(steamId);

        if (existsCheckResult.success && existsCheckResult.exists) {
          logger.info(`Steam ID ${steamId} (user: ${username}) already exists in database, not adding to queue`);
          return null;
        }

        if (!existsCheckResult.success) {
          logger.warn(`Failed to check if ID ${steamId} (user: ${username}) exists: ${existsCheckResult.error}. Adding to queue anyway.`);
        }
      }

      // Validate and fix username
      if (!username || typeof username !== 'string' || username.trim() === '') {
        logger.warn(`Empty/invalid username for Steam ID ${steamId}, using fallback 'Professor'`);
        username = 'Professor';
      }

      // Create new profile object
      const profile = {
        steam_id: steamId,
        username: username,
        timestamp: Date.now(),
        checks: {
          animated_avatar: "to_check",
          avatar_frame: "to_check",
          mini_profile_background: "to_check",
          profile_background: "to_check",
          steam_level: "to_check",
          friends: "to_check",
          csgo_inventory: "to_check"
        }
      };

      // Add to queue
      profiles.push(profile);
      const saveSuccess = await this.writeQueueProfiles(profiles);

      if (!saveSuccess) {
        throw new Error('Failed to save updated queue');
      }

      logger.info(`Added profile ${steamId} (user: ${username}) to queue`);
      return profile;
    } catch (error) {
      logger.error(`Error adding profile to queue: ${error.message}`);
      throw error;
    }
  }

  async updateProfileCheck(steamId, checkName, status) {
    try {
      const profiles = await this.readQueueProfiles();

      // Find the profile
      const profileIndex = profiles.findIndex(p => p.steam_id === steamId);
      if (profileIndex === -1) {
        logger.warn(`Profile ${steamId} not found in queue`);
        return false;
      }

      // Validate status
      const validStatuses = ["to_check", "passed", "failed", "deferred"];
      if (!validStatuses.includes(status)) {
        logger.error(`Invalid status '${status}' for check update. Valid statuses: ${validStatuses.join(', ')}`);
        return false;
      }

      // Update the check status
      profiles[profileIndex].checks[checkName] = status;
      const saveSuccess = await this.writeQueueProfiles(profiles);

      if (!saveSuccess) {
        throw new Error('Failed to save updated profile check');
      }

      const username = profiles[profileIndex].username || 'unknown';
      logger.debug(`Updated ${steamId} (user: ${username}) check '${checkName}' to '${status}'`);
      return true;
    } catch (error) {
      logger.error(`Error updating profile check: ${error.message}`);
      return false;
    }
  }

  async removeProfileFromQueue(steamId) {
    try {
      const profiles = await this.readQueueProfiles();

      // Find the profile to get username for logging
      const profileToRemove = profiles.find(p => p.steam_id === steamId);
      const username = profileToRemove?.username || 'unknown';

      // Filter out the profile
      const filteredProfiles = profiles.filter(p => p.steam_id !== steamId);

      if (filteredProfiles.length < profiles.length) {
        const saveSuccess = await this.writeQueueProfiles(filteredProfiles);

        if (!saveSuccess) {
          throw new Error('Failed to save queue after profile removal');
        }

        logger.info(`Removed profile ${steamId} (user: ${username}) from local queue`);

        // Mark as complete in Redis if Redis client is enabled
        if (this.redisQueueClient) {
          try {
            await this.redisQueueClient.completeItems([steamId]);
            logger.debug(`Marked ${steamId} as complete in Redis validator queue`);
          } catch (redisError) {
            logger.error(`Failed to mark ${steamId} as complete in Redis: ${redisError.message}`);
          }
        }

        return true;
      } else {
        logger.warn(`Profile ${steamId} not found in queue to remove`);
        return false;
      }
    } catch (error) {
      logger.error(`Error removing profile from queue: ${error.message}`);
      return false;
    }
  }

  async processNextQueued() {
    const profiles = await this.getQueuedProfiles();

    if (profiles.length === 0) {
      return null;
    }

    return profiles[0];
  }

  /**
   * Check if instance is healthy enough to claim new work from Redis
   */
  isHealthyToClaimWork() {
    // Check 1: Do we have deferred checks in local queue?
    const profiles = this.getQueuedProfilesSync();
    const hasDeferredChecks = profiles.some(profile =>
      Object.values(profile.checks).some(status => status === "deferred")
    );

    if (hasDeferredChecks) {
      logger.debug('Instance has deferred checks - not claiming new work from Redis');
      return false;
    }

    // Check 2: Do we have at least one healthy endpoint?
    if (!this.steamValidator) {
      logger.debug('No steamValidator available - assuming healthy');
      return true;
    }

    try {
      const connectionStatus = this.steamValidator.getCooldownStatus();
      const hasHealthyEndpoint = Object.values(connectionStatus.endpointSummary).some(
        endpoint => endpoint.availableConnections > 0
      );

      if (!hasHealthyEndpoint) {
        logger.debug('All endpoints on cooldown - not claiming new work from Redis');
        return false;
      }

      logger.debug('Instance is healthy - can claim new work from Redis');
      return true;
    } catch (error) {
      logger.error(`Error checking health status: ${error.message}`);
      return false;
    }
  }

  /**
   * Synchronous version of getQueuedProfiles for health checks
   */
  getQueuedProfilesSync() {
    try {
      const data = fs.readFileSync(this.queuePath, 'utf8');
      const parsed = JSON.parse(data);
      return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
      return [];
    }
  }

  /**
   * Pull new profiles from Redis validator queue and add to local queue
   */
  async pullFromRedisQueue(count = 1) {
    if (!this.redisQueueClient) {
      return 0;
    }

    // Health check: Don't claim if we're unhealthy
    if (!this.isHealthyToClaimWork()) {
      logger.debug('Instance not healthy - skipping Redis claim');
      return 0;
    }

    try {
      const items = await this.redisQueueClient.claimItems(count);

      if (items.length === 0) {
        return 0;
      }

      logger.info(`Pulled ${items.length} profiles from Redis validator queue`);

      // Add each claimed item to local queue
      let addedCount = 0;
      for (const item of items) {
        try {
          const profile = await this.addProfileToQueue(item.id, item.username);
          if (profile) {
            addedCount++;
          } else {
            // Profile already exists or failed to add - release back to Redis
            await this.redisQueueClient.releaseItems([item.id]);
            logger.debug(`Released ${item.id} back to Redis (already exists or failed to add)`);
          }
        } catch (error) {
          logger.error(`Error adding profile ${item.id} from Redis: ${error.message}`);
          await this.redisQueueClient.releaseItems([item.id]);
        }
      }

      logger.info(`Added ${addedCount}/${items.length} profiles from Redis to local queue`);
      return addedCount;
    } catch (error) {
      logger.error(`Error pulling from Redis queue: ${error.message}`);
      return 0;
    }
  }

  async getNextProcessableProfile() {
    const profiles = await this.getQueuedProfiles();

    // If local queue is empty and Redis is enabled, try to pull new profiles
    if (profiles.length === 0 && this.redisQueueClient) {
      logger.debug('Local queue empty, pulling from Redis validator queue...');
      const pulledCount = await this.pullFromRedisQueue(5);
      if (pulledCount > 0) {
        return this.getNextProcessableProfile();
      }
      return null;
    }

    if (profiles.length === 0) {
      return null;
    }

    // Look for a profile with "to_check" checks
    for (const profile of profiles) {
      const hasToCheck = Object.values(profile.checks).some(status => status === "to_check");
      const hasDeferred = Object.values(profile.checks).some(status => status === "deferred");

      if (hasToCheck) {
        return profile;
      }

      // If profile has no "to_check" but no "deferred" either, it's complete
      if (!hasToCheck && !hasDeferred) {
        return profile;
      }
    }

    // If no profiles with "to_check" found, return first profile with deferred checks
    for (const profile of profiles) {
      const hasDeferred = Object.values(profile.checks).some(status => status === "deferred");
      if (hasDeferred) {
        return profile;
      }
    }

    return null;
  }

  async getAllChecksPassed(steamId) {
    const profiles = await this.getQueuedProfiles();
    const profile = profiles.find(p => p.steam_id === steamId);

    if (!profile) {
      logger.warn(`Profile ${steamId} not found in queue when checking status`);
      return false;
    }

    const allPassed = Object.values(profile.checks).every(status => status === "passed");
    return allPassed;
  }

  async convertDeferredChecksToToCheck() {
    try {
      const profiles = await this.readQueueProfiles();
      let conversionsCount = 0;
      let profilesAffected = 0;

      for (const profile of profiles) {
        let profileChanged = false;

        for (const [checkName, status] of Object.entries(profile.checks)) {
          if (status === "deferred") {
            profile.checks[checkName] = "to_check";
            conversionsCount++;
            profileChanged = true;
          }
        }

        if (profileChanged) {
          profilesAffected++;
          const username = profile.username || 'unknown';
          logger.debug(`Converted deferred checks for ${profile.steam_id} (user: ${username})`);
        }
      }

      if (conversionsCount > 0) {
        const saveSuccess = await this.writeQueueProfiles(profiles);

        if (!saveSuccess) {
          throw new Error('Failed to save converted deferred checks');
        }

        logger.info(`Converted ${conversionsCount} deferred checks to 'to_check' across ${profilesAffected} profiles`);
      } else {
        logger.debug('No deferred checks found to convert');
      }

      return {
        conversions: conversionsCount,
        profilesAffected: profilesAffected
      };
    } catch (error) {
      logger.error(`Error converting deferred checks: ${error.message}`);
      return {
        conversions: 0,
        profilesAffected: 0
      };
    }
  }

  async getDeferredCheckStats() {
    const profiles = await this.getQueuedProfiles();
    let totalDeferred = 0;
    let profilesWithDeferred = 0;

    for (const profile of profiles) {
      let profileDeferredCount = 0;

      for (const status of Object.values(profile.checks)) {
        if (status === "deferred") {
          totalDeferred++;
          profileDeferredCount++;
        }
      }

      if (profileDeferredCount > 0) {
        profilesWithDeferred++;
      }
    }

    return {
      totalDeferred,
      profilesWithDeferred,
      totalProfiles: profiles.length
    };
  }

  async getDeferredChecksFromQueue() {
    const profiles = await this.getQueuedProfiles();
    const deferredChecks = [];

    for (const profile of profiles) {
      for (const [checkType, status] of Object.entries(profile.checks)) {
        if (status === "deferred") {
          deferredChecks.push({ steamId: profile.steam_id, checkType });
        }
      }
    }
    return deferredChecks;
  }

  async getAllChecksComplete(steamId) {
    const profiles = await this.getQueuedProfiles();
    const profile = profiles.find(p => p.steam_id === steamId);

    if (!profile) {
      logger.warn(`Profile ${steamId} not found in queue when checking completion status`);
      return { allComplete: false, allPassed: false };
    }

    const allComplete = Object.values(profile.checks).every(status =>
      status === "passed" || status === "failed"
    );

    const allPassed = Object.values(profile.checks).every(status => status === "passed");

    return {
      allComplete,
      allPassed
    };
  }

  async getQueueStats() {
    const profiles = await this.getQueuedProfiles();

    const stats = {
      totalProfiles: profiles.length,
      byUsername: {},
      byStatus: {
        to_check: 0,
        passed: 0,
        failed: 0,
        deferred: 0
      }
    };

    for (const profile of profiles) {
      const username = profile.username || 'unknown';

      // Count by username
      if (!stats.byUsername[username]) {
        stats.byUsername[username] = 0;
      }
      stats.byUsername[username]++;

      // Count check statuses
      for (const status of Object.values(profile.checks)) {
        if (stats.byStatus[status] !== undefined) {
          stats.byStatus[status]++;
        }
      }
    }

    return stats;
  }

  async getProfileBySteamId(steamId) {
    const profiles = await this.getQueuedProfiles();
    return profiles.find(p => p.steam_id === steamId) || null;
  }

  delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async updateProfileCheckExternal(steamId, checkName, status, source = 'external') {
    try {
      logger.info(`External update from ${source}: ${steamId} check '${checkName}' -> '${status}'`);
      const result = await this.updateProfileCheck(steamId, checkName, status);

      if (result) {
        logger.info(`Successfully applied external update from ${source} for ${steamId}`);
      } else {
        logger.warn(`Failed to apply external update from ${source} for ${steamId}`);
      }

      return result;
    } catch (error) {
      logger.error(`Error in external update from ${source}: ${error.message}`);
      return false;
    }
  }
}

module.exports = QueueManager;
