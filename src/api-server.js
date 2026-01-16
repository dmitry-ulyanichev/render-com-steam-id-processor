// steam-id-processor/src/api-server.js
const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const CONFIG = require('../config/config');
const QueueManager = require('./queue-manager');
const logger = require('./utils/logger');

class ApiServer {
  constructor() {
    this.app = express();
    this.server = null;
    this.queueManager = null;
    this.port = CONFIG.API_PORT || 3002;
    this.setupMiddleware();
    this.setupRoutes();
  }

  setupMiddleware() {
    // Trust proxy headers
    this.app.set('trust proxy', true);
    
    // Parse JSON with size limit
    this.app.use(express.json({ 
      limit: '10mb',
      strict: true,
      type: 'application/json'
    }));
    
    // Security headers
    this.app.use((req, res, next) => {
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('X-Frame-Options', 'DENY');
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
      
      if (req.method === 'OPTIONS') {
        return res.status(200).end();
      }
      
      next();
    });

    // Request logging
    this.app.use((req, res, next) => {
      const clientIP = req.headers['x-real-ip'] || 
                       req.headers['x-forwarded-for'] || 
                       req.connection.remoteAddress;
      
      logger.info(`API: ${req.method} ${req.path} from ${clientIP}`);
      next();
    });
  }

  setupRoutes() {
    // Initialize queue manager
    this.queueManager = new QueueManager(CONFIG);

    // Health check endpoint
    this.app.get('/health', (req, res) => {
      res.json({
        status: 'ok',
        service: 'steam-id-processor-api',
        timestamp: new Date().toISOString(),
        uptime: process.uptime()
      });
    });

    // Cooldowns status endpoint - REAL IMPLEMENTATION
    this.app.get('/health/cooldowns', async (req, res) => {
      try {
        const cooldownData = await this.getCooldownStatus();
        
        res.json({
          status: 'ok',
          service: 'steam-id-processor',
          cooldowns: cooldownData.cooldowns,
          summary: cooldownData.summary,
          overall_status: cooldownData.overallStatus,
          timestamp: new Date().toISOString()
        });
        
      } catch (error) {
        logger.error(`Error getting cooldown status: ${error.message}`);
        res.status(500).json({
          status: 'error',
          error: 'Failed to get cooldown status',
          details: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Add profiles to queue
    this.app.post('/profiles', async (req, res) => {
      try {
        const profiles = Array.isArray(req.body) ? req.body : [req.body];
        const results = [];

        for (const profile of profiles) {
          // Validate profile structure
          if (!profile.steam_id || !profile.username) {
            results.push({
              success: false,
              error: 'Missing required fields: steam_id and username',
              profile: profile
            });
            continue;
          }

          // Add to queue using existing queue manager
          const result = await this.queueManager.addProfileToQueue(
            profile.steam_id, 
            profile.username
          );

          if (result) {
            results.push({
              success: true,
              steam_id: profile.steam_id,
              username: profile.username,
              added: true
            });
            logger.info(`API: Added profile ${profile.steam_id} (${profile.username}) to queue`);
          } else {
            results.push({
              success: true,
              steam_id: profile.steam_id,
              username: profile.username,
              added: false,
              message: 'Profile already exists in queue'
            });
          }
        }

        const successCount = results.filter(r => r.success).length;
        
        res.json({
          success: true,
          message: `Processed ${profiles.length} profiles, ${successCount} successful`,
          results: results,
          timestamp: new Date().toISOString()
        });

      } catch (error) {
        logger.error(`Error adding profiles to queue: ${error.message}`);
        res.status(500).json({
          success: false,
          error: 'Failed to add profiles to queue',
          details: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Get queue contents
    this.app.get('/profiles/queue', async (req, res) => {
      try {
        const profiles = await this.queueManager.getQueuedProfiles();
        const stats = await this.queueManager.getQueueStats();

        res.json({
          success: true,
          queue: {
            profiles: profiles,
            stats: stats
          },
          timestamp: new Date().toISOString()
        });

      } catch (error) {
        logger.error(`Error getting queue contents: ${error.message}`);
        res.status(500).json({
          success: false,
          error: 'Failed to get queue contents',
          details: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // 404 handler
    this.app.use('*', (req, res) => {
      res.status(404).json({
        success: false,
        error: 'Endpoint not found',
        available_endpoints: [
          'GET /health',
          'GET /health/cooldowns', 
          'POST /profiles',
          'GET /profiles/queue'
        ],
        timestamp: new Date().toISOString()
      });
    });
  }

  async getCooldownStatus() {
    try {
      // Read endpoint cooldowns file
      const cooldownPath = path.join(__dirname, '../endpoint_cooldowns.json');
      const configPath = path.join(__dirname, '../config_proxies.json');
      
      let cooldownData = { connections: [] };
      let configData = { connections: [] };
      
      // Read cooldown file if exists
      if (await fs.pathExists(cooldownPath)) {
        cooldownData = await fs.readJson(cooldownPath);
      }
      
      // Read config file if exists  
      if (await fs.pathExists(configPath)) {
        configData = await fs.readJson(configPath);
      }
      
      const now = Date.now();
      const cooldowns = {};
      const summary = {
        totalConnections: configData.connections.length,
        availableConnections: 0,
        endpointsInCooldown: [],
        shortCooldowns: [], // < 30 minutes
        longCooldowns: []   // >= 30 minutes
      };
      
      // Process each connection
      for (let i = 0; i < configData.connections.length; i++) {
        const configConn = configData.connections[i];
        const cooldownConn = cooldownData.connections.find(c => c.index === i) || { endpoint_cooldowns: {} };
        
        const connectionKey = configConn.type === 'direct' ? 'direct' : `socks5_${i}`;
        cooldowns[connectionKey] = {
          type: configConn.type,
          url: configConn.url || null,
          endpoints: {}
        };
        
        let hasActiveCooldowns = false;
        
        // Check each endpoint for this connection
        const commonEndpoints = ['inventory', 'friends', 'steam_level', 'animated_avatar', 'avatar_frame', 'mini_profile_background', 'profile_background'];
        
        for (const endpoint of commonEndpoints) {
          const cooldown = cooldownConn.endpoint_cooldowns[endpoint];
          
          if (cooldown && cooldown.cooldown_until > now) {
            // Active cooldown
            const remainingMs = cooldown.cooldown_until - now;
            const remainingMinutes = Math.ceil(remainingMs / 60000);
            
            cooldowns[connectionKey].endpoints[endpoint] = {
              inCooldown: true,
              remainingMs: remainingMs,
              remainingMinutes: remainingMinutes,
              reason: cooldown.reason,
              backoffLevel: cooldown.backoff_level || null,
              until: new Date(cooldown.cooldown_until).toISOString()
            };
            
            hasActiveCooldowns = true;
            
            // Categorize cooldown duration
            const cooldownInfo = {
              endpoint: endpoint,
              connection: connectionKey,
              remainingMinutes: remainingMinutes,
              reason: cooldown.reason
            };
            
            if (remainingMinutes >= 30) {
              summary.longCooldowns.push(cooldownInfo);
            } else {
              summary.shortCooldowns.push(cooldownInfo);
            }
            
            // Track unique endpoints in cooldown
            if (!summary.endpointsInCooldown.includes(endpoint)) {
              summary.endpointsInCooldown.push(endpoint);
            }
            
          } else {
            // No active cooldown
            cooldowns[connectionKey].endpoints[endpoint] = {
              inCooldown: false,
              remainingMs: 0,
              remainingMinutes: 0
            };
          }
        }
        
        if (!hasActiveCooldowns) {
          summary.availableConnections++;
        }
      }
      
      // Determine overall status
      let overallStatus = 'healthy';
      if (summary.longCooldowns.length > 0) {
        overallStatus = 'degraded'; // Long cooldowns present
      } else if (summary.shortCooldowns.length > 0) {
        overallStatus = 'limited'; // Only short cooldowns
      }
      
      return {
        cooldowns: cooldowns,
        summary: summary,
        overallStatus: overallStatus
      };
      
    } catch (error) {
      logger.error(`Error reading cooldown files: ${error.message}`);
      throw error;
    }
  }

  async start() {
    return new Promise((resolve, reject) => {
      this.server = this.app.listen(this.port, '0.0.0.0', () => {
        logger.info(`🚀 Steam ID Processor API Server started on port ${this.port}`);
        logger.info(`📡 Available endpoints:`);
        logger.info(`   GET  http://localhost:${this.port}/health`);
        logger.info(`   GET  http://localhost:${this.port}/health/cooldowns`);
        logger.info(`   POST http://localhost:${this.port}/profiles`);
        logger.info(`   GET  http://localhost:${this.port}/profiles/queue`);
        resolve();
      });

      this.server.on('error', (error) => {
        logger.error(`API Server error: ${error.message}`);
        reject(error);
      });
    });
  }

  async stop() {
    if (this.server) {
      return new Promise((resolve) => {
        this.server.close(() => {
          logger.info('API Server stopped');
          resolve();
        });
      });
    }
  }
}

module.exports = ApiServer;