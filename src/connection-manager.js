// steam-id-processor/src/connection-manager.js
const fs = require('fs-extra');
const path = require('path');
const axios = require('axios');
const { SocksProxyAgent } = require('socks-proxy-agent');
const logger = require('./utils/logger');

class ConnectionManager {
  constructor(cooldownManager) {
    this.cooldownManager = cooldownManager;
    this.connections = [{ id: 'direct', type: 'direct' }];
    this.roundRobinIndex = 0;

    this.loadProxies();
  }

  loadProxies() {
    const configPath = process.env.PROXY_CONFIG_PATH || null;
    const candidates = [];

    if (configPath) {
      candidates.push(configPath);
    }
    candidates.push(path.resolve('./proxy.json'));
    candidates.push('/etc/secrets/proxy.json');

    for (const filePath of candidates) {
      try {
        if (fs.existsSync(filePath)) {
          const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
          if (data.proxies && Array.isArray(data.proxies) && data.proxies.length > 0) {
            data.proxies.forEach((proxy, i) => {
              this.connections.push({
                id: `proxy-${i}`,
                type: 'socks5',
                config: {
                  host: proxy.host,
                  port: proxy.port,
                  username: proxy.username,
                  password: proxy.password
                }
              });
            });
            logger.info(`Loaded ${data.proxies.length} SOCKS5 proxies from ${filePath}`);
            logger.info(`Total connections: ${this.connections.length} (1 direct + ${data.proxies.length} proxies)`);
            return;
          }
        }
      } catch (error) {
        logger.warn(`Failed to load proxies from ${filePath}: ${error.message}`);
      }
    }

    logger.info('No proxy configuration found - using direct connection only');
  }

  _compoundKey(connectionId) {
    return `inventory:${connectionId}`;
  }

  _isConnectionAvailable(connectionId) {
    return this.cooldownManager.isEndpointAvailable(this._compoundKey(connectionId));
  }

  _markConnectionCooldown(connectionId, errorType, errorMessage) {
    this.cooldownManager.markEndpointCooldown(this._compoundKey(connectionId), errorType, errorMessage);
  }

  _resetConnectionBackoff(connectionId) {
    this.cooldownManager.resetBackoffOnSuccess(this._compoundKey(connectionId));
  }

  _getConnectionCooldownRemaining(connectionId) {
    return this.cooldownManager.getNextAvailableTimeForEndpoint(this._compoundKey(connectionId));
  }

  _buildOrderedConnections() {
    const ordered = [];

    // Direct first if available
    const direct = this.connections.find(c => c.type === 'direct');
    if (direct && this._isConnectionAvailable(direct.id)) {
      ordered.push(direct);
    }

    // Proxies in round-robin order, skipping those in cooldown
    const proxies = this.connections.filter(c => c.type === 'socks5');
    if (proxies.length > 0) {
      for (let i = 0; i < proxies.length; i++) {
        const idx = (this.roundRobinIndex + i) % proxies.length;
        const proxy = proxies[idx];
        if (this._isConnectionAvailable(proxy.id)) {
          ordered.push(proxy);
        }
      }
      // Advance round-robin for next call
      this.roundRobinIndex = (this.roundRobinIndex + 1) % proxies.length;
    }

    return ordered;
  }

  _createAxiosInstance(connection) {
    const config = {
      timeout: 25000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'
      }
    };

    if (connection.type === 'socks5') {
      const { host, port, username, password } = connection.config;
      const proxyUrl = username
        ? `socks5://${username}:${password}@${host}:${port}`
        : `socks5://${host}:${port}`;
      const agent = new SocksProxyAgent(proxyUrl);
      config.httpAgent = agent;
      config.httpsAgent = agent;
    }

    return axios.create(config);
  }

  async makeInventoryRequest(url) {
    const ordered = this._buildOrderedConnections();

    if (ordered.length === 0) {
      const nextAvailableIn = this.getNextAvailableTime();
      logger.warn(`All inventory connections in cooldown, next available in ~${Math.ceil(nextAvailableIn / 60000)}m`);
      return { allInCooldown: true, nextAvailableIn, endpointName: 'inventory' };
    }

    for (const connection of ordered) {
      try {
        logger.debug(`Trying inventory request via ${connection.id}`);
        const instance = this._createAxiosInstance(connection);
        const response = await instance.get(url);

        this._resetConnectionBackoff(connection.id);
        logger.info(`Inventory request succeeded via ${connection.id}`);
        return { success: true, data: response.data, connectionId: connection.id };
      } catch (error) {
        const status = error.response ? error.response.status : null;

        // 403/401 = private inventory, treat as success
        if (status === 403) {
          this._resetConnectionBackoff(connection.id);
          logger.info(`Private inventory detected via ${connection.id} (403)`);
          return { success: false, isPrivateInventory: true, connectionId: connection.id, error: error.message, errorObj: error };
        }
        if (status === 401) {
          this._resetConnectionBackoff(connection.id);
          logger.info(`Unauthorized inventory via ${connection.id} (401)`);
          return { success: false, isPrivateInventory: true, connectionId: connection.id, error: error.message, errorObj: error };
        }

        // 429 = rate limited, mark this connection and try next
        if (status === 429) {
          logger.warn(`Rate limit (429) on inventory via ${connection.id}, trying next connection`);
          this._markConnectionCooldown(connection.id, '429', error.message);
          continue;
        }

        // Connection/network error = mark cooldown, try next
        if (this.cooldownManager.isConnectionError(error)) {
          const errorType = this.cooldownManager.categorizeConnectionError(error);
          logger.warn(`${errorType} on inventory via ${connection.id}: ${error.message}`);
          this._markConnectionCooldown(connection.id, errorType, error.message);
          continue;
        }

        // Other HTTP errors (5xx etc.) = mark cooldown, try next
        logger.warn(`Error on inventory via ${connection.id} (${status || 'unknown'}): ${error.message}`);
        this._markConnectionCooldown(connection.id, 'connection_error', error.message);
        continue;
      }
    }

    // All connections exhausted
    const nextAvailableIn = this.getNextAvailableTime();
    logger.warn(`All inventory connections exhausted, next available in ~${Math.ceil(nextAvailableIn / 60000)}m`);
    return { allInCooldown: true, nextAvailableIn, endpointName: 'inventory' };
  }

  areAllInCooldown() {
    return this.connections.every(c => !this._isConnectionAvailable(c.id));
  }

  getNextAvailableTime() {
    let minRemaining = Infinity;
    for (const conn of this.connections) {
      const remaining = this._getConnectionCooldownRemaining(conn.id);
      if (remaining < minRemaining) {
        minRemaining = remaining;
      }
    }
    return minRemaining === Infinity ? 0 : minRemaining;
  }

  getStatus() {
    const now = Date.now();
    const result = {
      totalConnections: this.connections.length,
      availableConnections: 0,
      connections: []
    };

    for (const conn of this.connections) {
      const key = this._compoundKey(conn.id);
      const available = this._isConnectionAvailable(conn.id);
      if (available) result.availableConnections++;

      const remaining = this._getConnectionCooldownRemaining(conn.id);
      const cooldownData = this.cooldownManager.cooldowns.endpoint_cooldowns[key];

      result.connections.push({
        id: conn.id,
        type: conn.type,
        available,
        cooldownRemainingMs: remaining,
        cooldownRemainingMin: Math.ceil(remaining / 60000),
        reason: cooldownData ? cooldownData.reason : null,
        backoffLevel: cooldownData ? cooldownData.backoff_level : null
      });
    }

    return result;
  }
}

module.exports = ConnectionManager;
