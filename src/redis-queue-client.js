// steam-id-processor/src/redis-queue-client.js
const https = require('https');
const http = require('http');
const logger = require('./utils/logger');

/**
 * Redis Queue Client for steam-id-processor
 * Communicates with node_api_service Redis queue endpoints
 */
class RedisQueueClient {
    constructor(config) {
        this.queueApiUrl = config.queueApiUrl || process.env.NODE_API_SERVICE_URL || 'http://127.0.0.1:3001';
        this.apiKey = config.apiKey || process.env.LINK_HARVESTER_API_KEY || 'fa46kPOVnHT2a4aFmQS11dd70290';
        this.queueName = 'validator';
        this.instanceId = config.instanceId; // Unique ID for this instance

        logger.info(`Redis Queue Client initialized - Instance: ${this.instanceId}`);
        logger.info(`Queue API URL: ${this.queueApiUrl}`);
    }

    /**
     * Make HTTP request to queue API
     */
    async makeRequest(method, endpoint, data = null) {
        return new Promise((resolve, reject) => {
            const fullUrl = `${this.queueApiUrl}${endpoint}`;
            const urlObj = new URL(fullUrl);
            const isHttps = urlObj.protocol === 'https:';
            const httpModule = isHttps ? https : http;

            const options = {
                hostname: urlObj.hostname,
                port: urlObj.port || (isHttps ? 443 : 80),
                path: urlObj.pathname,
                method: method,
                headers: {
                    'Content-Type': 'application/json',
                    'X-API-Key': this.apiKey
                },
                timeout: 30000
            };

            if (data) {
                const postData = JSON.stringify(data);
                options.headers['Content-Length'] = Buffer.byteLength(postData);
            }

            const req = httpModule.request(options, (res) => {
                let responseData = '';

                res.on('data', (chunk) => {
                    responseData += chunk;
                });

                res.on('end', () => {
                    try {
                        const parsed = JSON.parse(responseData);
                        if (res.statusCode === 200 && parsed.success) {
                            resolve(parsed);
                        } else {
                            reject(new Error(`Queue API error: ${parsed.error || responseData}`));
                        }
                    } catch (err) {
                        reject(new Error(`Failed to parse queue API response: ${err.message}`));
                    }
                });
            });

            req.on('error', (err) => {
                reject(new Error(`Queue API request failed: ${err.message}`));
            });

            req.on('timeout', () => {
                req.destroy();
                reject(new Error('Queue API request timeout'));
            });

            if (data) {
                req.write(JSON.stringify(data));
            }

            req.end();
        });
    }

    /**
     * Claim items from validator queue
     * @param {number} count - Number of items to claim
     * @returns {Array} Array of claimed items {id, username, data}
     */
    async claimItems(count = 1) {
        try {
            const response = await this.makeRequest('POST', `/queue/${this.queueName}/claim`, {
                instance_id: this.instanceId,
                count: count
            });

            logger.debug(`Claimed ${response.items.length} items from ${this.queueName} queue`);
            return response.items;
        } catch (error) {
            logger.error(`Error claiming from ${this.queueName} queue: ${error.message}`);
            return [];
        }
    }

    /**
     * Mark items as completed
     * @param {Array} itemIds - Array of item IDs to mark complete
     */
    async completeItems(itemIds) {
        try {
            await this.makeRequest('POST', `/queue/${this.queueName}/complete`, {
                instance_id: this.instanceId,
                items: itemIds
            });

            logger.debug(`Completed ${itemIds.length} items in ${this.queueName} queue`);
            return true;
        } catch (error) {
            logger.error(`Error completing items in ${this.queueName} queue: ${error.message}`);
            return false;
        }
    }

    /**
     * Release items back to queue (on error)
     * @param {Array} itemIds - Array of item IDs to release
     */
    async releaseItems(itemIds) {
        try {
            await this.makeRequest('POST', `/queue/${this.queueName}/release`, {
                instance_id: this.instanceId,
                items: itemIds
            });

            logger.debug(`Released ${itemIds.length} items back to ${this.queueName} queue`);
            return true;
        } catch (error) {
            logger.error(`Error releasing items in ${this.queueName} queue: ${error.message}`);
            return false;
        }
    }

    /**
     * Get queue statistics
     */
    async getStats() {
        try {
            const response = await this.makeRequest('GET', `/queue/${this.queueName}/stats`);
            return response.stats;
        } catch (error) {
            logger.error(`Error getting ${this.queueName} queue stats: ${error.message}`);
            return null;
        }
    }

    /**
     * Release all items claimed by this instance (cleanup orphaned claims on startup)
     * @returns {number} Number of items released
     */
    async releaseInstance() {
        try {
            const response = await this.makeRequest('POST', `/queue/${this.queueName}/release-instance`, {
                instance_id: this.instanceId
            });

            return response.released_count || 0;
        } catch (error) {
            logger.error(`Error releasing instance claims: ${error.message}`);
            return 0;
        }
    }
}

module.exports = RedisQueueClient;
