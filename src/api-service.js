// steam-id-processor\src\api-service.js
const axios = require('axios');
const path = require('path');
const logger = require('./utils/logger');

// Load environment variables
require('dotenv').config({ path: path.resolve(__dirname, '../../.env') });

class ApiService {
  constructor(config) {
    this.config = config;
    this.apiEndpoint = config.API_ENDPOINT;
    this.credentials = null;
    this.loadCredentials();
  }

  loadCredentials() {
    // Load credentials from environment variables
    this.credentials = {
      apiKey: process.env.LINK_HARVESTER_API_KEY
    };
    
    if (!this.credentials.apiKey) {
      logger.warn('LINK_HARVESTER_API_KEY not found in environment variables');
      logger.warn('API calls to kuchababok.com will fail without this key');
    } else {
      logger.info('API credentials loaded successfully from environment');
    }
  }
  
  async checkSteamIdExists(steamId) {
    try {
      logger.info(`Checking if Steam ID ${steamId} exists in database`);
      
      const url = `https://kuchababok.online/ru/links/api/check-id-exists/${steamId}/`;
      const response = await axios.get(url, {
        timeout: 5000
      });
      
      if (response.status === 200 && response.data) {
        return {
          success: true,
          exists: response.data.exists,
          steamId: response.data.steam_id
        };
      } else {
        logger.warn(`Unexpected response for existence check of ${steamId}`);
        return {
          success: false,
          error: 'Unexpected API response format',
          data: response.data
        };
      }
    } catch (error) {
      logger.error(`Error checking if ID exists: ${error.message}`);
      return {
        success: false,
        error: error.message
      };
    }
  }

  async sendSteamIdToApi(steamId, username) {
    if (!this.credentials?.apiKey) {
      return {
        success: false,
        error: 'Missing API key (LINK_HARVESTER_API_KEY not set in environment)'
      };
    }

    // Validate username parameter
    if (!username || typeof username !== 'string') {
      return {
        success: false,
        error: 'Invalid or missing username parameter'
      };
    }

    try {
      logger.info(`Sending Steam ID ${steamId} to kuchababok.com API (user: ${username})`);
      
      // Prepare request parameters - use username from parameter (from queue)
      const params = {
        steam_id: steamId,
        username: username,
        api_key: this.credentials.apiKey
      };
      
      // Send the request
      const response = await axios.get(this.apiEndpoint, {
        params,
        timeout: 10000
      });
      
      // Process response
      if (response.status === 200) {
        logger.info(`Successfully added Steam ID ${steamId} to kuchababok.com (user: ${username})`);
        return {
          success: true,
          data: response.data
        };
      } else {
        logger.warn(`Unexpected status code for ${steamId} (user: ${username}): ${response.status}`);
        return {
          success: false,
          error: `Unexpected status code: ${response.status}`,
          data: response.data
        };
      }
    } catch (error) {
      // Handle specific error types
      if (error.response) {
        // Server responded with an error
        const status = error.response.status;
        const errorData = error.response.data || {};
        const errorMessage = errorData.error || error.message;
        
        logger.error(`API error for ${steamId} (user: ${username}): ${status} - ${errorMessage}`);
        
        return {
          success: false,
          status,
          error: errorMessage,
          data: errorData
        };
      } else if (error.request) {
        // Request was made but no response received
        logger.error(`API no response for ${steamId} (user: ${username}): ${error.message}`);
        return {
          success: false,
          error: 'No response from server',
          details: error.message
        };
      } else {
        // Error setting up the request
        logger.error(`API request setup error for ${steamId} (user: ${username}): ${error.message}`);
        return {
          success: false,
          error: 'Request setup error',
          details: error.message
        };
      }
    }
  }

  async handleNewSteamId(steamId, username) {
    const result = {
      steamId,
      username,
      success: false,
      error: null
    };
    
    // Validate username parameter
    if (!username || typeof username !== 'string') {
      result.error = 'Invalid or missing username';
      logger.error(`Invalid username '${username}' for Steam ID ${steamId}`);
      return result;
    }
    
    try {
      // Send to API with username from parameter (from queue)
      const apiResponse = await this.sendSteamIdToApi(steamId, username);
      
      if (apiResponse.success) {
        // Successful API call
        result.success = true;
        logger.info(`Successfully added Steam ID ${steamId} (user: ${username}) to kuchababok.com`);
        return result;
      }
      
      // Handle different error types
      const errorMessage = apiResponse.error || 'Unknown error';
      result.error = errorMessage;
      
      if (errorMessage.includes('Link already exists')) {
        // Link already exists - consider this a success
        logger.info(`Steam ID ${steamId} (user: ${username}) already exists on kuchababok.com`);
        result.success = true;
        return result;
      } else if (errorMessage.includes('Invalid Steam ID format')) {
        // Invalid Steam ID format - don't retry
        logger.warn(`Invalid Steam ID format: ${steamId} (user: ${username})`);
        return result;
      } else {
        // Other errors - log and return failure
        logger.warn(`API call failed for ${steamId} (user: ${username}): ${errorMessage}`);
        return result;
      }
    } catch (error) {
      // Unexpected error
      const errorMessage = `Unexpected error processing ${steamId} (user: ${username}): ${error.message}`;
      logger.error(errorMessage);
      result.error = errorMessage;
      return result;
    }
  }
}

module.exports = ApiService;