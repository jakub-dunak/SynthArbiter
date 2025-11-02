// SynthArbiter Configuration
window.SYNTHARBITER_CONFIG = {
    // API Gateway URL - will be replaced during deployment
    API_BASE_URL: 'https://your-api-gateway-url.execute-api.us-east-1.amazonaws.com/prod',

    // Cognito Configuration - will be replaced during deployment
    COGNITO_CONFIG: {
        region: 'us-east-1',
        userPoolId: 'your-user-pool-id',
        userPoolWebClientId: 'your-user-pool-client-id'
    }
};

// Make config available for Amplify
window.COGNITO_CONFIG = window.SYNTHARBITER_CONFIG.COGNITO_CONFIG;
