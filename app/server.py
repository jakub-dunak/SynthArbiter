"""
SynthArbiter Flask Web Application
Main API server for ethical dilemma analysis
"""
from flask import Flask, render_template, request, jsonify
from agent.reasoning_engine import SynthArbiterAgent
import boto3
import uuid
import time
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False

# Initialize agent
agent = SynthArbiterAgent()

# Initialize DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table_name = os.getenv('DYNAMODB_TABLE', 'SynthArbiterAnalysisHistory')
try:
    table = dynamodb.Table(table_name)
except Exception as e:
    logger.warning(f"Could not connect to DynamoDB: {e}")
    table = None

@app.route('/')
def index():
    """Serve main application page"""
    return render_template('index.html')

@app.route('/api/analyze', methods=['POST'])
def analyze():
    """Analyze ethical scenario"""
    try:
        data = request.get_json()
        scenario = data.get('scenario', '')
        frameworks = data.get('frameworks', ['utilitarian', 'deontological'])
        
        if not scenario:
            return jsonify({'error': 'Scenario is required'}), 400
        
        logger.info(f"Analyzing scenario: {scenario[:100]}...")
        
        # Run agent reasoning
        state = agent.run(scenario, frameworks)
        
        # Generate analysis ID
        analysis_id = str(uuid.uuid4())
        
        # Store in DynamoDB if available
        if table:
            try:
                table.put_item(Item={
                    'analysisId': analysis_id,
                    'timestamp': int(time.time()),
                    'scenario': scenario,
                    'recommendation': state.final_recommendation,
                    'reasoning': state.reasoning_steps,
                    'evaluation': state.evaluation_scores,
                    'frameworks': frameworks
                })
            except Exception as e:
                logger.error(f"DynamoDB storage error: {e}")
        
        # Calculate tradeoffs for visualization
        tradeoffs = {
            'utilitarian_harm': state.evaluation_scores.get('context_relevance', 0) * 5,
            'deontological_duty': state.evaluation_scores.get('ethical_coverage', 0) * 5,
            'rights_violation': -state.evaluation_scores.get('reasoning_coherence', 0) * 3,
            'precedent_risk': (state.evaluation_scores.get('context_relevance', 0) - 0.5) * 4
        }
        
        # Return analysis results
        return jsonify({
            'analysisId': analysis_id,
            'recommendation': state.final_recommendation,
            'reasoning': state.reasoning_steps,
            'outcomes': state.simulated_outcomes,
            'evaluation': state.evaluation_scores,
            'tradeoffs': tradeoffs,
            'context': len(state.retrieved_context),
            'frameworks': frameworks
        })
    
    except Exception as e:
        logger.error(f"Analysis error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history', methods=['GET'])
def history():
    """Get analysis history"""
    try:
        limit = request.args.get('limit', 10, type=int)
        
        if not table:
            return jsonify([])
        
        # Scan table (limited by DynamoDB scan constraints)
        response = table.scan(Limit=limit)
        items = response.get('Items', [])
        
        # Sort by timestamp descending
        items.sort(key=lambda x: x.get('timestamp', 0), reverse=True)
        
        return jsonify(items)
    
    except Exception as e:
        logger.error(f"History retrieval error: {e}")
        return jsonify([])

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'service': 'syntharbiter'
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

