"""
SynthArbiter Analyze Lambda Function
Main API endpoint for ethical scenario analysis
"""
import json
import boto3
import uuid
import time
import logging
import os
import sys
from typing import Dict, List, Optional

# Add the services directory to the path so we can import the clients
sys.path.append('/opt')  # Lambda layer path
try:
    from services.nemo_retriever_client import NeMOretrieverClient
    from services.opensearch_client import VectorStore
except ImportError:
    # Fallback if services not available in layer
    NeMOretrieverClient = None
    VectorStore = None

# Initialize OpenSearch client for vector search
try:
    opensearch_endpoint = os.environ.get('OPENSEARCH_ENDPOINT')
    if opensearch_endpoint:
        vector_store = VectorStore(endpoint=opensearch_endpoint)
    else:
        vector_store = None
        logger.warning("OpenSearch endpoint not configured")
except Exception as e:
    vector_store = None
    logger.error(f"Failed to initialize OpenSearch client: {e}")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
sagemaker = boto3.client('sagemaker-runtime')
dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')

def lambda_handler(event: Dict, context) -> Dict:
    """
    Main Lambda handler for ethical scenario analysis

    Args:
        event: API Gateway event
        context: Lambda context

    Returns:
        API Gateway response
    """
    try:
        # Parse request
        if 'body' not in event:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Missing request body'})
            }

        body = json.loads(event['body'])
        scenario = body.get('scenario', '')
        frameworks = body.get('frameworks', ['utilitarian', 'deontological'])

        if not scenario:
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Scenario is required'})
            }

        logger.info(f"Analyzing scenario: {scenario[:100]}...")

        # Get user ID from Cognito authorizer
        user_id = event.get('requestContext', {}).get('authorizer', {}).get('claims', {}).get('sub', 'anonymous')

        # Execute reasoning pipeline
        state = run_reasoning_pipeline(scenario, frameworks)

        # Generate analysis ID and store result
        analysis_id = str(uuid.uuid4())

        # Store in DynamoDB
        table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'SynthArbiterAnalysisHistory-prod')
        table = dynamodb.Table(table_name)

        table.put_item(Item={
            'analysisId': analysis_id,
            'userId': user_id,
            'timestamp': int(time.time()),
            'scenario': scenario,
            'recommendation': state.final_recommendation,
            'reasoning': state.reasoning_steps,
            'evaluation': state.evaluation_scores,
            'frameworks': frameworks
        })

        # Calculate tradeoffs for visualization
        tradeoffs = calculate_tradeoffs(state.evaluation_scores)

        # Return analysis results
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps({
                'analysisId': analysis_id,
                'recommendation': state.final_recommendation,
                'reasoning': state.reasoning_steps,
                'outcomes': state.simulated_outcomes,
                'evaluation': state.evaluation_scores,
                'tradeoffs': tradeoffs,
                'context': len(state.retrieved_context),
                'frameworks': frameworks
            })
        }

    except Exception as e:
        logger.error(f"Analysis error: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': str(e)})
        }

def run_reasoning_pipeline(scenario: str, frameworks: List[str]) -> Dict:
    """
    Execute the complete reasoning pipeline

    Args:
        scenario: Ethical dilemma scenario
        frameworks: List of ethical frameworks

    Returns:
        Complete analysis state
    """
    state = {
        'scenario': scenario,
        'reasoning_steps': [],
        'simulated_outcomes': [],
        'evaluation_scores': {},
        'final_recommendation': '',
        'retrieved_context': []
    }

    # Step 1: Guardrails validation (placeholder - will be separate Lambda)
    if not validate_scenario(scenario):
        state['final_recommendation'] = "Scenario rejected: content policy violation"
        return state

    # Step 2: Context retrieval (simplified - will use SageMaker embeddings)
    state['retrieved_context'] = retrieve_context(scenario)

    # Step 3: Ethical reasoning with SageMaker
    context_text = "\n\n".join([doc['text'] for doc in state['retrieved_context'][:5]])
    reasoning_response = generate_reasoning(scenario, context_text, frameworks)
    state['reasoning_steps'] = parse_reasoning_steps(reasoning_response)

    # Step 3.5: Validate output safety
    if not validate_output(reasoning_response):
        state['final_recommendation'] = "Analysis output failed safety validation"
        return state

    # Step 4: Outcome simulation
    state['simulated_outcomes'] = simulate_outcomes(scenario, state['reasoning_steps'])

    # Step 5: Quality evaluation
    state['evaluation_scores'] = evaluate_reasoning(state)

    # Step 6: Final recommendation synthesis
    state['final_recommendation'] = synthesize_recommendation(state, frameworks)

    return state

def validate_scenario(scenario: str) -> bool:
    """Validate scenario using guardrails Lambda"""
    try:
        guardrails_function = os.environ.get('GUARDRAILS_FUNCTION_NAME', f'syntharbiter-guardrails-{os.environ.get("ENVIRONMENT", "prod")}')

        response = lambda_client.invoke(
            FunctionName=guardrails_function,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'text': scenario,
                'type': 'input'
            })
        )

        result = json.loads(response['Payload'].read())
        return result.get('safe', True)  # Default to safe if guardrails fails

    except Exception as e:
        logger.error(f"Guardrails validation error: {e}")
        # Fallback to basic validation
        forbidden_terms = ['harm', 'kill', 'destroy', 'illegal']
        return not any(term in scenario.lower() for term in forbidden_terms)

def validate_output(text: str) -> bool:
    """Validate output text using guardrails Lambda"""
    try:
        guardrails_function = os.environ.get('GUARDRAILS_FUNCTION_NAME', f'syntharbiter-guardrails-{os.environ.get("ENVIRONMENT", "prod")}')

        response = lambda_client.invoke(
            FunctionName=guardrails_function,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'text': text,
                'type': 'output'
            })
        )

        result = json.loads(response['Payload'].read())
        return result.get('safe', True)  # Default to safe if guardrails fails

    except Exception as e:
        logger.error(f"Output guardrails validation error: {e}")
        # Fallback to basic validation
        forbidden_terms = ['harm', 'kill', 'destroy', 'illegal']
        return not any(term in text.lower() for term in forbidden_terms)

def retrieve_context(scenario: str) -> List[Dict]:
    """Retrieve relevant context using embedding model and vector search"""
    try:
        if not vector_store:
            logger.warning("Vector store not available, using fallback context")
            return get_fallback_context()

        # Generate embedding for the scenario
        embedding = generate_embedding(scenario)
        if not embedding:
            logger.warning("Failed to generate embedding, using fallback context")
            return get_fallback_context()

        # Search for similar vectors in OpenSearch
        results = vector_store.search_similar(
            query_embedding=embedding,
            top_k=5  # Return top 5 similar documents
        )

        # Format results for the reasoning pipeline
        context_docs = []
        for result in results:
            context_docs.append({
                'text': result.get('_source', {}).get('text', ''),
                'score': result.get('_score', 0.0),
                'metadata': result.get('_source', {}).get('metadata', {})
            })

        logger.info(f"Retrieved {len(context_docs)} context documents")
        return context_docs

    except Exception as e:
        logger.error(f"Context retrieval error: {e}")
        return get_fallback_context()

def generate_embedding(text: str) -> Optional[List[float]]:
    """Generate embedding vector using NIM embedding model"""
    try:
        embedding_endpoint = os.environ.get('EMBEDDING_ENDPOINT_NAME')
        if not embedding_endpoint:
            logger.error("Embedding endpoint not configured")
            return None

        # Prepare the request for NIM embedding model
        payload = {
            'input': [text],
            'model': 'nvidia/nv-embedqa-e5-v5',
            'encoding_format': 'float'
        }

        response = sagemaker.invoke_endpoint(
            EndpointName=embedding_endpoint,
            ContentType='application/json',
            Body=json.dumps(payload)
        )

        result = json.loads(response['Body'].read().decode('utf-8'))

        # NIM embedding models typically return embeddings in 'data' field
        if 'data' in result and result['data']:
            embedding = result['data'][0].get('embedding', [])
            logger.info(f"Generated embedding with {len(embedding)} dimensions")
            return embedding
        else:
            logger.error(f"Unexpected embedding response format: {result}")
            return None

    except Exception as e:
        logger.error(f"Embedding generation error: {e}")
        return None

def get_fallback_context() -> List[Dict]:
    """Provide fallback context when vector search is unavailable"""
    return [
        {
            'text': 'Ethical considerations in AI development require balancing innovation with responsible deployment. Key principles include transparency, accountability, and human oversight.',
            'score': 0.8,
            'metadata': {'source': 'fallback', 'type': 'ethical_principles'}
        },
        {
            'text': 'Research involving neural technologies should consider participant autonomy, informed consent, and potential long-term societal impacts.',
            'score': 0.7,
            'metadata': {'source': 'fallback', 'type': 'research_ethics'}
        },
        {
            'text': 'Utilitarian ethics focuses on maximizing overall happiness and minimizing harm, while deontological ethics emphasizes duty and moral rules.',
            'score': 0.6,
            'metadata': {'source': 'fallback', 'type': 'ethical_frameworks'}
        }
    ]

def generate_reasoning(scenario: str, context: str, frameworks: List[str]) -> str:
    """Generate ethical reasoning using NIM microservice"""
    endpoint_name = os.environ.get('NIM_ENDPOINT_NAME', 'syntharbiter-nim-reasoning-prod')

    frameworks_str = ", ".join(frameworks)
    messages = [
        {
            "role": "system",
            "content": "You are an AI ethicist analyzing ethical scenarios using structured ethical frameworks."
        },
        {
            "role": "user",
            "content": f"""Analyze the following scenario using these ethical frameworks: {frameworks_str}

Relevant context from academic literature:
{context}

Scenario:
{scenario}

Provide a structured ethical analysis with:
1. Stakeholder identification
2. Moral trade-offs under each framework
3. Potential consequences
4. Synthesized recommendation

Analysis:"""
        }
    ]

    try:
        # Get SageMaker runtime client for endpoint invocation
        sagemaker_runtime = boto3.client('sagemaker-runtime')

        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType='application/json',
            Body=json.dumps({
                'messages': messages,
                'max_tokens': 1024,
                'temperature': 0.7,
                'top_p': 0.9,
                'stream': False
            })
        )

        result = json.loads(response['Body'].read().decode('utf-8'))

        # NIM returns OpenAI-compatible format
        if 'choices' in result and result['choices']:
            return result['choices'][0]['message']['content']
        else:
            logger.error(f"Unexpected NIM response format: {result}")
            return "Reasoning generation failed - unexpected response format"

    except Exception as e:
        logger.error(f"NIM reasoning error: {e}")
        return "Reasoning generation failed"

def parse_reasoning_steps(response: str) -> List[str]:
    """Parse LLM response into structured steps"""
    steps = []
    lines = response.split('\n')
    current_step = ""

    for line in lines:
        line = line.strip()
        if line and (line.startswith('1.') or line.startswith('-') or line.startswith('*')):
            if current_step:
                steps.append(current_step)
            current_step = line.lstrip('123456789.-* ').strip()
        elif line and current_step:
            current_step += " " + line

    if current_step:
        steps.append(current_step)

    return steps[:5] if steps else [response[:500] + "..."]

def simulate_outcomes(scenario: str, reasoning_steps: List[str]) -> List[Dict]:
    """Generate counterfactual outcomes"""
    actions = ['grant_rights', 'deny_rights', 'conditional_rights']
    outcomes = []

    for action in actions:
        try:
            prompt = f"""Given scenario: {scenario}
And reasoning: {reasoning_steps[0] if reasoning_steps else "General analysis"}
If we {action.replace('_', ' ')}, what are the likely consequences?
Provide a brief analysis."""

            consequence_text = generate_reasoning(scenario, "", [])
            outcomes.append({
                'action': action,
                'consequences': consequence_text[:200] + "..."
            })
        except Exception as e:
            logger.error(f"Simulation error for {action}: {e}")
            outcomes.append({
                'action': action,
                'consequences': "Simulation unavailable"
            })

    return outcomes

def evaluate_reasoning(state: Dict) -> Dict:
    """Evaluate reasoning quality using evaluator Lambda"""
    try:
        evaluator_function = os.environ.get('EVALUATOR_FUNCTION_NAME', f'syntharbiter-evaluate-{os.environ.get("ENVIRONMENT", "prod")}')

        evaluation_data = {
            'scenario': state.get('scenario', ''),
            'reasoning': ' '.join(state.get('reasoning_steps', [])),
            'context': ' '.join([doc.get('text', '') for doc in state.get('retrieved_context', [])[:3]]),
            'frameworks': state.get('frameworks', [])
        }

        response = lambda_client.invoke(
            FunctionName=evaluator_function,
            InvocationType='RequestResponse',
            Payload=json.dumps(evaluation_data)
        )

        result = json.loads(response['Payload'].read())
        scores = result.get('body', '{}')
        if isinstance(scores, str):
            scores = json.loads(scores)

        return {
            'context_relevance': scores.get('context_relevance', 0.8),
            'reasoning_coherence': scores.get('reasoning_coherence', 0.8),
            'ethical_coverage': scores.get('ethical_coverage', 0.8),
            'overall_quality': scores.get('overall_quality', 0.8)
        }

    except Exception as e:
        logger.error(f"Evaluator Lambda error: {e}")
        # Fallback scoring
        return {
            'context_relevance': len(state.get('retrieved_context', [])) * 0.1,
            'reasoning_coherence': len(state.get('reasoning_steps', [])) * 0.2,
            'ethical_coverage': 0.8,
            'overall_quality': 0.8
        }

def synthesize_recommendation(state: Dict, frameworks: List[str]) -> str:
    """Synthesize final recommendation"""
    reasoning_steps = state.get('reasoning_steps', [])
    if not reasoning_steps:
        return "Unable to generate recommendation due to analysis failure."

    if len(reasoning_steps) > 0:
        recommendation = reasoning_steps[0][:300]
        if len(reasoning_steps) > 1:
            recommendation += " " + reasoning_steps[1][:200]
    else:
        recommendation = "Analysis completed. Review the detailed reasoning steps for specific recommendations."

    return recommendation

def calculate_tradeoffs(evaluation_scores: Dict) -> Dict:
    """Calculate tradeoffs for visualization"""
    return {
        'utilitarian_harm': evaluation_scores.get('context_relevance', 0) * 5,
        'deontological_duty': evaluation_scores.get('ethical_coverage', 0) * 5,
        'rights_violation': -evaluation_scores.get('reasoning_coherence', 0) * 3,
        'precedent_risk': (evaluation_scores.get('context_relevance', 0) - 0.5) * 4
    }
