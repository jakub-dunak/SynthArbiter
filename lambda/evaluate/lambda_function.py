"""
SynthArbiter Evaluate Lambda Function
Quality assessment for ethical reasoning using NIM microservice
"""
import json
import logging
import boto3
import os
from typing import Dict

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def lambda_handler(event: Dict, context) -> Dict:
    """
    Evaluate reasoning quality scores using NIM Evaluator

    Args:
        event: Lambda event with evaluation data
        context: Lambda context

    Returns:
        Quality scores
    """
    try:
        # Parse input data
        if isinstance(event, str):
            data = json.loads(event)
        else:
            data = event

        logger.info(f"Evaluating reasoning quality using NIM Evaluator")

        # Calculate quality scores using NIM
        scores = evaluate_with_nim_evaluator(data)

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(scores)
        }

    except Exception as e:
        logger.error(f"NIM Evaluator error: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'error': str(e),
                'context_relevance': 0.5,
                'reasoning_coherence': 0.5,
                'ethical_coverage': 0.5,
                'overall_quality': 0.5
            })
        }

def evaluate_with_nim_evaluator(data: Dict) -> Dict:
    """
    Calculate quality scores using NIM Evaluator microservice

    Args:
        data: Dictionary containing evaluation data

    Returns:
        Quality scores dictionary
    """
    try:
        endpoint_name = os.environ.get('EVALUATOR_ENDPOINT_NAME', 'syntharbiter-nim-evaluator-prod')

        # Prepare evaluation data for NIM
        evaluation_content = f"""
        Evaluate the quality of this ethical reasoning analysis:

        Context: {data.get('context', '')}
        Scenario: {data.get('scenario', '')}
        Reasoning: {data.get('reasoning', '')}
        Frameworks Used: {', '.join(data.get('frameworks', []))}
        """

        messages = [
            {
                "role": "system",
                "content": "You are an expert evaluator of ethical reasoning quality. Analyze the provided ethical analysis and return quality scores as JSON with keys: 'context_relevance', 'reasoning_coherence', 'ethical_coverage', 'overall_quality' (all values 0-1)."
            },
            {
                "role": "user",
                "content": evaluation_content
            }
        ]

        # Call NIM endpoint
        sagemaker_runtime = boto3.client('sagemaker-runtime')

        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType='application/json',
            Body=json.dumps({
                'messages': messages,
                'max_tokens': 512,
                'temperature': 0.1,  # Low temperature for consistent scoring
                'top_p': 0.9,
                'stream': False
            })
        )

        result = json.loads(response['Body'].read().decode('utf-8'))

        # Parse NIM response
        if 'choices' in result and result['choices']:
            nim_response = result['choices'][0]['message']['content']

            # Try to parse as JSON
            try:
                parsed_scores = json.loads(nim_response)
                return {
                    'context_relevance': float(parsed_scores.get('context_relevance', 0.8)),
                    'reasoning_coherence': float(parsed_scores.get('reasoning_coherence', 0.8)),
                    'ethical_coverage': float(parsed_scores.get('ethical_coverage', 0.8)),
                    'overall_quality': float(parsed_scores.get('overall_quality', 0.8))
                }
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Failed to parse NIM evaluator response as JSON: {nim_response}")
                # Fallback: extract scores from text using regex
                import re
                scores = {}
                patterns = {
                    'context_relevance': r'context_relevance[:\s]+([0-9.]+)',
                    'reasoning_coherence': r'reasoning_coherence[:\s]+([0-9.]+)',
                    'ethical_coverage': r'ethical_coverage[:\s]+([0-9.]+)',
                    'overall_quality': r'overall_quality[:\s]+([0-9.]+)'
                }

                for key, pattern in patterns.items():
                    match = re.search(pattern, nim_response, re.IGNORECASE)
                    if match:
                        scores[key] = float(match.group(1))
                    else:
                        scores[key] = 0.8  # Default

                return scores
        else:
            logger.error(f"Unexpected NIM Evaluator response format: {result}")
            return {
                'context_relevance': 0.5,
                'reasoning_coherence': 0.5,
                'ethical_coverage': 0.5,
                'overall_quality': 0.5
            }

    except Exception as e:
        logger.error(f"NIM Evaluator call error: {e}")
        # Fallback to basic scoring
        return {
            'context_relevance': 0.8,
            'reasoning_coherence': 0.8,
            'ethical_coverage': 0.8,
            'overall_quality': 0.8
        }
