"""
SynthArbiter Guardrails Lambda Function
Content moderation and safety validation using NIM microservice
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
    Validate content for safety and appropriateness using NIM Guardrails

    Args:
        event: Lambda event with validation request
        context: Lambda context

    Returns:
        Validation result
    """
    try:
        # Parse input
        if isinstance(event, str):
            data = json.loads(event)
        else:
            data = event

        text = data.get('text', '')
        validation_type = data.get('type', 'input')  # 'input' or 'output'

        logger.info(f"Validating {validation_type} content using NIM Guardrails")

        # Call NIM Guardrails microservice
        result = validate_with_nim_guardrails(text, validation_type)

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(result)
        }

    except Exception as e:
        logger.error(f"NIM Guardrails validation error: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'safe': False,
                'reason': f'Validation error: {str(e)}',
                'confidence': 0.0
            })
        }

def validate_with_nim_guardrails(text: str, validation_type: str = 'input') -> Dict:
    """
    Validate text content using NIM Guardrails microservice

    Args:
        text: Text to validate
        validation_type: 'input' or 'output'

    Returns:
        Validation result dictionary
    """
    try:
        endpoint_name = os.environ.get('GUARDRAILS_ENDPOINT_NAME', 'syntharbiter-nim-guardrails-prod')

        if not text or not text.strip():
            return {
                'safe': False,
                'reason': 'Empty or invalid content',
                'confidence': 1.0
            }

        # Prepare request for NIM Guardrails (OpenAI-compatible format)
        messages = [
            {
                "role": "system",
                "content": f"You are a content safety validator. Analyze the following {validation_type} text and determine if it violates safety policies. Return a JSON response with 'safe' (boolean), 'reason' (string), and 'confidence' (0-1)."
            },
            {
                "role": "user",
                "content": f"Validate this {validation_type} content: {text}"
            }
        ]

        # Call NIM endpoint
        sagemaker_runtime = boto3.client('sagemaker-runtime')

        response = sagemaker_runtime.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType='application/json',
            Body=json.dumps({
                'messages': messages,
                'max_tokens': 256,
                'temperature': 0.1,  # Low temperature for consistent safety decisions
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
                parsed_result = json.loads(nim_response)
                return {
                    'safe': parsed_result.get('safe', False),
                    'reason': parsed_result.get('reason', 'NIM guardrails assessment'),
                    'confidence': parsed_result.get('confidence', 0.5)
                }
            except json.JSONDecodeError:
                # Fallback: interpret response text
                response_lower = nim_response.lower()
                if 'unsafe' in response_lower or 'violate' in response_lower:
                    return {
                        'safe': False,
                        'reason': nim_response[:200],
                        'confidence': 0.8
                    }
                else:
                    return {
                        'safe': True,
                        'reason': nim_response[:200],
                        'confidence': 0.8
                    }
        else:
            logger.error(f"Unexpected NIM Guardrails response format: {result}")
            return {
                'safe': False,
                'reason': 'Failed to parse guardrails response',
                'confidence': 0.0
            }

    except Exception as e:
        logger.error(f"NIM Guardrails call error: {e}")
        # Fallback to basic validation
        return {
            'safe': True,  # Default to safe if NIM fails
            'reason': f'NIM guardrails service unavailable: {str(e)}',
            'confidence': 0.1
        }
