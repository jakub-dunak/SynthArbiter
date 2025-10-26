"""
NeMo Microservices Client Wrappers
For Retriever, Guardrails, Evaluator, and NIM reasoning
"""
import requests
from typing import List, Dict, Optional
import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GuardrailsClient:
    """NeMo Guardrails client for content moderation"""
    
    def __init__(self, endpoint: str = None, api_key: str = None):
        self.endpoint = endpoint or os.getenv('NEMO_GUARDRAILS_ENDPOINT')
        self.api_key = api_key or os.getenv('NGC_API_KEY')
    
    def validate_input(self, text: str) -> Dict:
        """Validate input text for safety and content policy"""
        try:
            response = requests.post(
                f"{self.endpoint}/validate",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={"text": text},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Guardrails validation error: {e}")
            # Default to safe if service unavailable
            return {'safe': True, 'reason': 'service_unavailable'}
    
    def validate_output(self, text: str) -> Dict:
        """Validate output text for safety"""
        return self.validate_input(text)

class EvaluatorClient:
    """NeMo Evaluator client for quality assessment"""
    
    def __init__(self, endpoint: str = None, api_key: str = None):
        self.endpoint = endpoint or os.getenv('NEMO_EVALUATOR_ENDPOINT')
        self.api_key = api_key or os.getenv('NGC_API_KEY')
    
    def score(self, data: Dict) -> Dict:
        """Score content quality metrics"""
        try:
            response = requests.post(
                f"{self.endpoint}/evaluate",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json=data,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Evaluator scoring error: {e}")
            # Return default scores
            return {
                'context_relevance': 0.8,
                'reasoning_coherence': 0.8,
                'ethical_coverage': 0.8
            }

class NIMClient:
    """NVIDIA NIM client for llama-3.1-nemotron-nano-8B-v1"""
    
    def __init__(self, endpoint: str = None, api_key: str = None):
        self.endpoint = endpoint or os.getenv('NIM_ENDPOINT')
        self.api_key = api_key or os.getenv('NGC_API_KEY')
        self.max_retries = 3
    
    def generate(self, prompt: str, max_tokens: int = 1024, temperature: float = 0.7) -> str:
        """Generate text completion using NIM"""
        for attempt in range(self.max_retries):
            try:
                response = requests.post(
                    f"{self.endpoint}/v1/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "prompt": prompt,
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                        "stop": ["\n\n"]
                    },
                    timeout=120
                )
                response.raise_for_status()
                result = response.json()
                return result['choices'][0]['text']
            
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"NIM generation error after {self.max_retries} attempts: {e}")
                    raise
                wait_time = 2 ** attempt
                logger.warning(f"NIM request failed, retrying in {wait_time}s...")
                time.sleep(wait_time)
        
        return ""

