"""
NeMo Retriever Client
Wraps NeMo Retriever API for embedding generation and semantic search
"""
import requests
from typing import List, Dict
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NeMoRetrieverClient:
    def __init__(self, endpoint: str = None, api_key: str = None):
        self.endpoint = endpoint or os.getenv('NEMO_RETRIEVER_ENDPOINT')
        self.api_key = api_key or os.getenv('NGC_API_KEY')
        
        if not self.endpoint:
            raise ValueError("NeMo Retriever endpoint not configured")
        if not self.api_key:
            raise ValueError("NGC API key not provided")
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a batch of texts

        Args:
            texts: List of text strings to embed

        Returns:
            List of embedding vectors (2048-dimensional)
        """
        try:
            response = requests.post(
                f"{self.endpoint}/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "input": texts,
                    "model": "llama-3.2-nv-embedqa-1b-v2"
                },
                timeout=60
            )
            response.raise_for_status()
            
            result = response.json()
            return [item['embedding'] for item in result['data']]
        
        except Exception as e:
            logger.error(f"Error embedding batch: {e}")
            raise
    
    def embed(self, text: str) -> List[float]:
        """
        Generate embedding for a single text

        Args:
            text: Text string to embed

        Returns:
            Embedding vector (2048-dimensional)
        """
        embeddings = self.embed_batch([text])
        return embeddings[0]

