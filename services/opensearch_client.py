"""
AWS OpenSearch Vector Store Client
Handles k-NN search for semantic similarity
"""
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from typing import List, Dict
import boto3
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VectorStore:
    def __init__(self, endpoint: str = None, region: str = 'us-east-1'):
        self.endpoint = endpoint or os.getenv('OPENSEARCH_ENDPOINT')
        self.region = region
        self.index_name = 'ethical-knowledge'
        
        if not self.endpoint:
            raise ValueError("OpenSearch endpoint not configured")
        
        # Get AWS credentials from environment or instance metadata
        session = boto3.Session()
        credentials = session.get_credentials()
        
        if credentials:
            awsauth = AWS4Auth(
                credentials.access_key,
                credentials.secret_key,
                self.region,
                'es',
                session_token=credentials.token
            )
        else:
            # Fallback to unsigned requests if using IAM role
            awsauth = None
        
        self.client = OpenSearch(
            hosts=[{'host': self.endpoint.replace('https://', ''), 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
    
    def create_index(self):
        """Create OpenSearch index with k-NN mapping"""
        index_body = {
            "settings": {
                "index": {
                    "knn": True,
                    "knn.space_type": "cosinesimilarity",
                    "number_of_shards": 3,
                    "number_of_replicas": 1
                }
            },
            "mappings": {
                "properties": {
                    "embedding": {
                        "type": "knn_vector",
                        "dimension": 2048,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimilarity",
                            "engine": "nmslib"
                        }
                    },
                    "text": {
                        "type": "text"
                    },
                    "metadata": {
                        "type": "object"
                    }
                }
            }
        }
        
        try:
            self.client.indices.create(
                index=self.index_name,
                body=index_body,
                ignore=400  # Ignore 400 if index already exists
            )
            logger.info(f"Index {self.index_name} created or already exists")
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            raise
    
    def bulk_insert(self, passages: List[Dict]):
        """
        Insert multiple passages with embeddings into index
        
        Args:
            passages: List of dicts with 'embedding', 'text', 'metadata'
        """
        from opensearchpy import helpers
        
        actions = []
        for i, passage in enumerate(passages):
            action = {
                "_index": self.index_name,
                "_id": passage.get('id', f"doc_{i}"),
                "_source": {
                    "embedding": passage.get('embedding'),
                    "text": passage.get('text'),
                    "metadata": passage.get('metadata', {})
                }
            }
            actions.append(action)
        
        try:
            success, failed = helpers.bulk(self.client, actions, chunk_size=100)
            logger.info(f"Bulk insert: {success} succeeded, {len(failed)} failed")
            return success
        except Exception as e:
            logger.error(f"Error in bulk insert: {e}")
            raise
    
    def search_similar(self, query_embedding: List[float], top_k: int = 10) -> List[Dict]:
        """
        Search for similar passages using k-NN
        
        Args:
            query_embedding: Query embedding vector
            top_k: Number of results to return
            
        Returns:
            List of similar documents
        """
        query = {
            "size": top_k,
            "query": {
                "knn": {
                    "embedding": {
                        "vector": query_embedding,
                        "k": top_k
                    }
                }
            }
        }
        
        try:
            response = self.client.search(
                index=self.index_name,
                body=query
            )
            
            results = []
            for hit in response['hits']['hits']:
                results.append({
                    'id': hit['_id'],
                    'text': hit['_source']['text'],
                    'metadata': hit['_source'].get('metadata', {}),
                    'score': hit['_score']
                })
            
            return results
        
        except Exception as e:
            logger.error(f"Error in similarity search: {e}")
            raise

