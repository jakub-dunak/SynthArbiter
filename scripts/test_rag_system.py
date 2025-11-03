"""
Test script for RAG system components
Tests embedding generation, vector storage, and retrieval
"""
import json
import logging
import boto3
import os
from typing import List, Dict

# Import our services
from services.opensearch_client import VectorStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize clients
sagemaker = boto3.client('sagemaker-runtime')

def test_embedding_generation(endpoint_name: str) -> List[float]:
    """Test NIM embedding model"""
    logger.info("Testing embedding generation...")

    test_texts = [
        "Ethical considerations in AI development require careful analysis of potential harms and benefits.",
        "Autonomous systems should prioritize human safety and well-being above all other concerns."
    ]

    try:
        payload = {
            'input': test_texts,
            'model': 'nvidia/nv-embedqa-e5-v5',
            'encoding_format': 'float'
        }

        response = sagemaker.invoke_endpoint(
            EndpointName=endpoint_name,
            ContentType='application/json',
            Body=json.dumps(payload)
        )

        result = json.loads(response['Body'].read().decode('utf-8'))

        if 'data' in result and result['data']:
            embedding = result['data'][0].get('embedding', [])
            logger.info(f"✅ Embedding generated successfully: {len(embedding)} dimensions")
            return embedding
        else:
            logger.error(f"❌ Unexpected embedding response: {result}")
            return []

    except Exception as e:
        logger.error(f"❌ Embedding generation failed: {e}")
        return []

def test_opensearch_connection(opensearch_endpoint: str) -> bool:
    """Test OpenSearch connection"""
    logger.info("Testing OpenSearch connection...")

    try:
        os.environ['OPENSEARCH_ENDPOINT'] = opensearch_endpoint
        vector_store = VectorStore()

        # Test index creation
        vector_store.index_name = 'test-rag-index'
        vector_store.create_index()
        logger.info("✅ OpenSearch connection successful")
        return True

    except Exception as e:
        logger.error(f"❌ OpenSearch connection failed: {e}")
        return False

def test_vector_storage(opensearch_endpoint: str, embedding: List[float]) -> bool:
    """Test storing and retrieving vectors"""
    logger.info("Testing vector storage and retrieval...")

    try:
        os.environ['OPENSEARCH_ENDPOINT'] = opensearch_endpoint
        vector_store = VectorStore()
        vector_store.index_name = 'test-rag-index'

        # Test document insertion
        test_doc = {
            'id': 'test_doc_1',
            'embedding': embedding,
            'text': 'This is a test document for ethical AI analysis.',
            'metadata': {'source': 'test', 'type': 'ethical_principles'}
        }

        vector_store.bulk_insert([test_doc])
        logger.info("✅ Vector storage successful")

        # Test retrieval
        results = vector_store.search_similar(embedding, top_k=1)
        if results and len(results) > 0:
            logger.info("✅ Vector retrieval successful")
            return True
        else:
            logger.error("❌ Vector retrieval failed - no results")
            return False

    except Exception as e:
        logger.error(f"❌ Vector storage/retrieval failed: {e}")
        return False

def main():
    """Run all RAG system tests"""
    import argparse

    parser = argparse.ArgumentParser(description='Test RAG system components')
    parser.add_argument('--embedding-endpoint', required=True,
                       help='SageMaker embedding endpoint name')
    parser.add_argument('--opensearch-endpoint', required=True,
                       help='OpenSearch endpoint URL')

    args = parser.parse_args()

    logger.info("🧪 Starting RAG System Tests")
    logger.info("=" * 50)

    tests_passed = 0
    total_tests = 3

    # Test 1: Embedding generation
    embedding = test_embedding_generation(args.embedding_endpoint)
    if embedding:
        tests_passed += 1
    else:
        logger.error("❌ Embedding test failed - cannot continue")
        return

    # Test 2: OpenSearch connection
    if test_opensearch_connection(args.opensearch_endpoint):
        tests_passed += 1

    # Test 3: Vector storage and retrieval
    if test_vector_storage(args.opensearch_endpoint, embedding):
        tests_passed += 1

    # Summary
    logger.info("=" * 50)
    logger.info(f"Test Results: {tests_passed}/{total_tests} tests passed")

    if tests_passed == total_tests:
        logger.info("🎉 All RAG system tests passed!")
    else:
        logger.error(f"❌ {total_tests - tests_passed} tests failed")

if __name__ == "__main__":
    main()
