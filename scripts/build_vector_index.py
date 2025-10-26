"""
Build vector index from preprocessed data
Embeds chunks and stores in OpenSearch
"""
import json
import sys
import logging
import argparse
from pathlib import Path
import boto3
from typing import List, Dict
import time

# Import service clients
from services.nemo_retriever_client import NeMoRetrieverClient
from services.opensearch_client import VectorStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_corpus_from_s3(bucket: str, key: str) -> List[Dict]:
    """Load preprocessed corpus from S3"""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    corpus = []
    
    for line in obj['Body'].iter_lines():
        if line:
            corpus.append(json.loads(line))
    
    logger.info(f"Loaded {len(corpus)} chunks from S3")
    return corpus

def load_corpus_from_local(file_path: str) -> List[Dict]:
    """Load preprocessed corpus from local file"""
    logger.info(f"Loading from {file_path}")
    
    corpus = []
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip():
                corpus.append(json.loads(line))
    
    logger.info(f"Loaded {len(corpus)} chunks from local file")
    return corpus

def main():
    """Main indexing job"""
    parser = argparse.ArgumentParser(description='Build OpenSearch Vector Index')
    parser.add_argument('--source', required=True, 
                       help='Source data (S3 path s3://bucket/key or local file path)')
    parser.add_argument('--index-name', default='ethical-knowledge',
                       help='OpenSearch index name')
    parser.add_argument('--batch-size', type=int, default=50,
                       help='Batch size for embedding generation')
    parser.add_argument('--opensearch-endpoint', 
                       default=None,
                       help='OpenSearch endpoint URL')
    
    args = parser.parse_args()
    
    # Initialize clients
    logger.info("Initializing clients...")
    retriever = NeMoRetrieverClient()
    
    # Initialize vector store
    if args.opensearch_endpoint:
        import os
        os.environ['OPENSEARCH_ENDPOINT'] = args.opensearch_endpoint
    
    vector_store = VectorStore()
    
    # Create index
    logger.info(f"Creating OpenSearch index: {args.index_name}")
    vector_store.index_name = args.index_name
    vector_store.create_index()
    
    # Load corpus (from S3 or local file)
    if args.source.startswith('s3://'):
        bucket, key = args.source.replace('s3://', '').split('/', 1)
        corpus = load_corpus_from_s3(bucket, key)
    else:
        corpus = load_corpus_from_local(args.source)
    
    if not corpus:
        logger.error("No corpus loaded. Exiting.")
        return
    
    # Process in batches
    batch_size = args.batch_size
    total_batches = (len(corpus) + batch_size - 1) // batch_size
    processed = 0
    failed = 0
    
    start_time = time.time()
    
    for i in range(0, len(corpus), batch_size):
        batch = corpus[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} chunks)")
        
        try:
            # Extract text for embedding
            texts = []
            for item in batch:
                text = item.get('text', '')
                if not text:
                    logger.warning(f"Skipping item without text: {item.get('id', 'unknown')}")
                    continue
                texts.append(text)
            
            if not texts:
                logger.warning("No valid texts in batch, skipping")
                continue
            
            # Generate embeddings
            logger.debug(f"Generating embeddings for {len(texts)} texts...")
            embeddings = retriever.embed_batch(texts)
            
            if len(embeddings) != len(texts):
                logger.error(f"Embedding count mismatch: got {len(embeddings)}, expected {len(texts)}")
                failed += len(texts)
                continue
            
            # Prepare passages with embeddings
            passages = []
            for idx, (item, embedding) in enumerate(zip(batch, embeddings)):
                chunk_id = item.get('metadata', {}).get('chunk_id', idx)
                doc_id = item.get('metadata', {}).get('id', f"doc_{processed + idx}")
                
                passage = {
                    'id': f"{doc_id}_chunk_{chunk_id}",
                    'embedding': embedding,
                    'text': item.get('text', ''),
                    'metadata': item.get('metadata', {})
                }
                passages.append(passage)
            
            # Insert into OpenSearch
            if passages:
                success = vector_store.bulk_insert(passages)
                processed += len(passages)
                logger.info(f"✓ Indexed batch {batch_num} ({processed}/{len(corpus)} total)")
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_num}: {e}")
            failed += len(batch)
            continue
        
        # Progress update
        elapsed = time.time() - start_time
        avg_time_per_batch = elapsed / batch_num if batch_num > 0 else 0
        remaining_batches = total_batches - batch_num
        eta_seconds = avg_time_per_batch * remaining_batches
        
        logger.info(f"Progress: {processed}/{len(corpus)} indexed "
                   f"| ETA: {int(eta_seconds)}s | Failed: {failed}")
    
    total_time = time.time() - start_time
    logger.info(f"✓ Vector index build complete!")
    logger.info(f"  Total chunks: {len(corpus)}")
    logger.info(f"  Successfully indexed: {processed}")
    logger.info(f"  Failed: {failed}")
    logger.info(f"  Time elapsed: {int(total_time)}s")
    logger.info(f"  Average speed: {processed/total_time:.2f} chunks/sec")

if __name__ == "__main__":
    main()

