"""
Data Preprocessing Pipeline
Tokenize, chunk, and extract entities from curated text
"""
import json
import logging
import boto3
from typing import List, Dict, Optional
import spacy
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPreprocessor:
    """Preprocesses curated documents for embedding"""
    
    def __init__(self, chunk_size: int = 500):
        self.chunk_size = chunk_size  # Target chunk size in tokens
        
        # Load spaCy model
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.warning("spaCy model not found. Install with: python -m spacy download en_core_web_sm")
            self.nlp = None
        
        self.s3_client = boto3.client('s3')
    
    def chunk_text(self, text: str, metadata: Dict = None) -> List[Dict]:
        """
        Chunk text into passages suitable for embedding
        
        Args:
            text: Input text to chunk
            metadata: Document metadata
            
        Returns:
            List of chunks with metadata
        """
        if not self.nlp:
            # Simple word-based chunking if spaCy not available
            return self._simple_chunk(text, metadata)
        
        # Use spaCy for intelligent sentence-aware chunking
        doc = self.nlp(text)
        
        chunks = []
        current_chunk = []
        current_length = 0
        chunk_id = 0
        
        for sent in doc.sents:
            sent_tokens = len(sent)
            
            # If adding this sentence would exceed chunk size
            if current_length + sent_tokens > self.chunk_size and current_chunk:
                # Save current chunk
                chunk_text = ' '.join(current_chunk)
                chunks.append({
                    'text': chunk_text,
                    'metadata': {
                        **(metadata or {}),
                        'chunk_id': chunk_id,
                        'chunk_length': current_length,
                        'preprocessed_at': datetime.utcnow().isoformat()
                    }
                })
                chunk_id += 1
                current_chunk = [sent.text]
                current_length = sent_tokens
            else:
                current_chunk.append(sent.text)
                current_length += sent_tokens
        
        # Add final chunk
        if current_chunk:
            chunk_text = ' '.join(current_chunk)
            chunks.append({
                'text': chunk_text,
                'metadata': {
                    **(metadata or {}),
                    'chunk_id': chunk_id,
                    'chunk_length': current_length,
                    'preprocessed_at': datetime.utcnow().isoformat()
                }
            })
        
        return chunks
    
    def _simple_chunk(self, text: str, metadata: Dict = None) -> List[Dict]:
        """Simple word-based chunking fallback"""
        words = text.split()
        chunks = []
        chunk_id = 0
        
        for i in range(0, len(words), self.chunk_size):
            chunk = ' '.join(words[i:i + self.chunk_size])
            chunks.append({
                'text': chunk,
                'metadata': {
                    **(metadata or {}),
                    'chunk_id': chunk_id,
                    'preprocessed_at': datetime.utcnow().isoformat()
                }
            })
            chunk_id += 1
        
        return chunks
    
    def extract_entities(self, text: str) -> List[str]:
        """Extract named entities from text"""
        if not self.nlp:
            return []
        
        doc = self.nlp(text)
        entities = list(set([ent.text for ent in doc.ents]))
        return entities
    
    def extract_concepts(self, text: str) -> Dict[str, List[str]]:
        """Extract key concepts: philosophers, ethical frameworks, technologies"""
        if not self.nlp:
            return {}
        
        doc = self.nlp(text)
        
        concepts = {
            'philosophers': [],
            'ethical_terms': [],
            'technologies': [],
            'organizations': []
        }
        
        # Named entity extraction
        for ent in doc.ents:
            if ent.label_ == 'PERSON':
                # Check if it's a known philosopher
                person_lower = ent.text.lower()
                known_philosophers = ['kant', 'mill', 'rawls', 'bentham', 'aristotle', 'plato', 
                              'hume', 'kierkegaard', 'nietzsche', 'sartre', 'foucault']
                if any(phil in person_lower for phil in known_philosophers):
                    concepts['philosophers'].append(ent.text)
            elif ent.label_ == 'ORG':
                concepts['organizations'].append(ent.text)
        
        # Extract ethical terms
        ethical_keywords = ['ethics', 'morality', 'moral', 'ethical', 'virtue', 'duty', 
                           'rights', 'justice', 'fairness', 'utilitarian', 'deontological', 
                           'consequentialist', 'autonomy', 'beneficence', 'non-maleficence']
        
        text_lower = text.lower()
        for keyword in ethical_keywords:
            if keyword in text_lower:
                concepts['ethical_terms'].append(keyword)
        
        # Extract technology terms
        tech_keywords = ['ai', 'artificial intelligence', 'neural', 'brain', 'organoid', 
                        'genetic', 'biotechnology', 'algorithm', 'machine learning', 
                        'consciousness', 'sentient', 'autonomous']
        
        for keyword in tech_keywords:
            if keyword in text_lower:
                concepts['technologies'].append(keyword)
        
        return concepts
    
    def normalize_text(self, text: str) -> str:
        """Normalize text: remove citations, clean up formatting"""
        # Remove common citation patterns
        import re
        
        # Remove (Author, Year) citations
        text = re.sub(r'\([A-Z][a-z]+\s*,\s*\d{4}\)', '', text)
        
        # Remove footnote markers [1], (1), etc.
        text = re.sub(r'\[?\d+\]?', '', text)
        
        # Remove multiple whitespaces
        text = re.sub(r'\s+', ' ', text)
        
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        
        # Remove email addresses
        text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
        
        return text.strip()
    
    def preprocess(self, documents: List[Dict]) -> List[Dict]:
        """
        Preprocess documents for vector store
        
        Returns:
            List of chunks ready for embedding
        """
        all_chunks = []
        
        for doc_idx, doc in enumerate(documents):
            text = doc.get('content', doc.get('text', ''))
            if not text:
                continue
            
            # Normalize text
            text = self.normalize_text(text)
            
            # Extract metadata
            metadata = {
                **doc.get('metadata', {}),
                'source': doc.get('source', 'unknown'),
                'id': doc.get('id', f"doc_{doc_idx}"),
                'url': doc.get('url', ''),
                'license': doc.get('license', '')
            }
            
            # Extract concepts
            concepts = self.extract_concepts(text)
            metadata['concepts'] = concepts
            
            # Extract named entities
            entities = self.extract_entities(text)
            metadata['entities'] = entities
            
            # Chunk text
            chunks = self.chunk_text(text, metadata)
            all_chunks.extend(chunks)
            
            if (doc_idx + 1) % 100 == 0:
                logger.info(f"Processed {doc_idx + 1}/{len(documents)} documents")
        
        logger.info(f"Generated {len(all_chunks)} chunks from {len(documents)} documents")
        return all_chunks
    
    def load_from_s3(self, bucket: str, key: str) -> List[Dict]:
        """Load documents from S3"""
        logger.info(f"Loading from s3://{bucket}/{key}")
        
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        documents = []
        
        for line in response['Body'].iter_lines():
            if line:
                documents.append(json.loads(line))
        
        return documents
    
    def save_to_s3(self, chunks: List[Dict], bucket: str, key: str):
        """Save preprocessed chunks to S3"""
        logger.info(f"Uploading {len(chunks)} chunks to s3://{bucket}/{key}")
        
        # Save as JSONL
        body = '\n'.join(json.dumps(chunk) for chunk in chunks)
        
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode('utf-8'),
            ContentType='application/jsonl'
        )
        
        logger.info(f"✓ Uploaded to s3://{bucket}/{key}")

def main():
    """Main preprocessing pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Preprocessing Pipeline')
    parser.add_argument('--source', required=True, help='Source S3 path (bucket/key) or local file')
    parser.add_argument('--output', required=True, help='Output S3 path (bucket/key) or local file')
    parser.add_argument('--chunk-size', type=int, default=500, help='Chunk size in tokens')
    
    args = parser.parse_args()
    
    # Initialize preprocessor
    preprocessor = DataPreprocessor(chunk_size=args.chunk_size)
    
    # Load documents
    if args.source.startswith('s3://'):
        bucket, key = args.source.replace('s3://', '').split('/', 1)
        documents = preprocessor.load_from_s3(bucket, key)
    else:
        # Load from local file
        with open(args.source, 'r') as f:
            documents = [json.loads(line) for line in f if line.strip()]
    
    # Preprocess documents
    chunks = preprocessor.preprocess(documents)
    
    # Save chunks
    if args.output.startswith('s3://'):
        bucket, key = args.output.replace('s3://', '').split('/', 1)
        preprocessor.save_to_s3(chunks, bucket, key)
    else:
        # Save to local file
        with open(args.output, 'w') as f:
            for chunk in chunks:
                f.write(json.dumps(chunk) + '\n')
        
        logger.info(f"✓ Saved to {args.output}")

if __name__ == "__main__":
    main()

