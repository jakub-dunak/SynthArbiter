"""
NeMo Curator Data Pipeline
Processes raw data and applies quality filters
"""
import json
import logging
import boto3
from typing import List, Dict
from pathlib import Path
import spacy
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataCurator:
    """Curates data using quality filters and preprocessing"""
    
    def __init__(self):
        # Load spaCy model for NLP processing
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except OSError:
            logger.warning("spaCy model not found. Install with: python -m spacy download en_core_web_sm")
            self.nlp = None
        
        self.s3_client = boto3.client('s3')
    
    def deduplicate(self, documents: List[Dict]) -> List[Dict]:
        """Remove duplicate documents based on content hash"""
        seen = set()
        unique = []
        
        for doc in documents:
            content_hash = hash(doc.get('content', ''))
            if content_hash not in seen:
                seen.add(content_hash)
                unique.append(doc)
        
        logger.info(f"Removed {len(documents) - len(unique)} duplicates")
        return unique
    
    def detect_language(self, text: str) -> str:
        """Detect language of text"""
        if not text or len(text) < 50:
            return 'unknown'
        
        # Simple English detection (can be enhanced with langdetect library)
        common_english_words = ['the', 'and', 'or', 'but', 'is', 'are', 'was', 'were', 'for', 'to']
        words = text.lower().split()
        english_count = sum(1 for word in words if word in common_english_words)
        
        if len(words) > 0 and english_count / len(words) > 0.1:
            return 'en'
        
        return 'unknown'
    
    def filter_language(self, documents: List[Dict]) -> List[Dict]:
        """Keep only English documents"""
        filtered = []
        
        for doc in documents:
            content = doc.get('content', doc.get('text', ''))
            lang = self.detect_language(content)
            
            if lang == 'en':
                filtered.append(doc)
            else:
                logger.debug(f"Filtered out non-English document: {lang}")
        
        logger.info(f"Filtered to {len(filtered)} English documents")
        return filtered
    
    def assess_text_quality(self, text: str) -> Dict:
        """Assess text quality metrics"""
        if not text or len(text) < 100:
            return {'score': 0, 'reason': 'too_short'}
        
        # Basic quality checks
        word_count = len(text.split())
        char_count = len(text)
        sentence_count = text.count('.') + text.count('!') + text.count('?')
        
        # Check for reasonable structure
        avg_words_per_sentence = word_count / max(sentence_count, 1)
        
        score = 0
        if word_count >= 100:
            score += 30
        if avg_words_per_sentence >= 10 and avg_words_per_sentence <= 50:
            score += 30
        if char_count / word_count >= 4.0:  # Reasonable average word length
            score += 20
        if sentence_count > 0:
            score += 20
        
        return {
            'score': score,
            'word_count': word_count,
            'char_count': char_count,
            'sentence_count': sentence_count,
            'avg_words_per_sentence': round(avg_words_per_sentence, 2)
        }
    
    def filter_quality(self, documents: List[Dict], min_score: int = 60) -> List[Dict]:
        """Filter documents by quality score"""
        filtered = []
        
        for doc in documents:
            content = doc.get('content', doc.get('text', ''))
            quality = self.assess_text_quality(content)
            
            if quality['score'] >= min_score:
                doc['quality_metrics'] = quality
                filtered.append(doc)
            else:
                logger.debug(f"Filtered low quality document (score: {quality['score']})")
        
        logger.info(f"Filtered to {len(filtered)} quality documents (min score: {min_score})")
        return filtered
    
    def remove_pii(self, text: str) -> str:
        """Remove personally identifiable information"""
        if not self.nlp:
            # Simple pattern-based PII removal
            import re
            
            # Email patterns
            text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', text)
            
            # Phone patterns
            text = re.sub(r'\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', '[PHONE]', text)
            
            # SSN patterns
            text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN]', text)
            
            return text
        
        # Use spaCy for NER-based PII removal
        doc = self.nlp(text)
        
        # Remove named entities that might be PII
        filtered_tokens = []
        for token in doc:
            if token.ent_type_ in ['PERSON', 'EMAIL', 'PHONE']:
                continue
            filtered_tokens.append(token.text)
        
        return ' '.join(filtered_tokens)
    
    def process(self, documents: List[Dict], apply_pii_removal: bool = True) -> List[Dict]:
        """Process documents through quality filters"""
        logger.info(f"Processing {len(documents)} documents")
        
        # Step 1: Deduplicate
        curated = self.deduplicate(documents)
        
        # Step 2: Language detection
        curated = self.filter_language(curated)
        
        # Step 3: Quality assessment
        curated = self.filter_quality(curated)
        
        # Step 4: PII removal
        if apply_pii_removal:
            for doc in curated:
                content = doc.get('content', doc.get('text', ''))
                doc['content'] = self.remove_pii(content)
                doc['pii_removed'] = True
                doc['curated_at'] = datetime.utcnow().isoformat()
        
        logger.info(f"Curated {len(curated)} documents")
        return curated
    
    def save_to_s3(self, documents: List[Dict], bucket: str, key: str):
        """Save curated documents to S3"""
        logger.info(f"Uploading {len(documents)} documents to s3://{bucket}/{key}")
        
        # Save as JSONL
        body = '\n'.join(json.dumps(doc) for doc in documents)
        
        self.s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode('utf-8'),
            ContentType='application/jsonl'
        )
        
        logger.info(f"✓ Uploaded to s3://{bucket}/{key}")

def main():
    """Main curator pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description='NeMo Curator Pipeline')
    parser.add_argument('--source', required=True, help='Source file or S3 path')
    parser.add_argument('--output', required=True, help='Output S3 path (bucket/key)')
    parser.add_argument('--min-quality', type=int, default=60, help='Minimum quality score')
    parser.add_argument('--no-pii-removal', action='store_true', help='Skip PII removal')
    
    args = parser.parse_args()
    
    # Initialize curator
    curator = DataCurator()
    
    # Load documents
    documents = []
    
    if args.source.startswith('s3://'):
        # Load from S3
        bucket, key = args.source.replace('s3://', '').split('/', 1)
        logger.info(f"Loading from s3://{bucket}/{key}")
        
        response = curator.s3_client.get_object(Bucket=bucket, Key=key)
        for line in response['Body'].iter_lines():
            if line:
                documents.append(json.loads(line))
    else:
        # Load from local file
        with open(args.source, 'r') as f:
            for line in f:
                if line.strip():
                    documents.append(json.loads(line))
    
    # Process documents
    curated_docs = curator.process(documents, apply_pii_removal=not args.no_pii_removal)
    
    # Save to S3
    bucket, key = args.output.replace('s3://', '').split('/', 1)
    curator.save_to_s3(curated_docs, bucket, key)

if __name__ == "__main__":
    main()

