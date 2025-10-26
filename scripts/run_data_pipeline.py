"""
Complete Data Pipeline Orchestrator
Runs: Scraping → Curator → Preprocessing → Vector Indexing
"""
import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_command(cmd: list, description: str):
    """Run a shell command"""
    logger.info(f"Running: {description}")
    logger.debug(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"✓ {description} completed successfully")
        if result.stdout:
            logger.debug(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ {description} failed")
        logger.error(f"Error: {e.stderr}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Complete Data Pipeline')
    parser.add_argument('--sources', default='all', 
                       choices=['all', 'sep', 'arxiv', 'synthetic'],
                       help='Which data sources to scrape')
    parser.add_argument('--skip-scraping', action='store_true',
                       help='Skip scraping step (use existing data)')
    parser.add_argument('--s3-bucket', 
                       help='S3 bucket for data storage (optional, uses local if not provided)')
    parser.add_argument('--environment', default='dev',
                       help='Environment (dev/prod)')
    
    args = parser.parse_args()
    
    logger.info("=== SynthArbiter Data Pipeline ===")
    logger.info(f"Sources: {args.sources}")
    logger.info(f"S3 Bucket: {args.s3_bucket or 'Local files'}")
    
    # Define paths
    base_dir = Path('data')
    raw_dir = base_dir / 'raw'
    curated_dir = base_dir / 'curated'
    processed_dir = base_dir / 'processed'
    
    # Create directories
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Step 1: Scraping (if not skipped)
    if not args.skip_scraping:
        if args.sources in ['all', 'sep']:
            logger.info("\n=== Step 1a: Scraping Stanford SEP ===")
            sep_file = raw_dir / 'sep_articles.jsonl'
            run_command(
                ['python', 'data_acquisition/scrapers/sep_scraper.py'],
                'SEP scraping'
            )
        
        if args.sources in ['all', 'arxiv']:
            logger.info("\n=== Step 1b: Scraping arXiv ===")
            run_command(
                ['python', 'data_acquisition/scrapers/arxiv_scraper.py'],
                'arXiv scraping'
            )
        
        if args.sources in ['all', 'synthetic']:
            logger.info("\n=== Step 1c: Generating Synthetic Scenarios ===")
            synthetic_file = base_dir / 'synthetic_scenarios.jsonl'
            run_command(
                ['python', 'data_acquisition/synthetic_generator.py'],
                'Synthetic scenario generation'
            )
    
    # Step 2: Curator Pipeline
    logger.info("\n=== Step 2: Data Curation ===")
    
    curated_files = []
    
    # Curate each data source
    for source in ['sep_articles', 'arxiv_ethics_papers', 'synthetic_scenarios']:
        raw_file = raw_dir / f'{source}.jsonl'
        curated_file = curated_dir / f'{source}_curated.jsonl'
        
        if raw_file.exists():
            if args.s3_bucket:
                source_path = f"s3://{args.s3_bucket}/data-raw/{raw_file.name}"
                output_path = f"s3://{args.s3_bucket}/data-curated/{curated_file.name}"
            else:
                source_path = str(raw_file)
                output_path = str(curated_file)
            
            run_command(
                ['python', 'data_acquisition/curator_pipeline.py',
                 '--source', source_path,
                 '--output', output_path,
                 '--min-quality', '60'],
                f'Curation for {source}'
            )
            curated_files.append(curated_file)
        else:
            logger.warning(f"Skipping {source} - file not found")
    
    if not curated_files:
        logger.error("No curated files generated")
        return False
    
    # Step 3: Preprocessing Pipeline
    logger.info("\n=== Step 3: Data Preprocessing ===")
    
    processed_files = []
    
    for curated_file in curated_files:
        processed_file = processed_dir / curated_file.name.replace('_curated', '_processed')
        
        if args.s3_bucket:
            source_path = f"s3://{args.s3_bucket}/data-curated/{curated_file.name}"
            output_path = f"s3://{args.s3_bucket}/data-processed/{processed_file.name}"
        else:
            source_path = str(curated_file)
            output_path = str(processed_file)
        
        if args.s3_bucket or Path(source_path.replace('s3://', '')).exists():
            run_command(
                ['python', 'data_acquisition/preprocess.py',
                 '--source', source_path,
                 '--output', output_path,
                 '--chunk-size', '500'],
                f'Preprocessing for {curated_file.stem}'
            )
            processed_files.append(processed_file)
    
    # Step 4: Vector Indexing
    logger.info("\n=== Step 4: Building Vector Index ===")
    
    # Combine all processed files into one corpus
    corpus_file = processed_dir / 'corpus.jsonl'
    
    logger.info(f"Combining {len(processed_files)} processed files into corpus...")
    
    with open(corpus_file, 'w') as outfile:
        for proc_file in processed_files:
            file_path = proc_file if not args.s3_bucket else None
            
            if args.s3_bucket:
                # Download from S3 first
                import boto3
                s3 = boto3.client('s3')
                bucket = args.s3_bucket
                key = f"data-processed/{proc_file.name}"
                
                logger.info(f"Downloading s3://{bucket}/{key}")
                obj = s3.get_object(Bucket=bucket, Key=key)
                
                for line in obj['Body'].iter_lines():
                    if line:
                        outfile.write(line.decode('utf-8') + '\n')
            else:
                # Read from local file
                with open(file_path, 'r') as infile:
                    for line in infile:
                        if line.strip():
                            outfile.write(line)
    
    logger.info(f"✓ Created corpus with combined data")
    
    # Build vector index
    if args.s3_bucket:
        source_path = f"s3://{args.s3_bucket}/data-processed/corpus.jsonl"
    else:
        source_path = str(corpus_file)
    
    run_command(
        ['python', 'scripts/build_vector_index.py',
         '--source', source_path,
         '--index-name', 'ethical-knowledge',
         '--batch-size', '50'],
        'Vector index building'
    )
    
    logger.info("\n=== Pipeline Complete ===")
    logger.info("Data pipeline executed successfully")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

