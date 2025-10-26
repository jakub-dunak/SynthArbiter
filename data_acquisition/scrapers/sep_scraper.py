"""
Stanford Encyclopedia of Philosophy Scraper
Respects robots.txt, implements rate limiting, preserves attribution
"""
import requests
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SEPScraper:
    def __init__(self, base_url: str, rate_limit: int = 1):
        self.base_url = base_url
        self.rate_limit = rate_limit  # requests per second
        self.last_request_time = 0
        self.robots_parser = RobotFileParser()
        self.robots_parser.set_url(f"{base_url}/robots.txt")
        self.robots_parser.read()
    
    def _can_fetch(self, url: str) -> bool:
        """Check if URL can be fetched according to robots.txt"""
        return self.robots_parser.can_fetch("SynthArbiter/1.0", url)
    
    def _rate_limit(self):
        """Enforce rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < (1.0 / self.rate_limit):
            time.sleep((1.0 / self.rate_limit) - elapsed)
        self.last_request_time = time.time()
    
    def fetch_article(self, path: str) -> Dict:
        """
        Fetch and parse an SEP article
        Returns article content with metadata
        """
        url = f"{self.base_url}{path}"
        
        if not self._can_fetch(url):
            logger.warning(f"robots.txt disallows: {url}")
            return None
        
        self._rate_limit()
        
        try:
            response = requests.get(
                url,
                headers={
                    'User-Agent': 'SynthArbiter/1.0 (Research; Contact: support@example.com)',
                    'Accept': 'text/html'
                },
                timeout=30
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract article metadata
            title = soup.find('h1', class_='article-title')
            if not title:
                title = soup.find('title')
            title_text = title.get_text(strip=True) if title else path.split('/')[-1]
            
            # Extract article body (main content)
            article_body = soup.find('div', class_='article-content') or soup.find('article')
            if not article_body:
                article_body = soup.find('div', id='main-text')
            
            if not article_body:
                logger.error(f"Could not find article content in {url}")
                return None
            
            # Remove non-content elements
            for tag in article_body.find_all(['nav', 'header', 'footer', 'aside', 'script', 'style']):
                tag.decompose()
            
            content = article_body.get_text(separator='\n', strip=True)
            
            # Extract references/citations
            references = []
            for ref in article_body.find_all(['cite', 'span'], class_='reference'):
                references.append(ref.get_text(strip=True))
            
            return {
                'id': f"sep_{path.replace('/', '_')}",
                'title': title_text,
                'content': content,
                'url': url,
                'source': 'Stanford Encyclopedia of Philosophy',
                'license': 'CC BY-NC-ND 4.0',
                'references': references,
                'scraped_at': datetime.utcnow().isoformat(),
                'path': path
            }
        
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def scrape_articles(self, paths: List[str]) -> List[Dict]:
        """Scrape multiple articles"""
        articles = []
        for path in paths:
            logger.info(f"Scraping: {path}")
            article = self.fetch_article(path)
            if article:
                articles.append(article)
        
        return articles

if __name__ == "__main__":
    scraper = SEPScraper("https://plato.stanford.edu", rate_limit=1)
    
    # Test with a small subset
    test_paths = [
        "/entries/consciousness",
        "/entries/artificial-intelligence"
    ]
    
    articles = scraper.scrape_articles(test_paths)
    
    # Save to JSONL
    with open("data/raw/sep_articles.jsonl", "w") as f:
        for article in articles:
            f.write(json.dumps(article) + "\n")
    
    logger.info(f"Scraped {len(articles)} articles")

