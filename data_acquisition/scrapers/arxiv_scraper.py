"""
arXiv API Scraper for AI Ethics Papers
Uses official arXiv API - no web scraping required
"""
import requests
import feedparser
from typing import List, Dict
import json
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ArxivScraper:
    def __init__(self, base_url: str = "https://arxiv.org/api/query"):
        self.base_url = base_url
    
    def search_papers(self, query: str, max_results: int = 100) -> List[Dict]:
        """
        Search arXiv for papers matching query
        Returns list of paper metadata
        """
        params = {
            'search_query': query,
            'start': 0,
            'max_results': max_results,
            'sortBy': 'submittedDate',
            'sortOrder': 'descending'
        }
        
        try:
            response = requests.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            feed = feedparser.parse(response.content)
            papers = []
            
            for entry in feed.entries:
                # Extract information
                paper = {
                    'id': f"arxiv_{entry.id.split('/')[-1]}",
                    'title': entry.title,
                    'authors': [author.name for author in entry.authors],
                    'published': entry.published,
                    'arxiv_id': entry.id.split('/')[-1],
                    'summary': entry.summary,
                    'url': entry.link,
                    'source': 'arXiv',
                    'license': 'arXiv Open Access',
                    'scraped_at': datetime.utcnow().isoformat(),
                    'tags': [tag.term for tag in entry.tags],
                    'arxiv_primary_category': entry.arxiv_primary_category['term'] if hasattr(entry, 'arxiv_primary_category') else None
                }
                papers.append(paper)
            
            logger.info(f"Found {len(papers)} papers")
            return papers
        
        except Exception as e:
            logger.error(f"Error searching arXiv: {e}")
            return []
    
    def filter_relevant(self, papers: List[Dict], keywords: List[str] = None) -> List[Dict]:
        """Filter papers based on keywords in title/summary"""
        if not keywords:
            return papers
        
        keywords_lower = [kw.lower() for kw in keywords]
        relevant = []
        
        for paper in papers:
            text = f"{paper['title']} {paper['summary']}".lower()
            if any(kw in text for kw in keywords_lower):
                relevant.append(paper)
        
        return relevant

if __name__ == "__main__":
    scraper = ArxivScraper()
    
    # Search for AI ethics and consciousness papers
    query = "cat:cs.AI AND (abstract:\"ethics\" OR abstract:\"consciousness\" OR abstract:\"mind\")"
    
    papers = scraper.search_papers(query, max_results=50)
    
    # Filter for relevant papers
    keywords = ['ethics', 'consciousness', 'artificial', 'sentience', 'rights']
    relevant_papers = scraper.filter_relevant(papers, keywords)
    
    # Save to JSONL
    with open("data/raw/arxiv_ethics_papers.jsonl", "w") as f:
        for paper in relevant_papers:
            f.write(json.dumps(paper) + "\n")
    
    logger.info(f"Saved {len(relevant_papers)} relevant papers")

