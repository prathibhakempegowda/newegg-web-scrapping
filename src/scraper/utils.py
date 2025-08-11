"""
Utility functions for web scraping
"""
import asyncio
import random
import re
import time
import logging
from typing import List, Optional, Dict, Any, Union
from urllib.parse import urljoin, urlparse
from datetime import datetime
import hashlib

from fake_useragent import UserAgent
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class ScrapingUtils:
    """Utility class for web scraping operations"""
    
    def __init__(self):
        self.ua = UserAgent()
    
    @staticmethod
    def get_random_delay(min_delay: float = 2.0, max_delay: float = 4.0) -> float:
        """Get a random delay between min and max values"""
        return random.uniform(min_delay, max_delay)
    
    @staticmethod
    async def async_delay(min_delay: float = 2.0, max_delay: float = 4.0):
        """Async delay with random timing"""
        delay = ScrapingUtils.get_random_delay(min_delay, max_delay)
        await asyncio.sleep(delay)
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent string"""
        try:
            return self.ua.random
        except Exception:
            # Fallback to predefined user agents
            from .config import USER_AGENTS
            return random.choice(USER_AGENTS)
    
    @staticmethod
    def clean_text(text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        
        # Remove special characters that might cause issues
        text = re.sub(r'[^\w\s\-.,!?()$%&@#]', '', text)
        
        return text
    
    @staticmethod
    def extract_price(price_text: str) -> Optional[float]:
        """Extract price from text string"""
        if not price_text:
            return None
        
        # Remove currency symbols and extract numbers
        price_match = re.search(r'[\$]?(\d+(?:,\d{3})*(?:\.\d{2})?)', price_text.replace(',', ''))
        if price_match:
            try:
                return float(price_match.group(1))
            except ValueError:
                pass
        
        return None
    
    @staticmethod
    def extract_rating(rating_text: str) -> Optional[float]:
        """Extract rating from text or HTML"""
        if not rating_text:
            return None
        
        # Look for rating patterns like "4.5 out of 5" or "4.5/5"
        rating_patterns = [
            r'(\d+\.?\d*)\s*(?:out\s*of\s*5|/5|\s*stars?)',
            r'(\d+\.?\d*)\s*eggs?',  # Newegg specific
            r'rating[:\s]*(\d+\.?\d*)',
            r'(\d+\.?\d*)'  # Fallback to any decimal
        ]
        
        for pattern in rating_patterns:
            match = re.search(pattern, rating_text, re.IGNORECASE)
            if match:
                try:
                    rating = float(match.group(1))
                    if 0 <= rating <= 5:  # Validate rating range
                        return rating
                except ValueError:
                    continue
        
        return None
    
    @staticmethod
    def extract_review_count(text: str) -> Optional[int]:
        """Extract review count from text"""
        if not text:
            return None
        
        # Look for patterns like "(123 reviews)" or "123 ratings"
        count_patterns = [
            r'\((\d+(?:,\d{3})*)\s*reviews?\)',
            r'(\d+(?:,\d{3})*)\s*reviews?',
            r'(\d+(?:,\d{3})*)\s*ratings?',
            r'(\d+(?:,\d{3})*)'  # Fallback to any number
        ]
        
        for pattern in count_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    count_str = match.group(1).replace(',', '')
                    return int(count_str)
                except ValueError:
                    continue
        
        return None
    
    @staticmethod
    def parse_date(date_text: str) -> Optional[datetime]:
        """Parse date from various formats"""
        if not date_text:
            return None
        
        # Common date patterns
        date_patterns = [
            r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY
            r'(\d{4}-\d{2}-\d{2})',      # YYYY-MM-DD
            r'(\w+ \d{1,2}, \d{4})',     # Month DD, YYYY
            r'(\d{1,2} \w+ \d{4})',      # DD Month YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_text)
            if match:
                date_str = match.group(1)
                try:
                    # Try different parsing formats
                    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%B %d, %Y', '%d %B %Y']:
                        try:
                            return datetime.strptime(date_str, fmt)
                        except ValueError:
                            continue
                except Exception:
                    continue
        
        return None
    
    @staticmethod
    def is_verified_purchase(text: str) -> bool:
        """Check if review is from verified purchase"""
        if not text:
            return False
        
        verified_indicators = [
            'verified purchase',
            'verified buyer',
            'confirmed purchase',
            'verified owner'
        ]
        
        text_lower = text.lower()
        return any(indicator in text_lower for indicator in verified_indicators)
    
    @staticmethod
    def generate_hash(text: str) -> str:
        """Generate a hash for deduplication"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    @staticmethod
    def safe_find_element(soup: BeautifulSoup, selectors: List[str], method: str = 'css') -> Optional[Any]:
        """Safely find element using multiple selectors"""
        for selector in selectors:
            try:
                if method == 'css':
                    element = soup.select_one(selector)
                    if element:
                        return element
                elif method == 'xpath':
                    # Note: BeautifulSoup doesn't support XPath directly
                    # This would need lxml or selenium
                    pass
            except Exception as e:
                logger.debug(f"Selector {selector} failed: {e}")
                continue
        
        return None
    
    @staticmethod
    def safe_find_elements(soup: BeautifulSoup, selectors: List[str], method: str = 'css') -> List[Any]:
        """Safely find multiple elements using multiple selectors"""
        for selector in selectors:
            try:
                if method == 'css':
                    elements = soup.select(selector)
                    if elements:
                        return elements
                elif method == 'xpath':
                    # Note: BeautifulSoup doesn't support XPath directly
                    pass
            except Exception as e:
                logger.debug(f"Selector {selector} failed: {e}")
                continue
        
        return []
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """Validate URL format"""
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc])
        except Exception:
            return False
    
    @staticmethod
    def normalize_url(url: str, base_url: str = "") -> str:
        """Normalize and join URLs"""
        if not url:
            return ""
        
        if url.startswith(('http://', 'https://')):
            return url
        
        if base_url:
            return urljoin(base_url, url)
        
        return url

class RetryHelper:
    """Helper class for retry logic with exponential backoff"""
    
    @staticmethod
    async def async_retry(
        func,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
        exceptions: tuple = (Exception,)
    ):
        """Async retry with exponential backoff"""
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                result = func()
                if asyncio.iscoroutine(result):
                    return await result
                else:
                    return result
            except exceptions as e:
                last_exception = e
                
                if attempt == max_retries:
                    break
                
                delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                jitter = random.uniform(0, 0.1) * delay  # Add jitter
                total_delay = delay + jitter
                
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {total_delay:.2f}s")
                await asyncio.sleep(total_delay)
        
        raise last_exception

class RateLimiter:
    """Simple rate limiter for API calls"""
    
    def __init__(self, max_calls: int = 10, time_window: float = 60.0):
        self.max_calls = max_calls
        self.time_window = time_window
        self.calls = []
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire rate limit permission"""
        async with self._lock:
            now = time.time()
            
            # Remove old calls outside the time window
            self.calls = [call_time for call_time in self.calls if now - call_time < self.time_window]
            
            # Check if we can make a call
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            
            # Calculate wait time
            oldest_call = min(self.calls)
            wait_time = self.time_window - (now - oldest_call)
            
            if wait_time > 0:
                logger.info(f"Rate limit reached. Waiting {wait_time:.2f}s")
                await asyncio.sleep(wait_time)
                await self.acquire()  # Recursive call after waiting

def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file) if log_file else logging.NullHandler()
        ]
    )
    
    return logging.getLogger(__name__)
