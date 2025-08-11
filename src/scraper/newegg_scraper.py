"""
Newegg-specific web scraper implementation
"""
import asyncio
import json
import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from .base_scraper import BaseScraper, ScrapingResult
from .config import SELECTORS, XPATH_SELECTORS, SELENIUM_EXPLICIT_WAIT
from .utils import ScrapingUtils

logger = logging.getLogger(__name__)

@dataclass
class ProductInfo:
    """Product information data class"""
    title: Optional[str] = None
    brand: Optional[str] = None
    price: Optional[float] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    description: Optional[str] = None
    url: Optional[str] = None
    scraped_at: datetime = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.now()

@dataclass
class ReviewInfo:
    """Customer review information data class"""
    reviewer_name: Optional[str] = None
    rating: Optional[float] = None
    title: Optional[str] = None
    body: Optional[str] = None
    date: Optional[datetime] = None
    verified_purchase: bool = False
    helpful_count: Optional[int] = None
    product_url: Optional[str] = None
    review_id: Optional[str] = None
    scraped_at: datetime = None
    
    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.now()
        if self.review_id is None and self.reviewer_name and self.body:
            # Generate a simple hash for review ID
            import hashlib
            content = f"{self.reviewer_name}{self.body}{self.date}"
            self.review_id = hashlib.md5(content.encode()).hexdigest()[:16]

class NeweggScraper(BaseScraper):
    """Specialized scraper for Newegg product pages"""
    
    def __init__(self):
        super().__init__()
        self.utils = ScrapingUtils()
    
    async def _extract_data_selenium(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Dict[str, Any]:
        """Extract data using Selenium with enhanced capabilities"""
        try:
            # Extract product information
            product_info = await self._extract_product_info_selenium(soup, driver)
            
            # Extract reviews
            reviews = await self._extract_reviews_selenium(soup, driver)
            
            return {
                "product": product_info,
                "reviews": reviews,
                "total_reviews_found": len(reviews),
                "extraction_method": "selenium"
            }
            
        except Exception as e:
            logger.error(f"Selenium data extraction failed: {e}")
            raise
    
    async def _extract_data_requests(self, soup: BeautifulSoup, response: Any) -> Dict[str, Any]:
        """Extract data using requests/aiohttp methods"""
        try:
            # Check if soup is valid
            if soup is None:
                raise ValueError("BeautifulSoup object is None - page content could not be parsed")
            
            # Extract product information
            product_info = await self._extract_product_info_requests(soup)
            
            # Extract reviews (limited for static content)
            reviews = await self._extract_reviews_requests(soup)
            
            return {
                "product": product_info,
                "reviews": reviews,
                "total_reviews_found": len(reviews),
                "extraction_method": "requests"
            }
            
        except Exception as e:
            logger.error(f"Requests data extraction failed: {e}")
            raise
    
    async def _extract_product_info_selenium(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Dict[str, Any]:
        """Extract product information using Selenium"""
        product = ProductInfo()
        
        try:
            # Extract title
            product.title = self._extract_product_title(soup, driver)
            
            # Extract brand
            product.brand = self._extract_product_brand(soup, driver)
            
            # Extract price
            product.price = self._extract_product_price(soup, driver)
            
            # Extract rating
            product.rating = self._extract_product_rating(soup, driver)
            
            # Extract review count
            product.review_count = self._extract_review_count(soup, driver)
            
            # Extract description
            product.description = self._extract_product_description(soup, driver)
            
            # Set URL
            product.url = driver.current_url
            
            logger.info(f"Extracted product: {product.title} - ${product.price}")
            
            return {
                "title": product.title,
                "brand": product.brand,
                "price": product.price,
                "rating": product.rating,
                "review_count": product.review_count,
                "description": product.description,
                "url": product.url,
                "scraped_at": product.scraped_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Product info extraction failed: {e}")
            return {}
    
    async def _extract_product_info_requests(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract product information using requests"""
        product = ProductInfo()
        
        try:
            # Extract title
            title_selectors = SELECTORS["product"]["title"]
            product.title = self.extract_text_safe(soup, title_selectors)
            
            # Extract brand
            brand_selectors = SELECTORS["product"]["brand"]
            product.brand = self.extract_text_safe(soup, brand_selectors)
            
            # Extract price
            price_selectors = SELECTORS["product"]["price"]
            price_text = self.extract_text_safe(soup, price_selectors)
            product.price = self.utils.extract_price(price_text) if price_text else None
            
            # Extract rating
            rating_selectors = SELECTORS["product"]["rating"]
            rating_text = self.extract_text_safe(soup, rating_selectors)
            product.rating = self.utils.extract_rating(rating_text) if rating_text else None
            
            # Extract review count
            review_count_selectors = SELECTORS["product"]["review_count"]
            review_count_text = self.extract_text_safe(soup, review_count_selectors)
            product.review_count = self.utils.extract_review_count(review_count_text) if review_count_text else None
            
            # Extract description
            desc_selectors = SELECTORS["product"]["description"]
            product.description = self.extract_text_safe(soup, desc_selectors)
            
            return {
                "title": product.title,
                "brand": product.brand,
                "price": product.price,
                "rating": product.rating,
                "review_count": product.review_count,
                "description": product.description,
                "scraped_at": product.scraped_at.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Product info extraction failed: {e}")
            return {}
    
    def _extract_product_title(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Optional[str]:
        """Extract product title with multiple strategies"""
        # Strategy 1: CSS selectors
        title_selectors = SELECTORS["product"]["title"]
        title = self.extract_text_safe(soup, title_selectors)
        if title:
            return title
        
        # Strategy 2: XPath with Selenium
        try:
            xpath_selectors = XPATH_SELECTORS["product"]["title"]
            for xpath in xpath_selectors:
                try:
                    element = driver.find_element(By.XPATH, xpath)
                    if element and element.text.strip():
                        return self.utils.clean_text(element.text)
                except NoSuchElementException:
                    continue
        except Exception as e:
            logger.debug(f"XPath title extraction failed: {e}")
        
        # Strategy 3: Page title fallback
        try:
            page_title = driver.title
            if page_title and "Newegg" not in page_title:
                return self.utils.clean_text(page_title)
        except Exception:
            pass
        
        return None
    
    def _extract_product_brand(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Optional[str]:
        """Extract product brand"""
        brand_selectors = SELECTORS["product"]["brand"]
        brand = self.extract_text_safe(soup, brand_selectors)
        
        if brand:
            # Clean brand name
            brand = re.sub(r'^(Brand:\s*|Manufacturer:\s*)', '', brand, flags=re.IGNORECASE)
            return self.utils.clean_text(brand)
        
        # Try to extract from title
        title = self._extract_product_title(soup, driver)
        if title:
            # Common brand extraction patterns
            brand_patterns = [
                r'^(AMD|Intel|NVIDIA|ASUS|MSI|Gigabyte|EVGA|Corsair|G\.Skill|Samsung|Western Digital)\b',
                r'\b(Ryzen|Core|GeForce|Radeon)\s+\w+',
            ]
            
            for pattern in brand_patterns:
                match = re.search(pattern, title, re.IGNORECASE)
                if match:
                    return match.group(1)
        
        return None
    
    def _extract_product_price(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Optional[float]:
        """Extract product price"""
        price_selectors = SELECTORS["product"]["price"]
        price_text = self.extract_text_safe(soup, price_selectors)
        
        if price_text:
            return self.utils.extract_price(price_text)
        
        # Try XPath with Selenium
        try:
            xpath_selectors = XPATH_SELECTORS["product"]["price"]
            for xpath in xpath_selectors:
                try:
                    element = driver.find_element(By.XPATH, xpath)
                    if element and element.text.strip():
                        return self.utils.extract_price(element.text)
                except NoSuchElementException:
                    continue
        except Exception as e:
            logger.debug(f"XPath price extraction failed: {e}")
        
        return None
    
    def _extract_product_rating(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Optional[float]:
        """Extract product rating"""
        rating_selectors = SELECTORS["product"]["rating"]
        
        # Try CSS selectors first
        rating_element = self.utils.safe_find_element(soup, rating_selectors)
        if rating_element:
            # Check for data attributes first
            for attr in ['data-rating', 'data-value', 'rating']:
                rating_value = rating_element.get(attr)
                if rating_value:
                    try:
                        return float(rating_value)
                    except (ValueError, TypeError):
                        continue
            
            # Check text content
            rating_text = rating_element.get_text(strip=True)
            if rating_text:
                rating = self.utils.extract_rating(rating_text)
                if rating:
                    return rating
        
        # Try to count visual rating elements (like stars/eggs)
        try:
            # Look for filled stars/eggs
            filled_rating_selectors = [
                ".rating-eggs .rating-filled",
                ".rating-stars .filled",
                ".egg.full",
                ".star.filled"
            ]
            
            for selector in filled_rating_selectors:
                filled_elements = soup.select(selector)
                if filled_elements:
                    return float(len(filled_elements))
        except Exception as e:
            logger.debug(f"Visual rating extraction failed: {e}")
        
        return None
    
    def _extract_review_count(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Optional[int]:
        """Extract review count"""
        review_count_selectors = SELECTORS["product"]["review_count"]
        review_count_text = self.extract_text_safe(soup, review_count_selectors)
        
        if review_count_text:
            return self.utils.extract_review_count(review_count_text)
        
        return None
    
    def _extract_product_description(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Optional[str]:
        """Extract product description"""
        desc_selectors = SELECTORS["product"]["description"]
        
        # Try to get description from multiple potential locations
        descriptions = []
        
        for selector in desc_selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if text and len(text) > 50:  # Only meaningful descriptions
                    descriptions.append(text)
        
        if descriptions:
            # Return the longest description
            return self.utils.clean_text(max(descriptions, key=len))
        
        return None
    
    def _extract_reviews_from_json_ld(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract reviews from JSON-LD structured data"""
        reviews = []
        
        try:
            # Find all JSON-LD script tags
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            
            for script in json_ld_scripts:
                try:
                    if not script.string:
                        continue
                    
                    # Parse JSON-LD data
                    json_data = json.loads(script.string)
                    
                    # Handle arrays of JSON-LD objects
                    if isinstance(json_data, list):
                        for item in json_data:
                            reviews.extend(self._parse_json_ld_reviews(item))
                    else:
                        reviews.extend(self._parse_json_ld_reviews(json_data))
                        
                except json.JSONDecodeError as e:
                    logger.debug(f"Failed to parse JSON-LD script: {e}")
                    continue
                except Exception as e:
                    logger.debug(f"Error processing JSON-LD script: {e}")
                    continue
            
            if reviews:
                logger.info(f"Extracted {len(reviews)} reviews from JSON-LD structured data")
            
            return reviews
            
        except Exception as e:
            logger.error(f"JSON-LD extraction failed: {e}")
            return []
    
    def _parse_json_ld_reviews(self, json_data: Dict) -> List[Dict[str, Any]]:
        """Parse JSON-LD data to extract review information"""
        reviews = []
        
        try:
            # Check for Product schema with reviews
            if json_data.get('@type') == 'Product':
                # Look for aggregateRating and reviews
                aggregate_rating = json_data.get('aggregateRating', {})
                product_reviews = json_data.get('review', [])
                
                # Ensure reviews is a list
                if isinstance(product_reviews, dict):
                    product_reviews = [product_reviews]
                
                for review_data in product_reviews:
                    if isinstance(review_data, dict):
                        review = self._parse_single_json_ld_review(review_data)
                        if review:
                            reviews.append(review)
            
            # Check for Review schema directly
            elif json_data.get('@type') == 'Review':
                review = self._parse_single_json_ld_review(json_data)
                if review:
                    reviews.append(review)
            
            # Check for ReviewAction or other review-related schemas
            elif 'review' in json_data:
                review_data = json_data['review']
                if isinstance(review_data, list):
                    for review in review_data:
                        parsed_review = self._parse_single_json_ld_review(review)
                        if parsed_review:
                            reviews.append(parsed_review)
                elif isinstance(review_data, dict):
                    parsed_review = self._parse_single_json_ld_review(review_data)
                    if parsed_review:
                        reviews.append(parsed_review)
            
        except Exception as e:
            logger.debug(f"Error parsing JSON-LD reviews: {e}")
        
        return reviews
    
    def _parse_single_json_ld_review(self, review_data: Dict) -> Optional[Dict[str, Any]]:
        """Parse a single JSON-LD review object"""
        try:
            review = {}
            
            # Extract reviewer name
            author = review_data.get('author', {})
            if isinstance(author, dict):
                reviewer_name = author.get('name', 'Anonymous')
            elif isinstance(author, str):
                reviewer_name = author
            else:
                reviewer_name = 'Anonymous'
            
            review['reviewer_name'] = self.utils.clean_text(reviewer_name)
            
            # Extract rating
            rating_value = None
            review_rating = review_data.get('reviewRating', {})
            if isinstance(review_rating, dict):
                rating_value = review_rating.get('ratingValue')
            
            if rating_value is not None:
                try:
                    review['rating'] = float(rating_value)
                except (ValueError, TypeError):
                    review['rating'] = None
            else:
                review['rating'] = None
            
            # Extract title/headline
            headline = review_data.get('headline') or review_data.get('name')
            review['title'] = self.utils.clean_text(headline) if headline else None
            
            # Extract review body
            review_body = (
                review_data.get('reviewBody') or 
                review_data.get('description') or 
                review_data.get('text')
            )
            review['body'] = self.utils.clean_text(review_body) if review_body else None
            
            # Extract date
            date_published = review_data.get('datePublished')
            if date_published:
                review['date'] = self._parse_review_date(date_published)
            else:
                review['date'] = None
            
            # Extract verified purchase (if available)
            review['verified_purchase'] = False  # JSON-LD rarely includes this
            
            # Add metadata
            review['product_url'] = None  # Will be set by caller
            review['scraped_at'] = datetime.now().isoformat()
            
            # Only return if we have essential content
            if review.get('body') or review.get('title'):
                return review
            
        except Exception as e:
            logger.debug(f"Error parsing single JSON-LD review: {e}")
        
        return None
    
    async def _handle_cloudflare_protection(self, driver: webdriver.Chrome = None):
        """Enhanced Cloudflare protection handling"""
        try:
            # Use provided driver or fall back to self.driver
            if driver is None:
                driver = self.driver
            
            logger.info("Handling Cloudflare protection...")
            
            # Wait longer for Cloudflare to process
            await asyncio.sleep(10)  # Initial wait
            
            # Check if we need to interact with Cloudflare challenge
            page_source = driver.page_source.lower()
            
            # Look for different types of Cloudflare challenges
            if 'verify you are human' in page_source:
                logger.info("Human verification challenge detected")
                # Try to find and click verification button
                try:
                    verify_button = WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, 'input[type="checkbox"]'))
                    )
                    verify_button.click()
                    await asyncio.sleep(5)
                except TimeoutException:
                    logger.warning("Could not find verification checkbox")
            
            # Wait for Cloudflare to complete processing
            max_wait_time = 60  # Extended to 60 seconds
            wait_interval = 5
            total_waited = 0
            
            while total_waited < max_wait_time:
                await asyncio.sleep(wait_interval)
                total_waited += wait_interval
                
                current_source = driver.page_source.lower()
                
                # Check if Cloudflare protection is gone
                if not any(indicator in current_source for indicator in [
                    'cloudflare', 'unusual traffic', 'verify you are human', 'checking your browser'
                ]):
                    logger.info(f"Cloudflare protection cleared after {total_waited} seconds")
                    return
                
                # Check if we're on a product page (success)
                if any(indicator in current_source for indicator in [
                    'customer reviews', 'rating', 'price', 'add to cart'
                ]):
                    logger.info("Successfully bypassed Cloudflare protection")
                    return
                
                logger.info(f"Still waiting for Cloudflare... ({total_waited}/{max_wait_time}s)")
            
            # If we're still blocked, try refreshing the page
            logger.warning("Cloudflare protection still active, trying page refresh...")
            driver.refresh()
            await asyncio.sleep(15)
            
            # Final check
            final_source = driver.page_source.lower()
            if any(indicator in final_source for indicator in [
                'cloudflare', 'unusual traffic', 'verify you are human'
            ]):
                logger.error("Failed to bypass Cloudflare protection after all attempts")
                raise Exception("Cloudflare protection could not be bypassed")
            else:
                logger.info("Cloudflare protection bypassed after page refresh")
                
        except Exception as e:
            logger.error(f"Error handling Cloudflare protection: {e}")
            raise

    def _extract_reviews_from_json_simple(self, driver: webdriver.Chrome) -> List[Dict[str, Any]]:
        """Simplified JSON extraction focusing on most common patterns"""
        reviews = []
        
        try:
            page_source = driver.page_source
            
            # Look for the most common JSON patterns first
            simple_patterns = [
                r'"Comments":\s*"([^"]+)".*?"Rating":\s*(\d+).*?"NickName":\s*"([^"]+)"',
                r'"Rating":\s*(\d+).*?"NickName":\s*"([^"]+)".*?"Comments":\s*"([^"]+)"',
            ]
            
            for pattern in simple_patterns:
                matches = re.finditer(pattern, page_source, re.DOTALL)
                for match in matches:
                    try:
                        if pattern.startswith('"Comments"'):
                            comment, rating, nickname = match.groups()
                        else:
                            rating, nickname, comment = match.groups()
                        
                        review_data = {
                            "reviewer_name": nickname or "Anonymous",
                            "rating": int(rating) if rating else None,
                            "title": "",
                            "body": comment or "",
                            "date": None,
                            "verified_purchase": False,
                            "product_url": driver.current_url,
                            "scraped_at": datetime.now().isoformat()
                        }
                        
                        if review_data["body"]:  # Only add if we have content
                            reviews.append(review_data)
                            
                    except Exception as e:
                        logger.debug(f"Error parsing JSON match: {e}")
                        continue
                
                if reviews:  # If we found reviews with this pattern, use them
                    logger.info(f"Found {len(reviews)} reviews using simple JSON pattern")
                    return reviews
        
        except Exception as e:
            logger.debug(f"Simple JSON extraction failed: {e}")
        
        return reviews
    
    def _extract_reviews_from_json(self, driver: webdriver.Chrome) -> List[Dict[str, Any]]:
        """Extract reviews from Newegg's JSON data embedded in the page"""
        reviews = []
        
        try:
            # Get page source and look for JSON data
            page_source = driver.page_source
            
            # Look for the specific JSON structure that contains review data
            # Newegg embeds review data in various JSON structures
            json_patterns = [
                r'window\.__initialState__\s*=\s*({.*?});',
                r'"ReviewList":\s*(\[.*?\])',
                r'"CustomerReviews":\s*(\[.*?\])',
                r'"Reviews":\s*(\[.*?\])',
                r'reviewData\s*=\s*({.*?});',
                r'reviews:\s*(\[.*?\])',
                # Look for the complete nested structure we saw in the debug output
                r'(\[{[^}]*"Rating":[^}]*"NickName":[^}]*"Comments":[^}]*}[^\]]*\])',
                r'(\[{[^}]*"Comments":[^}]*"Rating":[^}]*"NickName":[^}]*}[^\]]*\])',
            ]
            
            for pattern in json_patterns:
                try:
                    matches = re.finditer(pattern, page_source, re.DOTALL)
                    for match in matches:
                        json_str = match.group(1)
                        
                        # Try to parse as array first (direct review list)
                        if json_str.strip().startswith('['):
                            try:
                                data = json.loads(json_str)
                                if isinstance(data, list) and len(data) > 0:
                                    # Check if this looks like review data
                                    first_item = data[0]
                                    if isinstance(first_item, dict) and any(key in first_item for key in ['Rating', 'Comments', 'NickName', 'Title']):
                                        logger.info(f"Found {len(data)} reviews in direct JSON array using pattern: {pattern[:50]}...")
                                        return self._parse_json_reviews(data)
                            except json.JSONDecodeError:
                                continue
                        
                        # Try to parse as object and find nested review data
                        else:
                            try:
                                data = json.loads(json_str)
                                review_data = self._find_review_data_in_json(data)
                                if review_data and len(review_data) > 0:
                                    logger.info(f"Found {len(review_data)} reviews in nested JSON using pattern: {pattern[:50]}...")
                                    return self._parse_json_reviews(review_data)
                            except json.JSONDecodeError:
                                continue
                                
                except Exception as e:
                    logger.debug(f"Error processing pattern {pattern[:30]}: {e}")
                    continue
            
            # If regex patterns don't work, try to find script tags and look for review objects
            soup = BeautifulSoup(page_source, 'html.parser')
            script_tags = soup.find_all('script', string=lambda text: text and ('Review' in text or 'rating' in text or 'Comments' in text))
            
            for script in script_tags[:10]:  # Limit to first 10 script tags
                script_content = script.string
                if script_content and len(script_content) > 1000:  # Only process substantial scripts
                    # Look for review arrays in the script content
                    review_patterns = [
                        r'\[{[^}]*"Rating":\s*\d+[^}]*"Comments":[^}]*}[^\]]*\]',
                        r'\[{[^}]*"NickName":[^}]*"Rating":[^}]*"Comments":[^}]*}[^\]]*\]',
                        r'\[{[^}]*"Title":[^}]*"Rating":[^}]*}[^\]]*\]'
                    ]
                    
                    for pattern in review_patterns:
                        matches = re.findall(pattern, script_content, re.DOTALL)
                        for match in matches:
                            try:
                                review_data = json.loads(match)
                                if isinstance(review_data, list) and len(review_data) > 0:
                                    # Verify this looks like review data
                                    first_review = review_data[0]
                                    if isinstance(first_review, dict) and any(key in first_review for key in ['Rating', 'Comments', 'NickName']):
                                        logger.info(f"Found {len(review_data)} reviews in script tag using pattern: {pattern[:50]}...")
                                        return self._parse_json_reviews(review_data)
                            except json.JSONDecodeError:
                                continue
            
            logger.info("No JSON review data found using enhanced patterns")
            return reviews
            
        except Exception as e:
            logger.error(f"Error extracting reviews from JSON: {e}")
            return reviews
    
    def _find_review_data_in_json(self, data: Dict) -> Optional[List[Dict]]:
        """Recursively search for review data in JSON structure"""
        if isinstance(data, dict):
            # Check common review data keys - expanded list
            for key in ['ReviewList', 'CustomerReviews', 'Reviews', 'reviewList', 'reviews', 'Review', 'ProductReviews', 'ItemReviews']:
                if key in data and isinstance(data[key], list) and len(data[key]) > 0:
                    # Verify this looks like review data
                    first_item = data[key][0]
                    if isinstance(first_item, dict) and any(review_key in first_item for review_key in ['Rating', 'Comments', 'NickName', 'Title', 'reviewer']):
                        return data[key]
            
            # Check for nested structures like ReviewInfo, ProductDetail, etc.
            for key in ['ReviewInfo', 'ProductDetail', 'ItemDetail', 'ReviewData', 'ProductReviews', 'data', 'content']:
                if key in data and isinstance(data[key], dict):
                    result = self._find_review_data_in_json(data[key])
                    if result:
                        return result
            
            # Check nested structures
            for value in data.values():
                if isinstance(value, (dict, list)):
                    result = self._find_review_data_in_json(value)
                    if result:
                        return result
                        
        elif isinstance(data, list):
            # Check if this list itself contains review objects
            if len(data) > 0:
                first_item = data[0]
                if isinstance(first_item, dict) and any(key in first_item for key in ['Rating', 'Comments', 'NickName', 'Title', 'reviewer']):
                    return data
            
            # Search within list items
            for item in data:
                if isinstance(item, (dict, list)):
                    result = self._find_review_data_in_json(item)
                    if result:
                        return result
        
        return None
    
    def _parse_json_reviews(self, review_data: List[Dict]) -> List[Dict[str, Any]]:
        """Parse JSON review data into standardized format"""
        reviews = []
        
        for review_json in review_data:
            try:
                review = {}
                
                # Extract reviewer name
                reviewer_name = (
                    review_json.get('NickName') or 
                    review_json.get('DisplayName') or 
                    review_json.get('reviewer_name') or 
                    review_json.get('author') or 
                    'Anonymous'
                )
                review['reviewer_name'] = self.utils.clean_text(reviewer_name)
                
                # Extract rating
                rating = review_json.get('Rating') or review_json.get('rating')
                if rating is not None:
                    try:
                        review['rating'] = float(rating)
                    except (ValueError, TypeError):
                        review['rating'] = None
                else:
                    review['rating'] = None
                
                # Extract title
                title = review_json.get('Title') or review_json.get('title') or review_json.get('headline')
                review['title'] = self.utils.clean_text(title) if title else None
                
                # Extract body/comments
                body = (
                    review_json.get('Comments') or 
                    review_json.get('body') or 
                    review_json.get('text') or 
                    review_json.get('content') or
                    review_json.get('review_text')
                )
                review['body'] = self.utils.clean_text(body) if body else None
                
                # Extract date
                date_str = (
                    review_json.get('InDate') or 
                    review_json.get('date') or 
                    review_json.get('review_date') or
                    review_json.get('created_at')
                )
                if date_str:
                    review['date'] = self._parse_review_date(date_str)
                else:
                    review['date'] = None
                
                # Extract verified purchase info
                verified = review_json.get('HasPurchased', False) or review_json.get('verified_purchase', False)
                review['verified_purchase'] = bool(verified)
                
                # Extract helpful count
                helpful_count = (
                    review_json.get('TotalVoting') or 
                    review_json.get('TotalConsented') or
                    review_json.get('helpful_count') or
                    review_json.get('upvotes')
                )
                if helpful_count is not None:
                    try:
                        review['helpful_count'] = int(helpful_count)
                    except (ValueError, TypeError):
                        review['helpful_count'] = None
                else:
                    review['helpful_count'] = None
                
                # Extract pros and cons if available
                pros = review_json.get('Pros')
                cons = review_json.get('Cons')
                if pros or cons:
                    additional_info = []
                    if pros:
                        additional_info.append(f"Pros: {pros}")
                    if cons:
                        additional_info.append(f"Cons: {cons}")
                    
                    # Append to body if it exists, otherwise create new body
                    if review['body']:
                        review['body'] += f"\n\n{' | '.join(additional_info)}"
                    else:
                        review['body'] = ' | '.join(additional_info)
                
                # Only add review if it has essential content
                if review.get('body') or review.get('title'):
                    reviews.append(review)
                    logger.debug(f"Parsed review from {review.get('reviewer_name', 'Unknown')}: {review.get('title', 'No title')}")
                
            except Exception as e:
                logger.debug(f"Error parsing individual review: {e}")
                continue
        
        logger.info(f"Successfully parsed {len(reviews)} reviews from JSON data")
        return reviews
    
    async def _extract_reviews_selenium(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> List[Dict[str, Any]]:
        """Extract reviews using simple, robust approach focusing on core elements"""
        reviews = []
        
        try:
            # Check for Cloudflare protection first
            page_source = driver.page_source
            if any(indicator in page_source.lower() for indicator in [
                'cloudflare', 'unusual traffic', 'verify you are human', 'checking your browser'
            ]):
                logger.warning("Cloudflare protection detected, attempting to handle...")
                await self._handle_cloudflare_protection(driver)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # Try JSON-LD extraction first (most reliable)
            logger.info("Attempting JSON-LD extraction...")
            json_ld_reviews = self._extract_reviews_from_json_ld(soup)
            if json_ld_reviews:
                logger.info(f"Successfully extracted {len(json_ld_reviews)} reviews from JSON-LD")
                return json_ld_reviews
            
            # Try embedded JSON extraction
            logger.info("Attempting embedded JSON extraction...")
            json_reviews = self._extract_reviews_from_json_simple(driver)
            if json_reviews:
                logger.info(f"Successfully extracted {len(json_reviews)} reviews from embedded JSON")
                return json_reviews
            
            # Navigate to reviews section
            logger.info("Navigating to reviews section...")
            await self._navigate_to_reviews_section(driver)
            
            # Get updated page source
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            logger.info(f"Current URL: {driver.current_url}")
            
            # Use multiple simple strategies to find reviews
            potential_reviews = []
            
            # Strategy 1: Look for elements with "comment" class (Newegg's actual structure)
            comment_elements = soup.find_all('div', class_='comments')
            if comment_elements:
                logger.info(f"Found {len(comment_elements)} elements with 'comments' class")
                potential_reviews.extend(comment_elements)
            
            # Strategy 2: Look for any div containing review-like text
            all_divs = soup.find_all('div')
            review_indicators = ['verified owner', 'pros:', 'cons:', 'overall review:', 'rating']
            
            for div in all_divs:
                div_text = div.get_text().lower() if div.get_text() else ""
                if any(indicator in div_text for indicator in review_indicators):
                    # Check if this looks like an individual review (not summary)
                    if len(div_text) > 100 and 'summary' not in div_text:
                        potential_reviews.append(div)
            
            # Remove duplicates
            unique_reviews = []
            seen_texts = set()
            for review in potential_reviews:
                text = review.get_text()[:200] if review.get_text() else ""
                if text and text not in seen_texts:
                    unique_reviews.append(review)
                    seen_texts.add(text)
            
            logger.info(f"Found {len(unique_reviews)} potential review elements")
            
            # Parse each review element
            for i, review_element in enumerate(unique_reviews[:50]):  # Limit to 50 reviews
                try:
                    review_data = self._extract_review_data_simple(review_element, driver.current_url)
                    if review_data and (review_data.get('body') or review_data.get('title')):
                        reviews.append(review_data)
                        logger.debug(f"Successfully parsed review {len(reviews)}")
                    else:
                        logger.debug(f"Skipped review {i+1} - insufficient data")
                except Exception as e:
                    logger.debug(f"Error parsing review {i+1}: {e}")
            
            logger.info(f"Successfully extracted {len(reviews)} reviews")
            
        except Exception as e:
            logger.error(f"Review extraction failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        return reviews
    
    async def _extract_reviews_requests(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """Extract reviews using requests (limited functionality)"""
        reviews = []
        
        try:
            review_selectors = SELECTORS["reviews"]["review_item"]
            review_elements = self.utils.safe_find_elements(soup, review_selectors)
            
            for review_element in review_elements:
                try:
                    review_data = self._parse_review_element(review_element)
                    if review_data:
                        reviews.append(review_data)
                except Exception as e:
                    logger.warning(f"Failed to parse review: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Static review extraction failed: {e}")
        
        return reviews
    
    async def _navigate_to_reviews_section(self, driver: webdriver.Chrome):
        """Navigate to reviews section of the page"""
        try:
            success = False
            
            logger.info("Looking for reviews tab...")
            
            # Method 1: Very specific JavaScript search for "Reviews (" pattern
            if not success:
                try:
                    logger.debug("Trying JavaScript search for Reviews tab with count...")
                    elements = driver.execute_script("""
                        var links = Array.from(document.querySelectorAll('a, button, div[role="tab"]'));
                        var reviewsLinks = links.filter(function(el) {
                            var text = el.textContent || el.innerText || '';
                            return text.toLowerCase().includes('reviews') && 
                                   text.includes('(') && 
                                   !text.toLowerCase().includes('specs') &&
                                   !text.toLowerCase().includes('q &');
                        });
                        console.log('Found review elements:', reviewsLinks.map(el => el.textContent));
                        return reviewsLinks;
                    """)
                    
                    if elements:
                        for element in elements:
                            element_text = driver.execute_script("return arguments[0].textContent || arguments[0].innerText", element)
                            logger.info(f"Found potential reviews tab: '{element_text}'")
                            
                            # Click the first one that looks like "Reviews (number)"
                            if 'reviews' in element_text.lower() and '(' in element_text:
                                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                                await asyncio.sleep(1)
                                driver.execute_script("arguments[0].click();", element)
                                success = True
                                logger.info(f"Clicked reviews tab: '{element_text}'")
                                break
                except Exception as e:
                    logger.debug(f"JavaScript reviews tab search failed: {e}")
            
            # Method 2: XPath specifically looking for "Reviews (" pattern
            if not success:
                try:
                    logger.debug("Trying XPath search for Reviews tab...")
                    xpath_patterns = [
                        "//a[contains(text(), 'Reviews') and contains(text(), '(') and not(contains(text(), 'Specs'))]",
                        "//div[@role='tab'][contains(text(), 'Reviews')]",
                        "//button[contains(text(), 'Reviews') and contains(text(), '(')]",
                        "//*[contains(@class, 'tab')][contains(text(), 'Reviews')]"
                    ]
                    
                    for xpath in xpath_patterns:
                        try:
                            reviews_element = driver.find_element(By.XPATH, xpath)
                            if reviews_element and reviews_element.is_displayed():
                                element_text = reviews_element.text
                                logger.info(f"Found reviews tab via XPath: '{element_text}'")
                                
                                driver.execute_script("arguments[0].scrollIntoView(true);", reviews_element)
                                await asyncio.sleep(1)
                                driver.execute_script("arguments[0].click();", reviews_element)
                                success = True
                                logger.info(f"Clicked reviews tab using XPath: '{element_text}'")
                                break
                        except (NoSuchElementException, Exception):
                            continue
                            
                except Exception as e:
                    logger.debug(f"XPath reviews tab search failed: {e}")
            
            # Method 3: Try CSS selectors for reviews tab from config
            if not success:
                logger.debug("Trying configured CSS selectors...")
                tab_selectors = SELECTORS["reviews"]["reviews_tab"]
                for selector in tab_selectors:
                    try:
                        element = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        
                        # Check if this is actually the reviews tab
                        element_text = element.text or ""
                        if 'reviews' not in element_text.lower():
                            logger.debug(f"Skipping element that doesn't contain 'reviews': '{element_text}'")
                            continue
                        
                        # Scroll to element first
                        driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        await asyncio.sleep(1)
                        
                        # Use JavaScript click to avoid interception
                        driver.execute_script("arguments[0].click();", element)
                        success = True
                        logger.info(f"Clicked reviews tab using selector: {selector}, text: '{element_text}'")
                        break
                        
                    except (TimeoutException, NoSuchElementException) as e:
                        logger.debug(f"Selector {selector} failed: {e}")
                        continue
            
            if success:
                # Wait for reviews content to load
                logger.info("Reviews tab clicked, waiting for content to load...")
                await asyncio.sleep(5)
                
                # Wait for reviews container to appear using config selectors
                try:
                    container_selectors = SELECTORS["reviews"]["container"]
                    for selector in container_selectors:
                        try:
                            WebDriverWait(driver, 10).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            logger.info(f"Reviews section loaded successfully with selector: {selector}")
                            return
                        except TimeoutException:
                            continue
                    
                    logger.warning("Reviews container not found after tab click")
                except Exception as e:
                    logger.warning(f"Error waiting for reviews container: {e}")
            else:
                logger.warning("Could not find reviews tab to click")
                # Try scrolling to find reviews section
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                await asyncio.sleep(2)
                
        except Exception as e:
            logger.debug(f"Could not navigate to reviews section: {e}")
            # Fallback: just scroll down to look for reviews
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                await asyncio.sleep(2)
            except Exception:
                pass
    
    async def _load_more_reviews(self, driver: webdriver.Chrome, max_loads: int = 5):
        """Load more reviews by clicking load more button"""
        try:
            loads_performed = 0
            
            while loads_performed < max_loads:
                # Look for load more button
                load_more_selectors = SELECTORS["reviews"]["load_more"]
                
                load_more_clicked = False
                for selector in load_more_selectors:
                    try:
                        element = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        
                        # Scroll to element and click
                        driver.execute_script("arguments[0].scrollIntoView(true);", element)
                        await asyncio.sleep(1)
                        driver.execute_script("arguments[0].click();", element)
                        
                        load_more_clicked = True
                        loads_performed += 1
                        
                        logger.info(f"Loaded more reviews (attempt {loads_performed})")
                        
                        # Wait for new content to load
                        await asyncio.sleep(3)
                        break
                        
                    except (TimeoutException, NoSuchElementException):
                        continue
                    except Exception as e:
                        logger.debug(f"Error clicking load more: {e}")
                        continue
                
                if not load_more_clicked:
                    logger.info("No more 'load more' button found")
                    break
                    
        except Exception as e:
            logger.debug(f"Load more reviews failed: {e}")
    
    def _parse_review_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats commonly found in reviews"""
        if not date_str:
            return None
        
        try:
            # Handle common formats manually using standard library
            import re
            from datetime import datetime
            
            # ISO format: 2023-12-25
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                return date_str
            
            # MM/DD/YYYY or MM-DD-YYYY
            match = re.match(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', date_str)
            if match:
                month, day, year = match.groups()
                try:
                    # Validate the date
                    datetime(int(year), int(month), int(day))
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except ValueError:
                    pass
            
            # Month DD, YYYY
            months = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12',
                'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
                'jun': '06', 'jul': '07', 'aug': '08', 'sep': '09',
                'oct': '10', 'nov': '11', 'dec': '12'
            }
            
            for month_name, month_num in months.items():
                pattern = rf'{month_name}\s+(\d{{1,2}}),?\s+(\d{{4}})'
                match = re.search(pattern, date_str.lower())
                if match:
                    day, year = match.groups()
                    try:
                        # Validate the date
                        datetime(int(year), int(month_num), int(day))
                        return f"{year}-{month_num}-{day.zfill(2)}"
                    except ValueError:
                        pass
            
            # Just return as is if we can't parse
            return date_str
            
        except Exception:
            return date_str
    
    def _extract_review_data_simple(self, review_element, product_url: str) -> Optional[Dict[str, Any]]:
        """Simple, robust review data extraction focusing on core elements"""
        try:
            review_text = review_element.get_text() if review_element else ""
            if not review_text or len(review_text) < 20:
                return None
            
            # Initialize review data
            review_data = {
                "reviewer_name": "Anonymous",
                "rating": None,
                "title": "",
                "body": "",
                "date": None,
                "verified_purchase": False,
                "product_url": product_url,
                "scraped_at": datetime.now().isoformat()
            }
            
            # Extract reviewer name - look for common patterns
            name_patterns = [
                r'By\s+([A-Za-z\s\.]+?)(?:\s|$)',
                r'([A-Za-z\s\.]+?)\s+(?:Verified|verified|Owner|owner)',
                r'^([A-Za-z\s\.]+?)(?:\s{2,}|\n)',
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, review_text)
                if match:
                    name = match.group(1).strip()
                    if len(name) > 1 and len(name) < 50:  # Reasonable name length
                        review_data["reviewer_name"] = name
                        break
            
            # Extract rating - look for star patterns or numbers
            rating_patterns = [
                r'(\d+)(?:\s*out\s*of\s*5|\s*\/\s*5|\s*stars?)',
                r'Rating:\s*(\d+)',
                r'(\d+)\s*star'
            ]
            
            for pattern in rating_patterns:
                match = re.search(pattern, review_text, re.IGNORECASE)
                if match:
                    try:
                        rating = int(match.group(1))
                        if 1 <= rating <= 5:
                            review_data["rating"] = rating
                            break
                    except ValueError:
                        continue
            
            # Extract main review content
            # Split text into lines and look for meaningful content
            lines = [line.strip() for line in review_text.split('\n') if line.strip()]
            
            # Try to identify title and body
            content_lines = []
            for line in lines:
                # Skip lines that look like metadata
                if any(keyword in line.lower() for keyword in ['verified', 'by ', 'rating', 'stars', 'out of']):
                    continue
                if len(line) > 10:  # Meaningful content
                    content_lines.append(line)
            
            if content_lines:
                # First substantial line might be title, rest is body
                if len(content_lines) > 1:
                    potential_title = content_lines[0]
                    if len(potential_title) < 100:  # Reasonable title length
                        review_data["title"] = potential_title
                        review_data["body"] = ' '.join(content_lines[1:])
                    else:
                        review_data["body"] = ' '.join(content_lines)
                else:
                    review_data["body"] = content_lines[0]
            
            # Look for verified purchase indicators
            if any(keyword in review_text.lower() for keyword in ['verified', 'owner', 'purchased']):
                review_data["verified_purchase"] = True
            
            # Extract date if possible
            date_patterns = [
                r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                r'(\w+\s+\d{1,2},?\s+\d{4})',
                r'(\d{4}-\d{2}-\d{2})'
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, review_text)
                if match:
                    try:
                        date_str = match.group(1)
                        # Simple date parsing
                        review_data["date"] = date_str
                        break
                    except:
                        continue
            
            # Only return if we have substantial content
            if review_data["body"] or review_data["title"]:
                return review_data
            
        except Exception as e:
            logger.debug(f"Error in simple review extraction: {e}")
        
        return None
    
    def _parse_review_element(self, review_element) -> Optional[Dict[str, Any]]:
        """Parse individual review element"""
        try:
            review = ReviewInfo()
            
            # Extract reviewer name
            name_selectors = SELECTORS["reviews"]["reviewer_name"]
            review.reviewer_name = self._extract_text_from_element(review_element, name_selectors)
            
            # Extract rating
            rating_selectors = SELECTORS["reviews"]["rating"]
            rating_text = self._extract_text_from_element(review_element, rating_selectors)
            review.rating = self.utils.extract_rating(rating_text) if rating_text else None
            
            # Extract title
            title_selectors = SELECTORS["reviews"]["title"]
            review.title = self._extract_text_from_element(review_element, title_selectors)
            
            # Extract body
            body_selectors = SELECTORS["reviews"]["body"]
            review.body = self._extract_text_from_element(review_element, body_selectors)
            
            # Extract date
            date_selectors = SELECTORS["reviews"]["date"]
            date_text = self._extract_text_from_element(review_element, date_selectors)
            review.date = self.utils.parse_date(date_text) if date_text else None
            
            # Extract verified purchase status
            verified_selectors = SELECTORS["reviews"]["verified"]
            verified_text = self._extract_text_from_element(review_element, verified_selectors)
            review.verified_purchase = self.utils.is_verified_purchase(verified_text or "")
            
            # Only return if we have essential data
            if review.reviewer_name or review.body:
                return {
                    "reviewer_name": review.reviewer_name,
                    "rating": review.rating,
                    "title": review.title,
                    "body": review.body,
                    "date": review.date.isoformat() if review.date else None,
                    "verified_purchase": review.verified_purchase,
                    "review_id": review.review_id,
                    "scraped_at": review.scraped_at.isoformat()
                }
            
        except Exception as e:
            logger.warning(f"Error parsing review element: {e}")
        
        return None
    
    def _extract_text_from_element(self, parent_element, selectors: List[str]) -> Optional[str]:
        """Extract text from element using CSS selectors"""
        for selector in selectors:
            try:
                element = parent_element.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    return self.utils.clean_text(text) if text else None
            except Exception:
                continue
        return None
    
    def _looks_like_individual_review(self, element) -> bool:
        """Check if an element looks like an individual review (not AI summary)"""
        try:
            text = element.get_text().lower()
            classes = ' '.join(element.get('class', [])).lower()
            
            # Positive indicators of individual reviews
            individual_review_indicators = [
                'verified owner', 'verified purchase', 'pros:', 'cons:', 
                'overall review:', 'helpful', 'anonymous', 'customer',
                'rating', 'star', 'egg', 'recommend', 'would buy'
            ]
            
            # Negative indicators (AI summary content)
            summary_indicators = [
                'what reviewers are saying', 'summary', 'ai generated',
                'compatible with older', 'good performance', 'runs cool',
                'includes a cooler', 'easy to install', 'stock cooler'
            ]
            
            # Check for individual review patterns
            has_individual_indicators = any(indicator in text for indicator in individual_review_indicators)
            has_summary_indicators = any(indicator in text for indicator in summary_indicators)
            
            # Look for structured review content
            has_structured_content = (
                ('pros:' in text and 'cons:' in text) or
                ('overall review:' in text) or
                ('verified' in text) or
                (len(text) > 100 and any(word in text for word in ['bought', 'purchased', 'using', 'installed']))
            )
            
            return has_individual_indicators or (has_structured_content and not has_summary_indicators)
            
        except Exception:
            return False
    
    def _parse_review_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats from Newegg reviews"""
        if not date_str:
            return None
        
        try:
            # Handle ISO format (e.g., "2022-10-28T11:59:08.95")
            if 'T' in date_str:
                # Remove timezone info if present and parse
                date_str = date_str.split('T')[0] + 'T' + date_str.split('T')[1].split('-')[0].split('+')[0]
                return datetime.fromisoformat(date_str.rstrip('Z'))
            
            # Handle other common formats
            date_formats = [
                "%Y-%m-%d",
                "%m/%d/%Y",
                "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S",
                "%m/%d/%Y %H:%M:%S"
            ]
            
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            logger.debug(f"Could not parse date: {date_str}")
            return None
            
        except Exception as e:
            logger.debug(f"Error parsing date '{date_str}': {e}")
            return None
    
    async def scrape_product_and_reviews(self, url: str, max_reviews: int = 100) -> Dict[str, Any]:
        """Main method to scrape product and reviews"""
        logger.info(f"Starting to scrape Newegg product: {url}")
        
        try:
            result = await self.scrape(url, method="fallback")
            
            if result.success:
                data = result.data
                
                # Limit reviews if requested
                if data.get("reviews") and len(data["reviews"]) > max_reviews:
                    data["reviews"] = data["reviews"][:max_reviews]
                    data["total_reviews_found"] = len(data["reviews"])
                
                logger.info(f"Successfully scraped product with {len(data.get('reviews', []))} reviews")
                return data
            else:
                logger.error(f"Scraping failed: {result.error}")
                return {"error": result.error}
                
        except Exception as e:
            logger.error(f"Scraping process failed: {e}")
            return {"error": str(e)}
