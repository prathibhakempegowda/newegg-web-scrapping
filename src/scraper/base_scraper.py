"""
Base scraper class with common functionality
"""
import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime

import aiohttp
import cloudscraper
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

from .config import (
    CHROME_OPTIONS, DEFAULT_HEADERS, SELENIUM_TIMEOUT, 
    SELENIUM_IMPLICIT_WAIT, SELENIUM_EXPLICIT_WAIT,
    MAX_RETRIES, RETRY_DELAY, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX
)
from .utils import ScrapingUtils, RetryHelper, RateLimiter

logger = logging.getLogger(__name__)

@dataclass
class ScrapingResult:
    """Data class for scraping results"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: datetime = None
    method_used: str = ""
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

class BaseScraper(ABC):
    """Base class for web scrapers with multiple strategies"""
    
    def __init__(self):
        self.utils = ScrapingUtils()
        self.retry_helper = RetryHelper()
        self.rate_limiter = RateLimiter(max_calls=30, time_window=60.0)  # 30 calls per minute
        self.session = None
        self.driver = None
        self.cloudscraper = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.cleanup()
    
    async def initialize(self):
        """Initialize scraping tools"""
        try:
            # Initialize aiohttp session
            connector = aiohttp.TCPConnector(limit=10, limit_per_host=3)
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers=DEFAULT_HEADERS
            )
            
            # Initialize cloudscraper
            self.cloudscraper = cloudscraper.create_scraper(
                browser={
                    'browser': 'chrome',
                    'platform': 'darwin',  # macOS
                    'mobile': False
                }
            )
            
            logger.info("Scraper initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize scraper: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
        
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")
        
        logger.info("Scraper cleanup completed")
    
    async def _handle_cloudflare_protection(self):
        """Detect and handle Cloudflare protection with enhanced detection"""
        try:
            # Enhanced Cloudflare indicators
            cloudflare_indicators = [
                "checking your browser before accessing",
                "please enable cookies and reload the page",
                "ddos protection by cloudflare",
                "cf-browser-verification",
                "challenge-platform",
                "just a moment",
                "cloudflare",
                "unusual traffic",
                "verify you are human",
                "checking your browser",
                "ray id"
            ]
            
            page_text = self.driver.page_source.lower()
            page_title = self.driver.title.lower()
            current_url = self.driver.current_url.lower()
            
            # Check both page content, title, and URL
            cloudflare_detected = (
                any(indicator in page_text for indicator in cloudflare_indicators) or
                "just a moment" in page_title or
                "cloudflare" in current_url
            )
            
            if cloudflare_detected:
                logger.warning("Cloudflare protection detected, implementing enhanced bypass...")
                
                # Check for interactive challenges
                try:
                    # Look for verification checkbox
                    checkbox = self.driver.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"]')
                    if checkbox:
                        logger.info("Found verification checkbox, attempting to click...")
                        checkbox[0].click()
                        await asyncio.sleep(5)
                except Exception as e:
                    logger.debug(f"No interactive challenge found: {e}")
                
                # Extended wait for Cloudflare to resolve (up to 60 seconds)
                max_wait = 60
                waited = 0
                
                while waited < max_wait:
                    await asyncio.sleep(5)  # Wait 5 seconds at a time
                    waited += 5
                    
                    # Check if challenge is resolved
                    current_text = self.driver.page_source.lower()
                    current_title = self.driver.title.lower()
                    current_url = self.driver.current_url.lower()
                    
                    cloudflare_still_active = (
                        any(indicator in current_text for indicator in cloudflare_indicators) or
                        "just a moment" in current_title or
                        "cloudflare" in current_url
                    )
                    
                    # Also check for signs of successful page load
                    success_indicators = [
                        "customer reviews", "add to cart", "price", "rating", 
                        "product", "newegg", "specifications"
                    ]
                    page_loaded_successfully = any(indicator in current_text for indicator in success_indicators)
                    
                    if not cloudflare_still_active or page_loaded_successfully:
                        logger.info(f"Cloudflare protection bypassed successfully after {waited}s")
                        # Wait a bit more for page to fully stabilize
                        await asyncio.sleep(5)
                        return
                    
                    logger.info(f"Still waiting for Cloudflare bypass... ({waited}/{max_wait}s)")
                
                # If still blocked, try refreshing the page once
                logger.warning("Cloudflare protection still active, attempting page refresh...")
                self.driver.refresh()
                await asyncio.sleep(15)
                
                # Final check after refresh
                final_text = self.driver.page_source.lower()
                final_title = self.driver.title.lower()
                
                if any(indicator in final_text for indicator in cloudflare_indicators) or "just a moment" in final_title:
                    logger.error("Failed to bypass Cloudflare protection after refresh")
                    raise Exception("Cloudflare protection could not be bypassed")
                else:
                    logger.info("Cloudflare protection bypassed after page refresh")
            
        except Exception as e:
            logger.error(f"Error handling Cloudflare protection: {e}")
            raise
    
    def _setup_selenium_driver(self) -> webdriver.Chrome:
        """Setup Selenium Chrome driver with optimal configuration for Cloudflare bypass"""
        try:
            chrome_options = Options()
            
            # Add all chrome options
            for option in CHROME_OPTIONS:
                chrome_options.add_argument(option)
            
            # Additional stealth options for Cloudflare bypass
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Disable notifications and popups
            prefs = {
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.images": 1,  # Enable images for better detection
                "profile.default_content_setting_values.media_stream_mic": 2,
                "profile.default_content_setting_values.media_stream_camera": 2,
                "profile.default_content_setting_values.geolocation": 2,
                "profile.default_content_settings.cookies": 1,
            }
            chrome_options.add_experimental_option("prefs", prefs)
            
            # Use webdriver manager to handle driver installation
            service = Service(ChromeDriverManager().install())
            
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set timeouts
            driver.implicitly_wait(SELENIUM_IMPLICIT_WAIT)
            driver.set_page_load_timeout(SELENIUM_TIMEOUT)
            
            # Execute stealth scripts for Cloudflare bypass
            stealth_js = """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            window.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'permissions', {get: () => ({query: () => Promise.resolve({state: 'granted'})})});
            """
            driver.execute_script(stealth_js)
            
            # Set viewport size for better rendering
            driver.set_window_size(1920, 1080)
            
            return driver
            
        except Exception as e:
            logger.error(f"Failed to setup Selenium driver: {e}")
            raise
    
    async def scrape_with_selenium(self, url: str) -> ScrapingResult:
        """Scrape using Selenium WebDriver with Cloudflare handling"""
        try:
            await self.rate_limiter.acquire()
            
            if not self.driver:
                self.driver = self._setup_selenium_driver()
            
            # Set random user agent
            user_agent = self.utils.get_random_user_agent()
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
            
            logger.info(f"Loading page with Selenium: {url}")
            self.driver.get(url)
            
            # Check for and handle Cloudflare protection
            await self._handle_cloudflare_protection()
            
            # Wait for page to load
            await asyncio.sleep(3)
            
            # Wait for specific elements to be present
            try:
                WebDriverWait(self.driver, SELENIUM_EXPLICIT_WAIT).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                logger.warning("Page load timeout, proceeding anyway")
            
            # Get page source and parse
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract data using implementation-specific method
            data = await self._extract_data_selenium(soup, self.driver)
            
            return ScrapingResult(
                success=True,
                data=data,
                method_used="selenium"
            )
            
        except Exception as e:
            logger.error(f"Selenium scraping failed: {e}")
            return ScrapingResult(
                success=False,
                error=str(e),
                method_used="selenium"
            )
    
    async def scrape_with_cloudscraper(self, url: str) -> ScrapingResult:
        """Scrape using cloudscraper (anti-bot protection)"""
        try:
            await self.rate_limiter.acquire()
            await self.utils.async_delay(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            
            headers = DEFAULT_HEADERS.copy()
            headers['User-Agent'] = self.utils.get_random_user_agent()
            
            logger.info(f"Fetching with cloudscraper: {url}")
            response = self.cloudscraper.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            data = await self._extract_data_requests(soup, response)
            
            return ScrapingResult(
                success=True,
                data=data,
                method_used="cloudscraper"
            )
            
        except Exception as e:
            logger.error(f"Cloudscraper scraping failed: {e}")
            return ScrapingResult(
                success=False,
                error=str(e),
                method_used="cloudscraper"
            )
    
    async def scrape_with_aiohttp(self, url: str) -> ScrapingResult:
        """Scrape using aiohttp session"""
        try:
            await self.rate_limiter.acquire()
            await self.utils.async_delay(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
            
            headers = DEFAULT_HEADERS.copy()
            headers['User-Agent'] = self.utils.get_random_user_agent()
            
            logger.info(f"Fetching with aiohttp: {url}")
            async with self.session.get(url, headers=headers) as response:
                response.raise_for_status()
                content = await response.read()
                
                soup = BeautifulSoup(content, 'html.parser')
                data = await self._extract_data_requests(soup, response)
                
                return ScrapingResult(
                    success=True,
                    data=data,
                    method_used="aiohttp"
                )
                
        except Exception as e:
            logger.error(f"Aiohttp scraping failed: {e}")
            return ScrapingResult(
                success=False,
                error=str(e),
                method_used="aiohttp"
            )
    
    async def scrape_with_fallback(self, url: str) -> ScrapingResult:
        """Try multiple scraping methods with fallback"""
        methods = [
            ("selenium", self.scrape_with_selenium),
            ("cloudscraper", self.scrape_with_cloudscraper),
            ("aiohttp", self.scrape_with_aiohttp)
        ]
        
        last_error = None
        
        for method_name, method_func in methods:
            try:
                logger.info(f"Attempting to scrape with {method_name}")
                result = await self.retry_helper.async_retry(
                    lambda m=method_func: m(url),
                    max_retries=2,
                    base_delay=RETRY_DELAY
                )
                
                if result.success:
                    logger.info(f"Successfully scraped with {method_name}")
                    return result
                else:
                    last_error = result.error
                    logger.warning(f"{method_name} failed: {result.error}")
                    
            except Exception as e:
                last_error = str(e)
                logger.warning(f"{method_name} method failed with exception: {e}")
                continue
        
        return ScrapingResult(
            success=False,
            error=f"All scraping methods failed. Last error: {last_error}",
            method_used="all_failed"
        )
    
    @abstractmethod
    async def _extract_data_selenium(self, soup: BeautifulSoup, driver: webdriver.Chrome) -> Dict[str, Any]:
        """Extract data using Selenium-specific methods"""
        pass
    
    @abstractmethod
    async def _extract_data_requests(self, soup: BeautifulSoup, response: Any) -> Dict[str, Any]:
        """Extract data using requests/aiohttp methods"""
        pass
    
    async def scrape(self, url: str, method: str = "fallback") -> ScrapingResult:
        """Main scraping method"""
        if not self.utils.validate_url(url):
            return ScrapingResult(
                success=False,
                error=f"Invalid URL: {url}",
                method_used=method
            )
        
        try:
            if method == "selenium":
                return await self.scrape_with_selenium(url)
            elif method == "cloudscraper":
                return await self.scrape_with_cloudscraper(url)
            elif method == "aiohttp":
                return await self.scrape_with_aiohttp(url)
            else:  # fallback
                return await self.scrape_with_fallback(url)
                
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            return ScrapingResult(
                success=False,
                error=str(e),
                method_used=method
            )
    
    def extract_text_safe(self, soup: BeautifulSoup, selectors: List[str]) -> Optional[str]:
        """Safely extract text using multiple selectors"""
        element = self.utils.safe_find_element(soup, selectors)
        if element:
            text = element.get_text(strip=True)
            return self.utils.clean_text(text) if text else None
        return None
    
    def extract_attribute_safe(self, soup: BeautifulSoup, selectors: List[str], attribute: str) -> Optional[str]:
        """Safely extract attribute using multiple selectors"""
        element = self.utils.safe_find_element(soup, selectors)
        if element:
            return element.get(attribute)
        return None
