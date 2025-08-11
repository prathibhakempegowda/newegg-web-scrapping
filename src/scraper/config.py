"""
Configuration settings for the web scraper
"""
import os
from typing import List, Dict, Any

# Base configuration
BASE_URL = "https://www.newegg.com"
TARGET_URL = "https://www.newegg.com/amd-ryzen-7-9000-series-ryzen-7-9800x3d-granite-ridge-zen-5-socket-am5-desktop-cpu-processor/p/N82E16819113877"

# Rate limiting and delays
REQUEST_DELAY_MIN = 2.0  # Minimum delay between requests (seconds)
REQUEST_DELAY_MAX = 4.0  # Maximum delay between requests (seconds)
MAX_CONCURRENT_REQUESTS = 3  # Maximum concurrent requests
MAX_RETRIES = 3  # Maximum retry attempts
RETRY_DELAY = 5.0  # Delay between retries (seconds)

# Selenium configuration
SELENIUM_TIMEOUT = 30  # Page load timeout
SELENIUM_IMPLICIT_WAIT = 10  # Implicit wait time
SELENIUM_EXPLICIT_WAIT = 20  # Explicit wait time

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
]

# Database configuration
DATABASE_PATH = "data/newegg_scraper.db"
DUCKDB_PATH = "data/analytics.duckdb"

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "logs/scraper.log"

# CSS Selectors for Newegg
SELECTORS = {
    "product": {
        "title": [
            "h1.product-title",
            ".product-wrap h1",
            "[data-testid='product-title']",
            ".product-view-title h1",
            "h1"  # Fallback
        ],
        "brand": [
            ".product-brand a",
            ".product-brand",
            "[data-testid='product-brand']",
            ".brand-name",
            ".product-manufacturer"
        ],
        "price": [
            ".price-current",
            ".product-price .price-current",
            "[data-testid='product-price']",
            ".price-now",
            ".price-map",
            ".price"
        ],
        "rating": [
            ".rating-eggs",
            ".product-rating .rating",
            "[data-testid='product-rating']",
            ".overall-rating",
            ".rating-summary .rating"
        ],
        "review_count": [
            ".item-rating-num",
            ".rating-text",
            "[data-testid='review-count']",
            ".review-summary",
            ".customer-review-count"
        ],
        "description": [
            ".product-bullets",
            ".product-overview",
            "[data-testid='product-description']",
            ".product-details",
            ".product-summary"
        ]
    },
    "reviews": {
        "reviews_tab": [
            "a[href*='#Reviews']",
            "a[href*='reviews']:not([href*='specs'])",
            ".tab-nav a:contains('Reviews')",
            "a:contains('Reviews (')",
            ".nav-link[href*='reviews']",
            "#Reviews-tab",
            ".product-tabs a[href*='Reviews']"
        ],
        "container": [
            ".reviews-section",
            "#reviews-section", 
            ".customer-reviews",
            ".reviews-container",
            "#reviews",
            ".tab-pane-reviews",
            "[class*='review'][class*='container']"
        ],
        "ai_summary_section": [
            ".product-vs-review",
            "[class*='summary']",
            ".ai-summary",
            ".review-summary",
            ".reviewers-saying",
            "[class*='pros-cons']"
        ],
        "individual_reviews_container": [
            ".reviews-list",
            ".customer-review-list",
            ".review-items",
            ".individual-reviews",
            "[class*='review'][class*='list']"
        ],
        "review_item": [
            "[class*='comment']:not([class*='summary'])",
            ".review-item:not(.product-vs-review)",
            ".customer-review:not([class*='summary'])",
            ".user-review",
            ".review-block:not([class*='summary'])",
            ".review:not([class*='summary']):not([class*='vs']):not([class*='banner'])",
            "[class*='review-item']:not([class*='summary'])",
            ".individual-review"
        ],
        "reviewer_name": [
            ".reviewer-name",
            "[class*='reviewer']:not([class*='summary'])",
            ".review-author",
            ".customer-name",
            ".review-by",
            ".user-name",
            "strong:first-child:not([class*='summary'])"
        ],
        "rating": [
            ".rating-eggs:not([class*='summary'])",
            "[class*='rating']:not([class*='summary'])",
            ".review-rating",
            ".stars",
            ".rating-stars",
            ".customer-rating",
            "[class*='egg']:not([class*='summary'])"
        ],
        "title": [
            ".review-title:not([class*='summary'])",
            ".review-heading",
            ".review-item-title",
            "h3:not([class*='summary'])", 
            "h4:not([class*='summary'])", 
            "h5:not([class*='summary'])",
            ".title:not([class*='summary'])"
        ],
        "body": [
            ".review-text:not([class*='summary'])",
            ".review-content:not([class*='summary'])",
            ".review-body",
            ".review-description",
            ".review-comment",
            ".overall-review:not([class*='summary'])",
            "p:not([class*='rating']):not([class*='date']):not([class*='summary'])"
        ],
        "date": [
            ".review-date",
            ".review-time",
            ".posted-date",
            ".review-timestamp",
            "[class*='date']:not([class*='summary'])",
            "time"
        ],
        "verified": [
            ".verified-owner",
            ".verified-purchase",
            ".verified-buyer",
            ".review-verified",
            "[class*='verified']"
        ],
        "helpful": [
            ".helpful-count",
            ".votes",
            "[class*='helpful']",
            ".review-helpful"
        ],
        "load_more": [
            ".btn-load-more",
            "#loadMoreReviews",
            ".show-more-reviews",
            ".load-more-button",
            "button[contains(., 'more')]"
        ]
    }
}

# XPath selectors as fallback
XPATH_SELECTORS = {
    "product": {
        "title": [
            "//h1[contains(@class, 'product-title')]",
            "//h1[@data-testid='product-title']",
            "//div[@class='product-wrap']//h1",
        ],
        "price": [
            "//*[contains(@class, 'price-current')]",
            "//*[@data-testid='product-price']",
            "//span[contains(@class, 'price-now')]"
        ],
        "rating": [
            "//*[contains(@class, 'rating-eggs')]",
            "//*[@data-testid='product-rating']",
            "//div[contains(@class, 'overall-rating')]"
        ]
    },
    "reviews": {
        "review_items": [
            "//div[contains(@class, 'review-item')]",
            "//*[@data-testid='review-item']",
            "//div[contains(@class, 'customer-review')]"
        ]
    }
}

# Headers for HTTP requests
DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}

# Chrome options for Selenium (optimized for Cloudflare bypass)
CHROME_OPTIONS = [
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-blink-features=AutomationControlled",
    "--exclude-switches=enable-automation",
    "--useAutomationExtension=false",
    "--disable-web-security",
    "--allow-running-insecure-content",
    "--disable-extensions",
    "--disable-plugins",
    "--disable-gpu",
    "--window-size=1920,1080",
    "--start-maximized",
    "--disable-infobars",
    "--disable-notifications",
    "--disable-popup-blocking",
    "--disable-save-password-bubble",
    "--disable-translate",
    "--disable-features=VizDisplayCompositor",
    "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

# Environment variables
def get_env_var(key: str, default: Any = None) -> Any:
    """Get environment variable with default fallback"""
    return os.getenv(key, default)

# Override config from environment
DATABASE_PATH = get_env_var("DATABASE_PATH", DATABASE_PATH)
DUCKDB_PATH = get_env_var("DUCKDB_PATH", DUCKDB_PATH)
LOG_LEVEL = get_env_var("LOG_LEVEL", LOG_LEVEL)
MAX_CONCURRENT_REQUESTS = int(get_env_var("MAX_CONCURRENT_REQUESTS", MAX_CONCURRENT_REQUESTS))
REQUEST_DELAY_MIN = float(get_env_var("REQUEST_DELAY_MIN", REQUEST_DELAY_MIN))
REQUEST_DELAY_MAX = float(get_env_var("REQUEST_DELAY_MAX", REQUEST_DELAY_MAX))
