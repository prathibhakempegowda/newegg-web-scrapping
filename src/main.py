"""
Main application entry point for the Newegg web scraper
"""
import asyncio
import argparse
import logging
import time
from typing import Dict, Any, Optional
from datetime import datetime
import json
import os
import sys

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from scraper import NeweggScraper, ScrapingResult
from storage import SQLiteHandler
from scraper.config import TARGET_URL, LOG_LEVEL, LOG_FILE, DATABASE_PATH
from scraper.utils import setup_logging

logger = logging.getLogger(__name__)

class ScrapingOrchestrator:
    """Main orchestrator for the scraping process"""
    
    def __init__(self, database_path: str = DATABASE_PATH):
        self.database_path = database_path
        self.db_handler = SQLiteHandler(database_path)
        self.scraper = None
        
    async def scrape_product(
        self, 
        url: str = TARGET_URL, 
        max_reviews: int = 100,
        method: str = "fallback"
    ) -> Dict[str, Any]:
        """
        Scrape a Newegg product page
        
        Args:
            url: Product URL to scrape
            max_reviews: Maximum number of reviews to extract
            method: Scraping method ('selenium', 'cloudscraper', 'aiohttp', 'fallback')
        
        Returns:
            Dictionary containing scraping results and metadata
        """
        start_time = time.time()
        session_data = {
            "url": url,
            "method_used": method,
            "success": False,
            "total_reviews_extracted": 0,
            "duration_seconds": 0
        }
        
        try:
            logger.info(f"Starting scraping process for: {url}")
            logger.info(f"Target reviews: {max_reviews}, Method: {method}")
            
            # Initialize scraper
            async with NeweggScraper() as scraper:
                self.scraper = scraper
                
                # Perform scraping
                scraped_data = await scraper.scrape_product_and_reviews(url, max_reviews)
                
                if "error" in scraped_data:
                    session_data["error_message"] = scraped_data["error"]
                    logger.error(f"Scraping failed: {scraped_data['error']}")
                    return {
                        "success": False,
                        "error": scraped_data["error"],
                        "session_data": session_data
                    }
                
                # Save to database
                product_data = scraped_data.get("product", {})
                reviews_data = scraped_data.get("reviews", [])
                
                if product_data:
                    product_id = self.db_handler.save_product(product_data)
                    logger.info(f"Saved product with ID: {product_id}")
                    
                    if reviews_data:
                        new_reviews_count = self.db_handler.save_reviews(reviews_data, product_id)
                        session_data["total_reviews_extracted"] = new_reviews_count
                        logger.info(f"Saved {new_reviews_count} new reviews")
                
                # Update session data
                session_data["success"] = True
                session_data["method_used"] = scraped_data.get("extraction_method", method)
                
                # Calculate duration
                duration = time.time() - start_time
                session_data["duration_seconds"] = round(duration, 2)
                
                # Save session
                session_id = self.db_handler.save_scraping_session(session_data)
                
                logger.info(f"Scraping completed successfully in {duration:.2f}s")
                
                return {
                    "success": True,
                    "product": product_data,
                    "reviews": reviews_data,
                    "statistics": {
                        "total_reviews_found": len(reviews_data),
                        "extraction_method": scraped_data.get("extraction_method"),
                        "duration_seconds": duration,
                        "session_id": session_id
                    },
                    "session_data": session_data
                }
                
        except Exception as e:
            duration = time.time() - start_time
            session_data["duration_seconds"] = round(duration, 2)
            session_data["error_message"] = str(e)
            
            # Save failed session
            self.db_handler.save_scraping_session(session_data)
            
            logger.error(f"Scraping process failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "session_data": session_data
            }
    
    def get_scraping_summary(self) -> Dict[str, Any]:
        """Get summary of all scraping activities"""
        try:
            stats = self.db_handler.get_database_stats()
            products = self.db_handler.get_products_summary()
            
            return {
                "database_statistics": stats,
                "products_summary": products,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            return {"error": str(e)}
    
    def export_data(self, output_file: str = "data/exported_data.json") -> bool:
        """Export all scraped data to JSON"""
        try:
            return self.db_handler.export_to_json(output_file)
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False

async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Newegg Product Scraper")
    parser.add_argument("--url", default=TARGET_URL, help="Product URL to scrape")
    parser.add_argument("--max-reviews", type=int, default=100, help="Maximum reviews to extract")
    parser.add_argument("--method", choices=["selenium", "cloudscraper", "aiohttp", "fallback"], 
                       default="fallback", help="Scraping method")
    parser.add_argument("--database", default=DATABASE_PATH, help="Database path")
    parser.add_argument("--log-level", default=LOG_LEVEL, help="Logging level")
    parser.add_argument("--export", action="store_true", help="Export data to JSON after scraping")
    parser.add_argument("--summary", action="store_true", help="Show summary of previous scraping sessions")
    parser.add_argument("--analysis", action="store_true", help="Run bonus category analysis")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, LOG_FILE)
    
    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    logger.info("=" * 60)
    logger.info("NEWEGG WEB SCRAPER - ENHANCED EDITION")
    logger.info("=" * 60)
    
    # Initialize orchestrator
    orchestrator = ScrapingOrchestrator(args.database)
    
    try:
        # Show summary if requested
        if args.summary:
            logger.info("Generating scraping summary...")
            summary = orchestrator.get_scraping_summary()
            
            if "error" not in summary:
                stats = summary["database_statistics"]
                print(f"\nDatabase Statistics:")
                print(f"  Total Products: {stats.get('total_products', 0)}")
                print(f"  Total Reviews: {stats.get('total_reviews', 0)}")
                print(f"  Total Sessions: {stats.get('total_sessions', 0)}")
                print(f"  Average Rating: {stats.get('average_review_rating', 'N/A')}")
                print(f"  Success Rate: {stats.get('scraping_success_rate', 'N/A')}%")
                print(f"  Database: {stats.get('database_path', 'N/A')}")
            else:
                print(f"Error generating summary: {summary['error']}")
        
        # Run category analysis if requested
        if args.analysis:
            logger.info("Running bonus category analysis...")
            try:
                from analysis import run_category_analysis
                if run_category_analysis:
                    results = run_category_analysis()
                    if "error" not in results:
                        print("\nCategory Analysis completed successfully!")
                        print(f"Check output files: {results.get('output_files', {})}")
                    else:
                        print(f"Analysis failed: {results['error']}")
                else:
                    print("Category analysis not available. Install pandas and duckdb.")
            except ImportError as e:
                print(f"Category analysis not available: {e}")
                print("Install required packages: pip install pandas duckdb")
        
        # Perform scraping if URL provided
        if not args.summary and not args.analysis:
            logger.info(f"Starting scraping process...")
            
            # Validate URL
            if not args.url or not args.url.startswith("http"):
                logger.error("Invalid URL provided")
                return
            
            # Run scraping
            result = await orchestrator.scrape_product(
                url=args.url,
                max_reviews=args.max_reviews,
                method=args.method
            )
            
            # Display results
            if result["success"]:
                stats = result["statistics"]
                product = result["product"]
                
                print(f"\n✅ Scraping Successful!")
                print(f"Product: {product.get('title', 'Unknown')}")
                print(f"Brand: {product.get('brand', 'Unknown')}")
                print(f"Price: ${product.get('price', 'Unknown')}")
                print(f"Rating: {product.get('rating', 'Unknown')}/5")
                print(f"Reviews Extracted: {stats['total_reviews_found']}")
                print(f"Method Used: {stats['extraction_method']}")
                print(f"Duration: {stats['duration_seconds']}s")
                
                # Export if requested
                if args.export:
                    export_file = f"data/export_{int(time.time())}.json"
                    if orchestrator.export_data(export_file):
                        print(f"Data exported to: {export_file}")
                    else:
                        print("Export failed")
                        
            else:
                print(f"\n❌ Scraping Failed: {result['error']}")
        
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Scraping session completed")

if __name__ == "__main__":
    asyncio.run(main())
