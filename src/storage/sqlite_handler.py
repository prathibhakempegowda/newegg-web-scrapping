"""
SQLite database handler for storing scraped data
"""
import sqlite3
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json
import os
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class SQLiteHandler:
    """SQLite database handler for scraping data"""
    
    def __init__(self, db_path: str = "data/newegg_scraper.db"):
        self.db_path = db_path
        self.ensure_directory()
        self.init_database()
    
    def ensure_directory(self):
        """Ensure the data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def init_database(self):
        """Initialize database with required tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create products table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    brand TEXT,
                    price REAL,
                    rating REAL,
                    review_count INTEGER,
                    description TEXT,
                    url TEXT UNIQUE,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create reviews table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reviews (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER,
                    reviewer_name TEXT,
                    rating REAL,
                    title TEXT,
                    body TEXT,
                    review_date TIMESTAMP,
                    verified_purchase BOOLEAN DEFAULT FALSE,
                    helpful_count INTEGER DEFAULT 0,
                    review_id TEXT UNIQUE,
                    product_url TEXT,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_id) REFERENCES products (id)
                )
            """)
            
            # Create scraping_sessions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scraping_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    method_used TEXT,
                    success BOOLEAN,
                    error_message TEXT,
                    total_reviews_extracted INTEGER DEFAULT 0,
                    duration_seconds REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_url ON products(url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_review_id ON reviews(review_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_rating ON products(rating)")
            
            conn.commit()
            logger.info("Database initialized successfully")
    
    def save_product(self, product_data: Dict[str, Any]) -> int:
        """Save product data and return product ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Check if product already exists
                cursor.execute("SELECT id FROM products WHERE url = ?", (product_data.get("url"),))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing product
                    product_id = existing[0]
                    cursor.execute("""
                        UPDATE products SET 
                            title = ?, brand = ?, price = ?, rating = ?, 
                            review_count = ?, description = ?, 
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (
                        product_data.get("title"),
                        product_data.get("brand"),
                        product_data.get("price"),
                        product_data.get("rating"),
                        product_data.get("review_count"),
                        product_data.get("description"),
                        product_id
                    ))
                    logger.info(f"Updated existing product {product_id}")
                else:
                    # Insert new product
                    cursor.execute("""
                        INSERT INTO products (title, brand, price, rating, review_count, description, url)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        product_data.get("title"),
                        product_data.get("brand"),
                        product_data.get("price"),
                        product_data.get("rating"),
                        product_data.get("review_count"),
                        product_data.get("description"),
                        product_data.get("url")
                    ))
                    product_id = cursor.lastrowid
                    logger.info(f"Inserted new product {product_id}")
                
                conn.commit()
                return product_id
                
            except Exception as e:
                logger.error(f"Error saving product: {e}")
                raise
    
    def save_reviews(self, reviews: List[Dict[str, Any]], product_id: int) -> int:
        """Save reviews data and return count of new reviews"""
        if not reviews:
            return 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                new_reviews_count = 0
                
                for review in reviews:
                    # Check if review already exists
                    review_id = review.get("review_id")
                    if review_id:
                        cursor.execute("SELECT id FROM reviews WHERE review_id = ?", (review_id,))
                        if cursor.fetchone():
                            continue  # Skip existing review
                    
                    # Parse date
                    review_date = None
                    if review.get("date"):
                        try:
                            if isinstance(review["date"], str):
                                review_date = datetime.fromisoformat(review["date"].replace('Z', '+00:00'))
                            else:
                                review_date = review["date"]
                        except (ValueError, TypeError):
                            pass
                    
                    # Insert new review
                    cursor.execute("""
                        INSERT INTO reviews (
                            product_id, reviewer_name, rating, title, body, 
                            review_date, verified_purchase, review_id, product_url
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        product_id,
                        review.get("reviewer_name"),
                        review.get("rating"),
                        review.get("title"),
                        review.get("body"),
                        review_date,
                        review.get("verified_purchase", False),
                        review_id,
                        review.get("product_url")
                    ))
                    new_reviews_count += 1
                
                conn.commit()
                logger.info(f"Saved {new_reviews_count} new reviews")
                return new_reviews_count
                
            except Exception as e:
                logger.error(f"Error saving reviews: {e}")
                raise
    
    def save_scraping_session(self, session_data: Dict[str, Any]) -> int:
        """Save scraping session information"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            try:
                cursor.execute("""
                    INSERT INTO scraping_sessions (
                        url, method_used, success, error_message, 
                        total_reviews_extracted, duration_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    session_data.get("url"),
                    session_data.get("method_used"),
                    session_data.get("success", False),
                    session_data.get("error_message"),
                    session_data.get("total_reviews_extracted", 0),
                    session_data.get("duration_seconds")
                ))
                
                session_id = cursor.lastrowid
                conn.commit()
                logger.info(f"Saved scraping session {session_id}")
                return session_id
                
            except Exception as e:
                logger.error(f"Error saving scraping session: {e}")
                raise
    
    def get_product_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Get product by URL"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM products WHERE url = ?", (url,))
            row = cursor.fetchone()
            
            if row:
                return dict(row)
            return None
    
    def get_reviews_by_product_id(self, product_id: int) -> List[Dict[str, Any]]:
        """Get all reviews for a product"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM reviews 
                WHERE product_id = ? 
                ORDER BY review_date DESC
            """, (product_id,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_products_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all products"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    p.*,
                    COUNT(r.id) as actual_review_count,
                    AVG(r.rating) as avg_review_rating
                FROM products p
                LEFT JOIN reviews r ON p.id = r.product_id
                GROUP BY p.id
                ORDER BY p.created_at DESC
            """)
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Product count
            cursor.execute("SELECT COUNT(*) FROM products")
            product_count = cursor.fetchone()[0]
            
            # Review count
            cursor.execute("SELECT COUNT(*) FROM reviews")
            review_count = cursor.fetchone()[0]
            
            # Session count
            cursor.execute("SELECT COUNT(*) FROM scraping_sessions")
            session_count = cursor.fetchone()[0]
            
            # Average rating
            cursor.execute("SELECT AVG(rating) FROM reviews WHERE rating IS NOT NULL")
            avg_rating = cursor.fetchone()[0]
            
            # Success rate
            cursor.execute("""
                SELECT 
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / COUNT(*) 
                FROM scraping_sessions
            """)
            success_rate = cursor.fetchone()[0]
            
            return {
                "total_products": product_count,
                "total_reviews": review_count,
                "total_sessions": session_count,
                "average_review_rating": round(avg_rating, 2) if avg_rating else None,
                "scraping_success_rate": round(success_rate, 2) if success_rate else None,
                "database_path": self.db_path
            }
    
    def export_to_json(self, output_file: str) -> bool:
        """Export all data to JSON file"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all products with reviews
                cursor.execute("""
                    SELECT 
                        p.*,
                        GROUP_CONCAT(
                            json_object(
                                'reviewer_name', r.reviewer_name,
                                'rating', r.rating,
                                'title', r.title,
                                'body', r.body,
                                'review_date', r.review_date,
                                'verified_purchase', r.verified_purchase
                            )
                        ) as reviews_json
                    FROM products p
                    LEFT JOIN reviews r ON p.id = r.product_id
                    GROUP BY p.id
                """)
                
                products = []
                for row in cursor.fetchall():
                    product = dict(row)
                    
                    # Parse reviews JSON
                    if product['reviews_json']:
                        try:
                            reviews_data = product['reviews_json'].split(',')
                            product['reviews'] = [json.loads(r) for r in reviews_data if r.strip()]
                        except json.JSONDecodeError:
                            product['reviews'] = []
                    else:
                        product['reviews'] = []
                    
                    del product['reviews_json']
                    products.append(product)
                
                # Write to file
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(products, f, indent=2, default=str)
                
                logger.info(f"Exported data to {output_file}")
                return True
                
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False
    
    def cleanup_old_sessions(self, days: int = 30) -> int:
        """Remove old scraping sessions"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                DELETE FROM scraping_sessions 
                WHERE created_at < datetime('now', '-{} days')
            """.format(days))
            
            deleted_count = cursor.rowcount
            conn.commit()
            
            logger.info(f"Cleaned up {deleted_count} old sessions")
            return deleted_count

    def get_products(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get list of products with pagination"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM products 
                ORDER BY created_at DESC 
                LIMIT ? OFFSET ?
            """, (limit, offset))
            
            products = []
            for row in cursor.fetchall():
                product = dict(row)
                # Convert datetime strings to proper format
                if product.get('scraped_at'):
                    product['scraped_at'] = product['scraped_at']
                products.append(product)
            
            return products
    
    def get_products_count(self) -> int:
        """Get total count of products"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM products")
            return cursor.fetchone()[0]
    
    def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific product by ID"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM products WHERE id = ?", (product_id,))
            row = cursor.fetchone()
            
            if row:
                product = dict(row)
                return product
            return None
    
    def get_reviews_by_product(self, product_id: int, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get reviews for a specific product with pagination"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM reviews 
                WHERE product_id = ? 
                ORDER BY created_at DESC 
                LIMIT ? OFFSET ?
            """, (product_id, limit, offset))
            
            reviews = []
            for row in cursor.fetchall():
                review = dict(row)
                # Convert datetime fields
                if review.get('review_date'):
                    review['date'] = review['review_date']
                if review.get('scraped_at'):
                    review['scraped_at'] = review['scraped_at']
                reviews.append(review)
            
            return reviews
    
    def get_reviews_count_by_product(self, product_id: int) -> int:
        """Get total count of reviews for a product"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM reviews WHERE product_id = ?", (product_id,))
            return cursor.fetchone()[0]
    
    def close(self):
        """Close database connections (for FastAPI lifespan)"""
        # SQLite connections are managed per request, so nothing to close
        logger.info("SQLite handler closed")
