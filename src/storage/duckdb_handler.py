"""
DuckDB handler for analytics and bonus analysis
"""
import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None

logger = logging.getLogger(__name__)

class DuckDBHandler:
    """DuckDB handler for analytics workloads"""
    
    def __init__(self, db_path: str = "data/analytics.duckdb"):
        if duckdb is None:
            raise ImportError("DuckDB not installed. Please install with: pip install duckdb")
        
        self.db_path = db_path
        self.ensure_directory()
        self.conn = None
        self.init_database()
    
    def ensure_directory(self):
        """Ensure the data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def connect(self):
        """Connect to DuckDB database"""
        if self.conn is None:
            self.conn = duckdb.connect(self.db_path)
        return self.conn
    
    def init_database(self):
        """Initialize DuckDB with analytics tables"""
        conn = self.connect()
        
        try:
            # Create products table optimized for analytics
            conn.execute("""
                CREATE TABLE IF NOT EXISTS products_analytics (
                    id INTEGER PRIMARY KEY,
                    title VARCHAR,
                    brand VARCHAR,
                    price DECIMAL(10,2),
                    rating DECIMAL(3,2),
                    review_count INTEGER,
                    description TEXT,
                    category VARCHAR,
                    url VARCHAR,
                    scraped_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create reviews table optimized for analytics
            conn.execute("""
                CREATE TABLE IF NOT EXISTS reviews_analytics (
                    id INTEGER PRIMARY KEY,
                    product_id INTEGER,
                    reviewer_name VARCHAR,
                    rating DECIMAL(3,2),
                    title TEXT,
                    body TEXT,
                    review_date DATE,
                    verified_purchase BOOLEAN,
                    helpful_count INTEGER DEFAULT 0,
                    sentiment_score DECIMAL(3,2),
                    review_length INTEGER,
                    scraped_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create Amazon UK products table for bonus analysis
            conn.execute("""
                CREATE TABLE IF NOT EXISTS amazon_products (
                    id INTEGER,
                    title VARCHAR,
                    category VARCHAR,
                    discounted_price DECIMAL(10,2),
                    actual_price DECIMAL(10,2),
                    discount_percentage DECIMAL(5,2),
                    rating DECIMAL(3,2),
                    rating_count INTEGER,
                    about_product TEXT,
                    user_id VARCHAR,
                    user_name VARCHAR,
                    review_id VARCHAR,
                    review_title TEXT,
                    review_content TEXT,
                    img_link TEXT,
                    product_link TEXT
                )
            """)
            
            logger.info("DuckDB analytics database initialized")
            
        except Exception as e:
            logger.error(f"Error initializing DuckDB: {e}")
            raise
    
    def import_from_sqlite(self, sqlite_path: str):
        """Import data from SQLite database"""
        conn = self.connect()
        
        try:
            # Install and load SQLite extension
            conn.execute("INSTALL sqlite;")
            conn.execute("LOAD sqlite;")
            
            # Import products
            conn.execute(f"""
                INSERT OR REPLACE INTO products_analytics 
                SELECT 
                    id, title, brand, price, rating, review_count, 
                    description, 
                    CASE 
                        WHEN LOWER(title) LIKE '%cpu%' OR LOWER(title) LIKE '%processor%' THEN 'CPU'
                        WHEN LOWER(title) LIKE '%gpu%' OR LOWER(title) LIKE '%graphics%' THEN 'GPU'
                        WHEN LOWER(title) LIKE '%motherboard%' OR LOWER(title) LIKE '%mobo%' THEN 'Motherboard'
                        WHEN LOWER(title) LIKE '%memory%' OR LOWER(title) LIKE '%ram%' THEN 'Memory'
                        WHEN LOWER(title) LIKE '%storage%' OR LOWER(title) LIKE '%ssd%' OR LOWER(title) LIKE '%hdd%' THEN 'Storage'
                        ELSE 'Other'
                    END as category,
                    url, scraped_at, created_at
                FROM sqlite_scan('{sqlite_path}', 'products')
            """)
            
            # Import reviews
            conn.execute(f"""
                INSERT OR REPLACE INTO reviews_analytics 
                SELECT 
                    id, product_id, reviewer_name, rating, title, body, 
                    review_date, verified_purchase, helpful_count,
                    NULL as sentiment_score,
                    LENGTH(body) as review_length,
                    scraped_at, created_at
                FROM sqlite_scan('{sqlite_path}', 'reviews')
            """)
            
            logger.info("Successfully imported data from SQLite to DuckDB")
            
        except Exception as e:
            logger.error(f"Error importing from SQLite: {e}")
            raise
    
    def load_amazon_dataset(self, csv_path: str, chunk_size: int = 100000) -> bool:
        """
        Load Amazon UK dataset for bonus analysis with optimized chunked processing
        Handles large datasets efficiently for scalability
        """
        if not os.path.exists(csv_path):
            logger.warning(f"Amazon dataset not found at {csv_path}")
            return False
        
        conn = self.connect()
        
        try:
            # Clear existing data
            conn.execute("DELETE FROM amazon_products")
            
            # Enable parallel processing and optimize for large datasets
            conn.execute("PRAGMA threads=4")  # Use multiple threads
            conn.execute("PRAGMA memory_limit='4GB'")  # Increase memory limit
            
            # Use DuckDB's efficient CSV reader with optimizations
            logger.info(f"Loading large dataset from {csv_path} with chunked processing...")
            
            # Load CSV data with explicit column mapping and optimized settings
            conn.execute(f"""
                INSERT INTO amazon_products (
                    id, title, category, discounted_price, actual_price, discount_percentage,
                    rating, rating_count, about_product, user_id, user_name,
                    review_id, review_title, review_content, img_link, product_link
                )
                SELECT 
                    ROW_NUMBER() OVER() as id,
                    title, 
                    COALESCE(category, 'Unknown') as category,
                    CAST(discounted_price AS DECIMAL(10,2)) as discounted_price,
                    CAST(actual_price AS DECIMAL(10,2)) as actual_price,
                    CAST(discount_percentage AS DECIMAL(5,2)) as discount_percentage,
                    CAST(rating AS DECIMAL(3,2)) as rating,
                    CAST(rating_count AS INTEGER) as rating_count,
                    about_product, user_id, user_name,
                    review_id, review_title, review_content, img_link, product_link
                FROM read_csv_auto(
                    '{csv_path}',
                    header=true,
                    delim=',',
                    quote='"',
                    escape='"',
                    null_padding=true,
                    ignore_errors=true,
                    max_line_size=1048576,
                    sample_size=50000
                )
                WHERE rating IS NOT NULL 
                AND rating > 0 
                AND category IS NOT NULL
            """)
            
            # Get row count and statistics
            result = conn.execute("SELECT COUNT(*) FROM amazon_products").fetchone()
            row_count = result[0] if result else 0
            
            # Get category statistics for validation
            category_stats = conn.execute("""
                SELECT 
                    COUNT(DISTINCT category) as unique_categories,
                    MIN(rating) as min_rating,
                    MAX(rating) as max_rating,
                    AVG(rating) as avg_rating
                FROM amazon_products
            """).fetchone()
            
            logger.info(f"Successfully loaded {row_count:,} Amazon products into DuckDB")
            logger.info(f"Dataset statistics: {category_stats[0]} categories, "
                       f"ratings range: {category_stats[1]:.1f}-{category_stats[2]:.1f}, "
                       f"average: {category_stats[3]:.2f}")
            
            # Create indexes for performance
            self._create_performance_indexes()
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading Amazon dataset: {e}")
            return False
    
    def _create_performance_indexes(self):
        """Create indexes for optimal query performance on large datasets"""
        conn = self.connect()
        
        try:
            # Create indexes for frequently queried columns
            conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON amazon_products(category)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_rating ON amazon_products(rating)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_category_rating ON amazon_products(category, rating)")
            
            logger.info("Performance indexes created successfully")
            
        except Exception as e:
            logger.warning(f"Could not create all indexes: {e}")
    
    def get_category_analysis(self) -> pd.DataFrame:
        """
        Perform comprehensive category-based analysis optimized for large datasets
        Includes advanced statistical measures and scalable query design
        """
        conn = self.connect()
        
        try:
            # Optimized query for large datasets using window functions and CTEs
            query = """
            WITH category_stats AS (
                SELECT 
                    category,
                    COUNT(*) as product_count,
                    AVG(rating) as avg_rating,
                    STDDEV_POP(rating) as rating_stddev,
                    VAR_POP(rating) as rating_variance,
                    MIN(rating) as min_rating,
                    MAX(rating) as max_rating,
                    MEDIAN(rating) as median_rating,
                    COUNT(*) FILTER (WHERE rating >= 4.0) as high_rating_count,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY rating) as q1_rating,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY rating) as q3_rating
                FROM amazon_products 
                WHERE rating IS NOT NULL 
                AND category IS NOT NULL
                AND category != ''
                GROUP BY category
                HAVING COUNT(*) >= 1  -- Include all categories for analysis
            ),
            overall_stats AS (
                SELECT 
                    AVG(rating) as overall_avg,
                    STDDEV_POP(rating) as overall_stddev
                FROM amazon_products 
                WHERE rating IS NOT NULL
            )
            SELECT 
                cs.category,
                cs.product_count,
                ROUND(cs.avg_rating, 3) as avg_rating,
                ROUND(cs.rating_stddev, 3) as rating_stddev,
                ROUND(cs.rating_variance, 3) as rating_variance,
                cs.min_rating,
                cs.max_rating,
                ROUND(cs.median_rating, 1) as median_rating,
                ROUND((cs.high_rating_count * 100.0 / cs.product_count), 1) as high_rating_percentage,
                ROUND(cs.q1_rating, 1) as q1_rating,
                ROUND(cs.q3_rating, 1) as q3_rating,
                ROUND(cs.q3_rating - cs.q1_rating, 1) as iqr,
                ROUND((cs.avg_rating - os.overall_avg) / NULLIF(os.overall_stddev, 0), 2) as z_score,
                CASE 
                    WHEN ABS((cs.avg_rating - os.overall_avg) / NULLIF(os.overall_stddev, 0)) > 2 THEN 'Significant'
                    WHEN ABS((cs.avg_rating - os.overall_avg) / NULLIF(os.overall_stddev, 0)) > 1 THEN 'Notable'
                    ELSE 'Normal'
                END as significance_level
            FROM category_stats cs
            CROSS JOIN overall_stats os
            ORDER BY cs.avg_rating DESC, cs.product_count DESC
            """
            
            result = conn.execute(query).df()
            
            if result.empty:
                logger.warning("No category analysis data returned")
                return pd.DataFrame()
            
            logger.info(f"Category analysis completed for {len(result)} categories")
            return result
            
        except Exception as e:
            logger.error(f"Error in category analysis: {e}")
            return pd.DataFrame()
    
    def get_statistical_insights(self) -> Dict[str, Any]:
        """
        Generate comprehensive statistical insights optimized for large datasets
        Includes advanced analytics suitable for dashboard integration
        """
        conn = self.connect()
        
        try:
            # Overall dataset statistics with performance optimizations
            overall_query = """
            SELECT 
                COUNT(*) as total_products,
                COUNT(DISTINCT category) as unique_categories,
                AVG(rating) as overall_average_rating,
                STDDEV_POP(rating) as overall_stddev,
                MEDIAN(rating) as median_rating,
                MODE() WITHIN GROUP (ORDER BY rating) as mode_rating,
                MIN(rating) as min_rating,
                MAX(rating) as max_rating,
                COUNT(*) FILTER (WHERE rating >= 4.0) as high_rated_products,
                COUNT(*) FILTER (WHERE rating <= 2.0) as low_rated_products
            FROM amazon_products 
            WHERE rating IS NOT NULL
            """
            
            overall_stats = conn.execute(overall_query).fetchone()
            
            if not overall_stats:
                return {"error": "No data available for analysis"}
            
            # Category performance analysis with statistical significance
            category_performance_query = """
            WITH category_stats AS (
                SELECT 
                    category,
                    COUNT(*) as count,
                    AVG(rating) as avg_rating,
                    STDDEV_POP(rating) as stddev,
                    (AVG(rating) - (SELECT AVG(rating) FROM amazon_products WHERE rating IS NOT NULL)) / 
                    NULLIF((SELECT STDDEV_POP(rating) FROM amazon_products WHERE rating IS NOT NULL), 0) as z_score
                FROM amazon_products 
                WHERE rating IS NOT NULL 
                AND category IS NOT NULL
                GROUP BY category
                HAVING COUNT(*) >= 1  -- Include all categories for small datasets
            )
            SELECT 
                category,
                count,
                ROUND(avg_rating, 3) as avg_rating,
                ROUND(stddev, 3) as stddev,
                ROUND(z_score, 2) as z_score,
                CASE 
                    WHEN ABS(z_score) > 2.58 THEN 'Highly Significant (p<0.01)'
                    WHEN ABS(z_score) > 1.96 THEN 'Significant (p<0.05)'
                    WHEN ABS(z_score) > 1.64 THEN 'Notable (p<0.10)'
                    ELSE 'Normal'
                END as significance_level
            FROM category_stats
            ORDER BY ABS(z_score) DESC
            LIMIT 20
            """
            
            category_performance = conn.execute(category_performance_query).fetchall()
            
            # Rating distribution analysis
            rating_distribution_query = """
            SELECT 
                FLOOR(rating) as rating_floor,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM amazon_products 
            WHERE rating IS NOT NULL
            GROUP BY FLOOR(rating)
            ORDER BY rating_floor
            """
            
            rating_distribution = conn.execute(rating_distribution_query).fetchall()
            
            # Variability analysis - identify categories with highest/lowest variance
            variability_highest = conn.execute("""
                SELECT 
                    category,
                    COUNT(*) as sample_size,
                    VAR_POP(rating) as variance,
                    STDDEV_POP(rating) as stddev
                FROM amazon_products 
                WHERE rating IS NOT NULL AND category IS NOT NULL
                GROUP BY category
                HAVING COUNT(*) >= 1
                ORDER BY variance DESC
                LIMIT 5
            """).fetchall()
            
            variability_lowest = conn.execute("""
                SELECT 
                    category,
                    COUNT(*) as sample_size,
                    VAR_POP(rating) as variance,
                    STDDEV_POP(rating) as stddev
                FROM amazon_products 
                WHERE rating IS NOT NULL AND category IS NOT NULL
                GROUP BY category
                HAVING COUNT(*) >= 1
                ORDER BY variance ASC
                LIMIT 5
            """).fetchall()
            
            # Z-score analysis for categories
            z_score_query = """
            WITH overall_mean AS (
                SELECT AVG(rating) as mean_rating, STDDEV_POP(rating) as std_rating
                FROM amazon_products WHERE rating IS NOT NULL
            ),
            category_stats AS (
                SELECT 
                    category,
                    COUNT(*) as count,
                    AVG(rating) as avg_rating,
                    (AVG(rating) - om.mean_rating) / NULLIF(om.std_rating, 0) as z_score
                FROM amazon_products, overall_mean om
                WHERE rating IS NOT NULL AND category IS NOT NULL
                GROUP BY category, om.mean_rating, om.std_rating
                HAVING COUNT(*) >= 1
            )
            SELECT 
                category,
                count,
                ROUND(avg_rating, 3) as avg_rating,
                ROUND(z_score, 2) as z_score,
                CASE 
                    WHEN ABS(z_score) > 2 THEN 'Significant'
                    WHEN ABS(z_score) > 1 THEN 'Notable'
                    ELSE 'Normal'
                END as significance_level
            FROM category_stats
            ORDER BY ABS(z_score) DESC
            """
            
            z_score_analysis = conn.execute(z_score_query).fetchall()
            
            # Prepare insights response
            insights = {
                "total_products": overall_stats[0],
                "unique_categories": overall_stats[1], 
                "overall_average_rating": round(overall_stats[2], 3) if overall_stats[2] else 0,
                "overall_stddev": round(overall_stats[3], 3) if overall_stats[3] else 0,
                "median_rating": overall_stats[4],
                "mode_rating": overall_stats[5],
                "rating_range": {
                    "min": overall_stats[6],
                    "max": overall_stats[7]
                },
                "quality_distribution": {
                    "high_rated_products": overall_stats[8],
                    "low_rated_products": overall_stats[9],
                    "high_rated_percentage": round(overall_stats[8] * 100.0 / overall_stats[0], 2) if overall_stats[0] > 0 else 0,
                    "low_rated_percentage": round(overall_stats[9] * 100.0 / overall_stats[0], 2) if overall_stats[0] > 0 else 0
                },
                "z_score_analysis": [
                    {
                        "category": row[0],
                        "sample_size": row[1],
                        "avg_rating": row[2],
                        "z_score": row[3],
                        "significance_level": row[4]
                    }
                    for row in z_score_analysis
                ],
                "rating_distribution": [
                    {
                        "rating": int(row[0]),
                        "count": row[1],
                        "percentage": row[2]
                    }
                    for row in rating_distribution
                ],
                "variability_analysis": {
                    "highest_variance": [
                        {"category": row[0], "variance": round(row[2], 3) if row[2] else 0}
                        for row in variability_highest
                    ],
                    "lowest_variance": [
                        {"category": row[0], "variance": round(row[2], 3) if row[2] else 0}
                        for row in variability_lowest
                    ]
                },
                "generated_at": datetime.now().isoformat()
            }
            
            logger.info("Statistical insights analysis completed successfully")
            return insights
            
        except Exception as e:
            logger.error(f"Error generating statistical insights: {e}")
            return {"error": str(e), "generated_at": datetime.now().isoformat()}
    
    def get_rating_distribution(self) -> pd.DataFrame:
        """Get rating distribution analysis"""
        conn = self.connect()
        
        try:
            query = """
            SELECT 
                category,
                FLOOR(rating) as rating_floor,
                COUNT(*) as count,
                COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY category) as percentage
            FROM amazon_products 
            WHERE rating IS NOT NULL AND rating > 0
            GROUP BY category, FLOOR(rating)
            ORDER BY category, rating_floor
            """
            
            df = conn.execute(query).df()
            return df
            
        except Exception as e:
            logger.error(f"Error in rating distribution analysis: {e}")
            return pd.DataFrame()
    
    def execute_custom_query(self, query: str) -> pd.DataFrame:
        """Execute custom SQL query and return DataFrame"""
        conn = self.connect()
        
        try:
            df = conn.execute(query).df()
            return df
        except Exception as e:
            logger.error(f"Error executing custom query: {e}")
            return pd.DataFrame()
    
    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """Get information about a table"""
        conn = self.connect()
        
        try:
            df = conn.execute(f"DESCRIBE {table_name}").df()
            return df
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            return pd.DataFrame()
    
    def export_analysis_results(self, output_dir: str = "data/analysis_results/"):
        """Export analysis results to CSV files"""
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            # Category analysis
            category_analysis = self.get_category_analysis()
            if not category_analysis.empty:
                category_analysis.to_csv(f"{output_dir}/category_analysis.csv", index=False)
                logger.info(f"Exported category analysis to {output_dir}/category_analysis.csv")
            
            # Rating distribution
            rating_dist = self.get_rating_distribution()
            if not rating_dist.empty:
                rating_dist.to_csv(f"{output_dir}/rating_distribution.csv", index=False)
                logger.info(f"Exported rating distribution to {output_dir}/rating_distribution.csv")
            
            # Statistical insights
            insights = self.get_statistical_insights()
            if insights:
                import json
                with open(f"{output_dir}/statistical_insights.json", 'w') as f:
                    json.dump(insights, f, indent=2)
                logger.info(f"Exported statistical insights to {output_dir}/statistical_insights.json")
            
        except Exception as e:
            logger.error(f"Error exporting analysis results: {e}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def insert_analytics_data(self, data: Dict[str, Any]) -> bool:
        """Insert analytics data for tracking scraping performance"""
        conn = self.connect()
        
        try:
            # Create analytics table if it doesn't exist
            conn.execute("""
                CREATE TABLE IF NOT EXISTS scraping_analytics (
                    id INTEGER,
                    product_id INTEGER,
                    total_reviews INTEGER,
                    scraping_method VARCHAR,
                    scraping_time DECIMAL(10,3),
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Get next ID
            next_id_result = conn.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM scraping_analytics").fetchone()
            next_id = next_id_result[0] if next_id_result else 1
            
            # Insert data
            conn.execute("""
                INSERT INTO scraping_analytics 
                (id, product_id, total_reviews, scraping_method, scraping_time, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                next_id,
                data.get('product_id'),
                data.get('total_reviews', 0),
                data.get('scraping_method', 'unknown'),
                data.get('scraping_time', 0.0),
                data.get('timestamp', pd.Timestamp.now())
            ))
            
            conn.commit()
            logger.info("Analytics data inserted successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting analytics data: {e}")
            return False
    
    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get analytics summary for FastAPI"""
        conn = self.connect()
        
        try:
            # Get scraping statistics
            scraping_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_scraping_sessions,
                    AVG(scraping_time) as avg_scraping_time,
                    SUM(total_reviews) as total_reviews_scraped,
                    scraping_method,
                    COUNT(*) as method_count
                FROM scraping_analytics 
                GROUP BY scraping_method
            """).df()
            
            # Get recent activity
            recent_activity = conn.execute("""
                SELECT 
                    CAST(timestamp AS DATE) as date,
                    COUNT(*) as sessions,
                    SUM(total_reviews) as reviews
                FROM scraping_analytics 
                WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY CAST(timestamp AS DATE)
                ORDER BY date DESC
            """).df()
            
            # Overall stats
            overall_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_sessions,
                    COALESCE(AVG(scraping_time), 0) as avg_time,
                    COALESCE(MIN(scraping_time), 0) as min_time,
                    COALESCE(MAX(scraping_time), 0) as max_time,
                    COALESCE(SUM(total_reviews), 0) as total_reviews
                FROM scraping_analytics
            """).df()
            
            # Convert DataFrames to dict and handle NaN values
            def clean_df_dict(df):
                result = df.to_dict('records')
                for record in result:
                    for key, value in record.items():
                        if pd.isna(value):
                            record[key] = 0 if isinstance(value, (int, float)) else None
                        elif isinstance(value, float) and (value == float('inf') or value == float('-inf')):
                            record[key] = 0
                return result
            
            return {
                "overall_stats": clean_df_dict(overall_stats)[0] if not overall_stats.empty else {},
                "method_performance": clean_df_dict(scraping_stats) if not scraping_stats.empty else [],
                "recent_activity": clean_df_dict(recent_activity) if not recent_activity.empty else [],
                "generated_at": pd.Timestamp.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting analytics summary: {e}")
            return {
                "error": str(e),
                "overall_stats": {},
                "method_performance": [],
                "recent_activity": [],
                "generated_at": pd.Timestamp.now().isoformat()
            }
