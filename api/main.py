"""
FastAPI application for Newegg Web Scraper
Provides REST API endpoints for scraping Newegg products and reviews
"""
import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, HttpUrl, Field, field_validator
import uvicorn

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.scraper.newegg_scraper import NeweggScraper
from src.storage.sqlite_handler import SQLiteHandler
from src.storage.duckdb_handler import DuckDBHandler
from src.analysis.large_scale_analyzer import LargeScaleAnalyzer, run_large_scale_analysis, get_analysis_summary

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize storage handlers
sqlite_handler = None
duckdb_handler = None
large_scale_analyzer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup resources"""
    global sqlite_handler, duckdb_handler, large_scale_analyzer
    
    # Startup
    logger.info("Initializing FastAPI Newegg Scraper...")
    try:
        sqlite_handler = SQLiteHandler()
        duckdb_handler = DuckDBHandler()
        large_scale_analyzer = LargeScaleAnalyzer()
        logger.info("Database handlers and analyzer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database handlers: {e}")
        
    yield
    
    # Cleanup
    logger.info("Shutting down FastAPI Newegg Scraper...")
    try:
        if sqlite_handler:
            sqlite_handler.close()
        if duckdb_handler:
            duckdb_handler.close()
        if large_scale_analyzer:
            large_scale_analyzer.cleanup()
        logger.info("Resources cleaned up successfully")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down FastAPI Newegg Scraper...")
    if sqlite_handler:
        sqlite_handler.close()
    if duckdb_handler:
        duckdb_handler.close()

# FastAPI app
app = FastAPI(
    title="Newegg Web Scraper API",
    description="Enhanced web scraper for Newegg products and reviews with Cloudflare bypass",
    version="2.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class ScrapeRequest(BaseModel):
    """Request model for scraping operation"""
    url: HttpUrl = Field(..., description="Newegg product URL to scrape")
    target_reviews: int = Field(50, ge=1, le=500, description="Number of reviews to scrape (1-500)")
    method: str = Field("fallback", description="Scraping method: selenium, cloudscraper, aiohttp, or fallback")
    
    @field_validator('url')
    @classmethod
    def validate_newegg_url(cls, v):
        url_str = str(v)
        if 'newegg.com' not in url_str:
            raise ValueError('URL must be from newegg.com')
        return v
    
    @field_validator('method')
    @classmethod
    def validate_method(cls, v):
        valid_methods = ['selenium', 'cloudscraper', 'aiohttp', 'fallback']
        if v.lower() not in valid_methods:
            raise ValueError(f'Method must be one of: {valid_methods}')
        return v.lower()

class ProductResponse(BaseModel):
    """Response model for product information"""
    id: Optional[int] = None
    title: Optional[str] = None
    brand: Optional[str] = None
    price: Optional[float] = None
    rating: Optional[float] = None
    review_count: Optional[int] = None
    description: Optional[str] = None
    url: str
    scraped_at: datetime

class ReviewResponse(BaseModel):
    """Response model for review information"""
    id: Optional[int] = None
    product_id: Optional[int] = None
    reviewer_name: Optional[str] = None
    rating: Optional[float] = None
    title: Optional[str] = None
    body: Optional[str] = None
    date: Optional[datetime] = None
    verified_purchase: bool = False
    scraped_at: datetime

class ScrapeResponse(BaseModel):
    """Response model for scraping operation"""
    success: bool
    product: Optional[ProductResponse] = None
    reviews: List[ReviewResponse] = []
    total_reviews: int = 0
    scraping_time: float
    method_used: str
    message: str

class JobStatus(BaseModel):
    """Background job status"""
    job_id: str
    status: str  # pending, running, completed, failed
    created_at: datetime
    completed_at: Optional[datetime] = None
    result: Optional[ScrapeResponse] = None
    error: Optional[str] = None

# In-memory job storage (use Redis in production)
jobs: Dict[str, JobStatus] = {}

# API Endpoints

@app.get("/", summary="API Health Check")
async def root():
    """Health check endpoint"""
    return {
        "message": "Newegg Web Scraper API",
        "version": "2.0.0",
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "features": [
            "Multi-strategy scraping (Selenium, CloudScraper, aiohttp)",
            "Enhanced Cloudflare bypass",
            "JSON-LD structured data support",
            "Concurrent scraping with rate limiting",
            "SQLite and DuckDB storage",
            "Background job processing",
            "Large-scale dataset analysis (2M+ rows)",
            "Advanced statistical insights",
            "Category performance analytics",
            "Scalable query optimization"
        ]
    }

@app.post("/scrape", response_model=ScrapeResponse, summary="Scrape Newegg Product")
async def scrape_product(request: ScrapeRequest):
    """
    Scrape a Newegg product page for product information and reviews
    
    - **url**: Newegg product URL
    - **target_reviews**: Number of reviews to scrape (1-500)
    - **method**: Scraping method (selenium, cloudscraper, aiohttp, fallback)
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"Starting synchronous scrape for: {request.url}")
        
        # Initialize scraper
        scraper = NeweggScraper()
        
        # Perform scraping
        result_data = await scraper.scrape_product_and_reviews(
            url=str(request.url),
            max_reviews=request.target_reviews
        )
        
        if not result_data or not result_data.get('product'):
            raise HTTPException(
                status_code=400,
                detail=f"Scraping failed: No product data returned"
            )
        
        # Store results in database
        product_id = None
        if result_data.get('product'):
            product_data = result_data['product']
            product_data['url'] = str(request.url)
            product_id = sqlite_handler.save_product(product_data)
        
        review_ids = []
        if result_data.get('reviews') and product_id:
            # save_reviews expects a list and product_id
            num_saved = sqlite_handler.save_reviews(result_data['reviews'], product_id)
            # Generate placeholder review IDs since save_reviews returns count
            review_ids = list(range(num_saved))
        
        # Store analytics data in DuckDB
        if product_id:
            duckdb_handler.insert_analytics_data({
                'product_id': product_id,
                'total_reviews': len(review_ids),
                'scraping_method': request.method,
                'scraping_time': (datetime.now() - start_time).total_seconds(),
                'timestamp': datetime.now()
            })
        
        scraping_time = (datetime.now() - start_time).total_seconds()
        
        # Prepare response
        product_response = None
        if result_data.get('product'):
            product_response = ProductResponse(
                id=product_id,
                **result_data['product']
            )
        
        review_responses = []
        if result_data.get('reviews'):
            for i, review_data in enumerate(result_data['reviews']):
                review_responses.append(ReviewResponse(
                    id=review_ids[i] if i < len(review_ids) else None,
                    product_id=product_id,
                    **review_data
                ))
        
        return ScrapeResponse(
            success=True,
            product=product_response,
            reviews=review_responses,
            total_reviews=len(review_responses),
            scraping_time=scraping_time,
            method_used=request.method,
            message=f"Successfully scraped {len(review_responses)} reviews"
        )
        
    except Exception as e:
        logger.error(f"Scraping error: {e}")
        scraping_time = (datetime.now() - start_time).total_seconds()
        
        return ScrapeResponse(
            success=False,
            product=None,
            reviews=[],
            total_reviews=0,
            scraping_time=scraping_time,
            method_used=request.method,
            message=f"Scraping failed: {str(e)}"
        )

@app.post("/scrape/async", summary="Start Background Scraping Job")
async def scrape_product_async(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Start a background scraping job for large or long-running operations
    
    Returns a job ID that can be used to check status
    """
    import uuid
    
    job_id = str(uuid.uuid4())
    
    # Create job status
    job_status = JobStatus(
        job_id=job_id,
        status="pending",
        created_at=datetime.now()
    )
    jobs[job_id] = job_status
    
    # Add background task
    background_tasks.add_task(
        run_background_scrape,
        job_id=job_id,
        request=request
    )
    
    return {
        "job_id": job_id,
        "status": "pending",
        "message": "Scraping job started",
        "check_status_url": f"/jobs/{job_id}"
    }

async def run_background_scrape(job_id: str, request: ScrapeRequest):
    """Run scraping operation in background"""
    try:
        # Update job status
        jobs[job_id].status = "running"
        
        start_time = datetime.now()
        
        # Initialize scraper
        scraper = NeweggScraper()
        
        # Perform scraping
        result_data = await scraper.scrape_product_and_reviews(
            url=str(request.url),
            max_reviews=request.target_reviews
        )
        
        if not result_data or not result_data.get('product'):
            jobs[job_id].status = "failed"
            jobs[job_id].error = "No product data returned"
            jobs[job_id].completed_at = datetime.now()
            return
        
        # Store results (same as synchronous version)
        product_id = None
        if result_data.get('product'):
            product_data = result_data['product']
            product_data['url'] = str(request.url)
            product_id = sqlite_handler.save_product(product_data)
        
        review_ids = []
        if result_data.get('reviews') and product_id:
            # save_reviews expects a list and product_id
            num_saved = sqlite_handler.save_reviews(result_data['reviews'], product_id)
            # Generate placeholder review IDs since save_reviews returns count
            review_ids = list(range(num_saved))
        
        scraping_time = (datetime.now() - start_time).total_seconds()
        
        # Store analytics data in DuckDB
        if product_id:
            duckdb_handler.insert_analytics_data({
                'product_id': product_id,
                'total_reviews': len(review_ids),
                'scraping_method': request.method,
                'scraping_time': scraping_time,
                'timestamp': datetime.now()
            })
        
        # Prepare response
        product_response = None
        if result_data.get('product'):
            product_response = ProductResponse(
                id=product_id,
                **result_data['product']
            )
        
        review_responses = []
        if result_data.get('reviews'):
            for i, review_data in enumerate(result_data['reviews']):
                review_responses.append(ReviewResponse(
                    id=review_ids[i] if i < len(review_ids) else None,
                    product_id=product_id,
                    **review_data
                ))
        
        # Update job with results
        jobs[job_id].status = "completed"
        jobs[job_id].completed_at = datetime.now()
        jobs[job_id].result = ScrapeResponse(
            success=True,
            product=product_response,
            reviews=review_responses,
            total_reviews=len(review_responses),
            scraping_time=scraping_time,
            method_used=request.method,
            message=f"Successfully scraped {len(review_responses)} reviews"
        )
        
    except Exception as e:
        logger.error(f"Background scraping error: {e}")
        jobs[job_id].status = "failed"
        jobs[job_id].error = str(e)
        jobs[job_id].completed_at = datetime.now()

@app.get("/jobs/{job_id}", response_model=JobStatus, summary="Check Job Status")
async def get_job_status(job_id: str = Path(..., description="Job ID")):
    """Check the status of a background scraping job"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return jobs[job_id]

@app.get("/jobs", summary="List All Jobs")
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by job status"),
    limit: int = Query(50, ge=1, le=200, description="Number of jobs to return")
):
    """List background scraping jobs with optional filtering"""
    filtered_jobs = list(jobs.values())
    
    if status:
        filtered_jobs = [job for job in filtered_jobs if job.status == status]
    
    # Sort by creation time (newest first)
    filtered_jobs.sort(key=lambda x: x.created_at, reverse=True)
    
    return {
        "jobs": filtered_jobs[:limit],
        "total": len(filtered_jobs),
        "limit": limit
    }

@app.get("/products", summary="Get Scraped Products")
async def get_products(
    limit: int = Query(50, ge=1, le=200, description="Number of products to return"),
    offset: int = Query(0, ge=0, description="Number of products to skip")
):
    """Get list of scraped products from database"""
    try:
        products = sqlite_handler.get_products(limit=limit, offset=offset)
        total = sqlite_handler.get_products_count()
        
        return {
            "products": products,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/products/{product_id}", summary="Get Product by ID")
async def get_product(product_id: int = Path(..., description="Product ID")):
    """Get a specific product and its reviews"""
    try:
        product = sqlite_handler.get_product(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        reviews = sqlite_handler.get_reviews_by_product(product_id)
        
        return {
            "product": product,
            "reviews": reviews,
            "total_reviews": len(reviews)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/products/{product_id}/reviews", summary="Get Product Reviews")
async def get_product_reviews(
    product_id: int = Path(..., description="Product ID"),
    limit: int = Query(50, ge=1, le=200, description="Number of reviews to return"),
    offset: int = Query(0, ge=0, description="Number of reviews to skip")
):
    """Get reviews for a specific product"""
    try:
        # Check if product exists
        product = sqlite_handler.get_product(product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        
        reviews = sqlite_handler.get_reviews_by_product(
            product_id, limit=limit, offset=offset
        )
        total = sqlite_handler.get_reviews_count_by_product(product_id)
        
        return {
            "product_id": product_id,
            "reviews": reviews,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < total
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/analytics/summary", summary="Get Analytics Summary")
async def get_analytics_summary():
    """Get analytics summary from DuckDB"""
    try:
        summary = duckdb_handler.get_analytics_summary()
        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analytics error: {str(e)}")

@app.delete("/jobs/{job_id}", summary="Delete Job")
async def delete_job(job_id: str = Path(..., description="Job ID")):
    """Delete a background job (completed or failed only)"""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    
    job = jobs[job_id]
    if job.status in ["pending", "running"]:
        raise HTTPException(
            status_code=400, 
            detail="Cannot delete running or pending job"
        )
    
    del jobs[job_id]
    return {"message": "Job deleted successfully"}

@app.delete("/jobs", summary="Clean Up Jobs")
async def cleanup_jobs():
    """Clean up completed and failed jobs"""
    global jobs
    initial_count = len(jobs)
    
    # Keep only pending and running jobs
    jobs = {
        job_id: job for job_id, job in jobs.items() 
        if job.status in ["pending", "running"]
    }
    
    cleaned_count = initial_count - len(jobs)
    
    return {
        "message": f"Cleaned up {cleaned_count} jobs",
        "remaining_jobs": len(jobs)
    }

# Large-Scale Analysis Endpoints

@app.post("/analytics/load-amazon-dataset", summary="Load Amazon UK Dataset")
async def load_amazon_dataset(
    background_tasks: BackgroundTasks,
    dataset_path: Optional[str] = Query(None, description="Path to Amazon UK CSV dataset")
):
    """
    Load the Amazon UK products dataset for large-scale analysis
    
    - **dataset_path**: Optional path to CSV file. If not provided, will look for standard locations
    """
    try:
        if not large_scale_analyzer:
            raise HTTPException(status_code=500, detail="Large scale analyzer not initialized")
        
        # Check if dataset needs to be downloaded
        dataset_available = large_scale_analyzer.download_and_prepare_dataset()
        
        if dataset_path:
            large_scale_analyzer.dataset_path = dataset_path
        
        # Start background loading if dataset is large
        import uuid
        job_id = str(uuid.uuid4())
        
        job_status = JobStatus(
            job_id=job_id,
            status="pending",
            created_at=datetime.now()
        )
        jobs[job_id] = job_status
        
        # Add background task for dataset loading
        background_tasks.add_task(
            load_dataset_background,
            job_id=job_id,
            analyzer=large_scale_analyzer
        )
        
        return {
            "job_id": job_id,
            "status": "pending",
            "message": "Dataset loading started",
            "dataset_available": dataset_available,
            "check_status_url": f"/jobs/{job_id}",
            "info": "For full 2M+ row analysis, ensure Amazon UK dataset is downloaded"
        }
        
    except Exception as e:
        logger.error(f"Error loading Amazon dataset: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def load_dataset_background(job_id: str, analyzer: LargeScaleAnalyzer):
    """Load dataset in background"""
    try:
        jobs[job_id].status = "running"
        
        # Load dataset with optimizations
        success = analyzer.load_dataset_optimized()
        
        if success:
            jobs[job_id].status = "completed"
            jobs[job_id].result = {
                "success": True,
                "message": "Dataset loaded successfully",
                "dataset_path": analyzer.dataset_path
            }
        else:
            jobs[job_id].status = "failed"
            jobs[job_id].error = "Failed to load dataset"
            
        jobs[job_id].completed_at = datetime.now()
        
    except Exception as e:
        logger.error(f"Background dataset loading error: {e}")
        jobs[job_id].status = "failed"
        jobs[job_id].error = str(e)
        jobs[job_id].completed_at = datetime.now()

@app.post("/analytics/run-comprehensive-analysis", summary="Run Large-Scale Category Analysis")
async def run_comprehensive_analysis(
    background_tasks: BackgroundTasks,
    export_results: bool = Query(True, description="Export results to files"),
    cache_results: bool = Query(True, description="Cache results for faster access")
):
    """
    Run comprehensive category-based analysis on the loaded Amazon dataset
    
    Optimized for 2+ million rows with advanced statistical insights
    """
    try:
        if not large_scale_analyzer:
            raise HTTPException(status_code=500, detail="Large scale analyzer not initialized")
        
        import uuid
        job_id = str(uuid.uuid4())
        
        job_status = JobStatus(
            job_id=job_id,
            status="pending",
            created_at=datetime.now()
        )
        jobs[job_id] = job_status
        
        # Add background task for analysis
        background_tasks.add_task(
            run_analysis_background,
            job_id=job_id,
            analyzer=large_scale_analyzer,
            export_results=export_results,
            cache_results=cache_results
        )
        
        return {
            "job_id": job_id,
            "status": "pending", 
            "message": "Large-scale analysis started",
            "check_status_url": f"/jobs/{job_id}",
            "estimated_duration": "2-5 minutes for 2M+ rows"
        }
        
    except Exception as e:
        logger.error(f"Error starting comprehensive analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def run_analysis_background(job_id: str, analyzer: LargeScaleAnalyzer, 
                                export_results: bool, cache_results: bool):
    """Run comprehensive analysis in background"""
    try:
        jobs[job_id].status = "running"
        
        # Run comprehensive analysis
        results = analyzer.run_comprehensive_analysis(cache_results=cache_results)
        
        if "error" in results:
            jobs[job_id].status = "failed"
            jobs[job_id].error = results["error"]
        else:
            # Export results if requested
            if export_results:
                exported_files = analyzer.export_results(results)
                results["exported_files"] = exported_files
            
            jobs[job_id].status = "completed"
            jobs[job_id].result = results
            
        jobs[job_id].completed_at = datetime.now()
        
    except Exception as e:
        logger.error(f"Background analysis error: {e}")
        jobs[job_id].status = "failed"
        jobs[job_id].error = str(e)
        jobs[job_id].completed_at = datetime.now()

@app.get("/analytics/quick-analysis", summary="Get Quick Analysis Summary")
async def get_quick_analysis():
    """
    Get a quick analysis summary from the current dataset
    
    Returns key metrics without running full comprehensive analysis
    """
    try:
        if not duckdb_handler:
            raise HTTPException(status_code=500, detail="DuckDB handler not initialized")
        
        # Check if data exists
        conn = duckdb_handler.connect()
        row_count = conn.execute("SELECT COUNT(*) FROM amazon_products").fetchone()[0]
        
        if row_count == 0:
            return {
                "message": "No data loaded. Use /analytics/load-amazon-dataset first",
                "dataset_size": 0
            }
        
        # Get quick insights
        category_analysis = duckdb_handler.get_category_analysis()
        statistical_insights = duckdb_handler.get_statistical_insights()
        
        # Return summary
        return get_analysis_summary({
            "analysis_metadata": {
                "dataset_size": row_count,
                "categories_analyzed": len(category_analysis),
                "timestamp": datetime.now().isoformat()
            },
            "performance_summary": {
                "top_performing_categories": category_analysis.head(3).to_dict('records') if not category_analysis.empty else []
            },
            "key_findings": [
                f"Analyzing {row_count:,} products across {len(category_analysis)} categories",
                f"Overall average rating: {statistical_insights.get('overall_average_rating', 0):.2f}",
                f"High-rated products: {statistical_insights.get('quality_distribution', {}).get('high_rated_percentage', 0):.1f}%"
            ]
        })
        
    except Exception as e:
        logger.error(f"Error in quick analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analytics/dataset-info", summary="Get Dataset Information")
async def get_dataset_info():
    """Get information about the currently loaded dataset"""
    try:
        if not duckdb_handler:
            raise HTTPException(status_code=500, detail="DuckDB handler not initialized")
        
        conn = duckdb_handler.connect()
        
        # Get basic dataset info
        dataset_info = conn.execute("""
            SELECT 
                COUNT(*) as total_products,
                COUNT(DISTINCT category) as unique_categories,
                COUNT(*) FILTER (WHERE rating IS NOT NULL AND rating > 0) as products_with_ratings,
                MIN(rating) as min_rating,
                MAX(rating) as max_rating,
                AVG(rating) as avg_rating
            FROM amazon_products
        """).fetchone()
        
        if not dataset_info or dataset_info[0] == 0:
            return {
                "message": "No dataset loaded",
                "dataset_loaded": False,
                "total_products": 0
            }
        
        # Get top categories
        top_categories = conn.execute("""
            SELECT category, COUNT(*) as count
            FROM amazon_products 
            WHERE category IS NOT NULL
            GROUP BY category 
            ORDER BY count DESC 
            LIMIT 10
        """).fetchall()
        
        return {
            "dataset_loaded": True,
            "total_products": dataset_info[0],
            "unique_categories": dataset_info[1],
            "products_with_ratings": dataset_info[2],
            "rating_stats": {
                "min": dataset_info[3],
                "max": dataset_info[4], 
                "average": round(dataset_info[5], 2) if dataset_info[5] else None
            },
            "top_categories": [
                {"category": cat[0], "product_count": cat[1]} 
                for cat in top_categories
            ],
            "data_completeness": {
                "rating_completeness": round((dataset_info[2] / dataset_info[0]) * 100, 1) if dataset_info[0] > 0 else 0
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting dataset info: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Exception handlers
@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "type": "internal_error",
            "timestamp": datetime.now().isoformat()
        }
    )

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
