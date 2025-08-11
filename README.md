# 🛒 Enterprise Web Scraper & Analytics Platform

A comprehensive web scraping and analytics platform designed for large-scale e-commerce data analysis, featuring multi-strategy scraping, real-time API, and advanced analytics capabilities.

##  Features

###  **Multi-Strategy Web Scraping**
- **Selenium WebDriver**: Dynamic content and JavaScript-heavy sites
- **CloudScraper**: Anti-bot protection bypass
- **aiohttp**: High-performance async scraping
- **Intelligent Fallback**: Automatic strategy switching on failures
- **Rate Limiting**: Configurable delays and respect for robots.txt

###  **FastAPI REST API (14 Endpoints)**
- **Product Management**: CRUD operations for scraped products
- **Advanced Search**: Filter by price, rating, category
- **Analytics Engine**: Real-time statistical analysis
- **Large-Scale Processing**: Handle 2M+ product datasets
- **Background Jobs**: Async processing with status tracking
- **Interactive Documentation**: Auto-generated Swagger UI

###  **Advanced Analytics Engine**
- **DuckDB Integration**: Optimized for large dataset analysis
- **Statistical Insights**: Mean, median, standard deviation, Z-scores
- **Category Performance**: Top/bottom performers, market share
- **Price Analysis**: Pricing trends and distribution
- **Quality Metrics**: Rating analysis and high-rated percentage
- **Export Formats**: JSON, CSV, Excel support

###  **Real Data Integration**
- **Kaggle Integration**: Download real Amazon UK dataset (2.2M products)
- **Data Validation**: Automatic quality checks and cleaning
- **Schema Flexibility**: Handle different data formats
- **Performance Optimization**: Chunked processing for large files

##  **Architecture**

```
web_scrape/
├── 📁 src/
│   ├──  scrapers/          # Multi-strategy scrapers
│   ├──  storage/           # Database handlers (SQLite + DuckDB)
│   ├──  analysis/          # Large-scale analytics engine
│   └──  utils/            # Utilities and helpers
├──  api/                   # FastAPI application
├──  data/                  # Datasets and databases
├──  .vscode/              # VS Code tasks configuration
└──  requirements.txt       # Python dependencies
```

##  **Quick Start**

### 1. **Installation**
```bash
# Clone the repository
git clone <repository-url>
cd web_scrape

# Install dependencies
pip install -r requirements.txt

# Setup environment
cp .env.example .env
# Edit .env with your configuration
   cd web_scrape
   ```

2. **Create a virtual environment:**
   ```bash
```

### 2. **Start the API Server**
# Direct command
cd api && python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 3. **Access Interactive API Documentation**
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **Analytics Dashboard**: http://localhost:8000/dashboard

### 4. **Download Real Dataset (Optional)**
```bash
python download_amazon_dataset.py
```

##  **API Endpoints**

### **Product Management**
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/products/` | List all products with pagination |
| POST | `/products/` | Create new product |
| GET | `/products/{id}` | Get specific product |
| PUT | `/products/{id}` | Update product |
| DELETE | `/products/{id}` | Delete product |

### **Search & Analytics**
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/products/search` | Advanced product search |
| GET | `/products/category/{category}` | Products by category |
| GET | `/analytics/summary` | Quick analytics overview |
| GET | `/analytics/category-analysis` | Detailed category performance |

### **Large-Scale Analytics**
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/analytics/load-amazon-dataset` | Load real Amazon dataset |
| POST | `/analytics/run-comprehensive-analysis` | Full analysis suite |
| GET | `/analytics/quick-analysis` | Fast overview |
| GET | `/analytics/dataset-info` | Dataset information |

### **Background Jobs**
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/jobs/{job_id}` | Check job status |
| GET | `/jobs/` | List all jobs |

##  **Configuration**

### **Environment Variables (.env)**
```env
# Scraping Configuration
USER_AGENT=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36
REQUEST_DELAY=1
MAX_RETRIES=3
TIMEOUT=30

# Database Configuration
DATABASE_URL=sqlite:///./data/newegg_scraper.db
DUCKDB_PATH=./data/analytics.duckdb

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
ENABLE_CORS=true
```

### **VS Code Tasks**
Pre-configured tasks for easy development:
- **Start API Server**: Launch FastAPI with auto-reload
- **Run Scraper**: Execute web scraping
- **Run Tests**: Execute test suite

##  **Analytics Capabilities**

### **Dataset Analysis**
- **Scale**: Handle 2M+ products efficiently
- **Performance**: Optimized DuckDB queries
- **Memory**: Chunked processing for large files
- **Export**: Multiple format support

### **Statistical Insights**
```python
# Example: Category Performance Analysis
{
  "category": "Electronics",
  "product_count": 15420,
  "avg_rating": 4.2,
  "price_range": {"min": 9.99, "max": 2999.99},
  "market_share": 12.5,
  "top_performer": true
}
```

### **Key Metrics**
- **Quality Score**: Percentage of 4+ star products
- **Price Analysis**: Mean, median, standard deviation
- **Market Share**: Category distribution
- **Performance Ranking**: Top/bottom categories
- **Trend Analysis**: Rating and price patterns

##  **Real Data Analysis Results**

### **Amazon UK Dataset (2.2M Products)**
- **Total Products**: 2,222,742 across 296 categories
- **Quality Distribution**: 39.6% high-rated (4+ stars)
- **Price Range**: £0.00 - £100,000 (avg: £94.26)
- **Top Category**: Sports & Outdoors (37.2% market share)
- **Best Rated**: Luxury Food & Drink (4.55 avg rating)

### **Key Insights**
- **Premium Categories**: 73 categories with >80% high-rated products
- **Market Segments**: Clear luxury vs budget segmentation
- **Quality Leaders**: Computer Memory (86.7% excellent rating)
- **Price Extremes**: From £6.93 (Office Papers) to £1,088 (Laptops)

##  **Development**

### **Project Structure**
```python
# Core Components
src/scrapers/          # Multi-strategy scraping engines
src/storage/           # Database abstraction layer
src/analysis/          # Analytics and insights engine
api/                   # REST API with FastAPI
```

### **Key Technologies**
- **Backend**: Python 3.8+, FastAPI, SQLAlchemy
- **Databases**: SQLite (OLTP), DuckDB (OLAP)
- **Scraping**: Selenium, aiohttp, CloudScraper
- **Analytics**: Pandas, NumPy, Statistical analysis
- **API**: OpenAPI/Swagger, async/await patterns

### **Performance Optimizations**
- **Chunked Processing**: Handle large datasets efficiently
- **Async Operations**: Non-blocking I/O operations
- **Database Indexing**: Optimized query performance
- **Memory Management**: Efficient data processing
- **Caching**: Strategic data caching

##  **Scalability**

### **Horizontal Scaling**
- **Microservices Ready**: API can be containerized
- **Database Sharding**: Support for distributed databases
- **Load Balancing**: Multiple API instances
- **Queue System**: Background job processing

### **Performance Benchmarks**
- **2M+ Products**: Analyzed in under 5 minutes
- **API Response**: <100ms for most endpoints
- **Memory Usage**: Optimized for large datasets
- **Concurrent Users**: Supports multiple simultaneous requests

##  **Security & Best Practices**

### **Web Scraping Ethics**
- **Rate Limiting**: Configurable delays between requests
- **robots.txt Compliance**: Automatic checking
- **User-Agent Rotation**: Avoid detection
- **Error Handling**: Graceful failure recovery

### **Data Privacy**
- **No Personal Data**: Only public product information
- **Data Anonymization**: Remove sensitive information
- **Compliance**: GDPR and data protection awareness

##  **License**

This project is licensed under the MIT License - see the LICENSE file for details.

##  **Acknowledgments**

- **Amazon UK Dataset**: Real e-commerce data for analysis
- **FastAPI**: High-performance web framework
- **DuckDB**: Analytical database for large-scale processing
- **Selenium**: Web automation and scraping capabilities

##  **Support**

For questions, issues, or contributions:
- **Issues**: Open a GitHub issue
- **Documentation**: Check the API docs at `/docs`
- **Examples**: See usage examples in the test files

---


