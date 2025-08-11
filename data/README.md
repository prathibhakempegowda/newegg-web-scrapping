# Data Directory

This directory contains the project's databases and datasets.

## Contents

### Databases
- `newegg_scraper.db` - SQLite database for scraped product data
- `analytics.duckdb` - DuckDB database for analytics processing

### Datasets (Downloaded)
- `amz_uk_processed_data.csv` - Amazon UK dataset (downloaded via kagglehub)

## Notes

- Large CSV files are excluded from Git via `.gitignore`
- Database files contain sample data and structure
- Use `download_amazon_dataset.py` to download the full Amazon UK dataset
- The analytics database is automatically created when running analysis

## Usage

The data directory is automatically created when:
1. Running the scraper for the first time
2. Starting the API server
3. Downloading datasets via the provided scripts
