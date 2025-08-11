#!/bin/bash

# Enhanced Web Scraper Setup Script
# This script helps set up the environment for the Newegg web scraper

echo "=========================================="
echo "Enhanced Web Scraper - Environment Setup"
echo "=========================================="

# Check Python version
python_version=$(python3 --version 2>&1)
if [[ $? -eq 0 ]]; then
    echo " Python found: $python_version"
else
    echo " Python 3 not found. Please install Python 3.8 or higher."
    exit 1
fi

# Check if we're in the right directory
if [[ ! -f "requirements.txt" ]]; then
    echo " requirements.txt not found. Please run this script from the project root directory."
    exit 1
fi

# Create virtual environment
echo "🔧 Creating virtual environment..."
python3 -m venv venv

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "🔧 Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "🔧 Installing Python packages..."
pip install -r requirements.txt

# Create necessary directories
echo "🔧 Creating directories..."
mkdir -p data
mkdir -p logs
mkdir -p data/analysis_results

# Check if Chrome is installed (for Selenium)
if command -v google-chrome &> /dev/null || command -v chrome &> /dev/null; then
    echo " Chrome browser found"
else
    echo "  Chrome browser not found. Selenium may not work properly."
    echo "Please install Google Chrome for best results."
fi

# Test the installation
echo " Testing installation..."
python3 test_setup.py

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Activate the virtual environment: source venv/bin/activate"
echo "2. Run the scraper: python run_scraper.py"
echo "3. For help: python src/main.py --help"
echo ""
echo "For bonus analysis (optional):"
echo "1. Download Amazon UK dataset from Kaggle"
echo "2. Place it in data/amazon-uk-products-dataset-2023.csv"
echo "3. Run: python src/main.py --analysis"
echo ""
echo "Happy scraping! "
