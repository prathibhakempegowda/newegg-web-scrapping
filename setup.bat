@echo off
REM Enhanced Web Scraper Setup Script for Windows
REM This script helps set up the environment for the Newegg web scraper

echo ==========================================
echo Enhanced Web Scraper - Environment Setup
echo ==========================================

REM Check Python version
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python not found Please install Python 38 or higher
    pause
    exit /b 1
) else (
    echo Python found
)

REM Check if we're in the right directory
if not exist "requirements.txt" (
    echo requirements.txt not found. Please run this script from the project root directory.
    pause
    exit /b 1
)

REM Create virtual environment
echo Creating virtual environment...
python -m venv venv

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip
echo Upgrading pip...
pip install --upgrade pip

REM Install requirements
echo Installing Python packages...
pip install -r requirements.txt

REM Create necessary directories
echo Creating directories...
if not exist "data" mkdir data
if not exist "logs" mkdir logs
if not exist "data\analysis_results" mkdir data\analysis_results

REM Test the installation
echo Testing installation...
python test_setup.py

echo.
echo ==========================================
echo Setup Complete!
echo ==========================================
echo.
echo Next steps:
echo 1. Activate the virtual environment: venv\Scripts\activate.bat
echo 2. Run the scraper: python run_scraper.py
echo 3. For help: python src\main.py --help
echo.
echo For bonus analysis (optional):
echo 1. Download Amazon UK dataset from Kaggle
echo 2. Place it in data\amazon-uk-products-dataset-2023.csv
echo 3. Run: python src\main.py --analysis
echo.
echo Happy scraping
pause
