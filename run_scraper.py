"""
Simple runner script for the Newegg web scraper
"""
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.main import main

if __name__ == "__main__":
    # Default configuration for quick testing
    default_args = [
        "--url", "https://www.newegg.com/amd-ryzen-7-9000-series-ryzen-7-9800x3d-granite-ridge-zen-5-socket-am5-desktop-cpu-processor/p/N82E16819113877",
        "--max-reviews", "50",
        "--method", "fallback",
        "--log-level", "INFO"
    ]
    
    # Use provided args or defaults
    if len(sys.argv) == 1:
        sys.argv.extend(default_args)
        print("Using default configuration...")
        print(f"Command: python {' '.join(sys.argv)}")
        print()
    
    # Run the main application
    asyncio.run(main())
