import kagglehub

#!/usr/bin/env python3
"""
Download Amazon UK Products Dataset 2023 using kagglehub
"""

import kagglehub
import os
import shutil
from pathlib import Path

def download_amazon_dataset():
    """Download Amazon UK dataset using kagglehub"""
    try:
        print("Downloading Amazon UK Products Dataset 2023...")
        
        # Download latest version
        path = kagglehub.dataset_download("asaniczka/amazon-uk-products-dataset-2023")
        
        print("Path to dataset files:", path)
        
        # List files in the downloaded directory
        if os.path.exists(path):
            files = os.listdir(path)
            print(f"Downloaded files: {files}")
            
            # Copy to our data directory for easier access
            data_dir = Path("data")
            data_dir.mkdir(exist_ok=True)
            
            for file in files:
                if file.endswith('.csv'):
                    src = os.path.join(path, file)
                    dst = os.path.join(data_dir, file)
                    shutil.copy2(src, dst)
                    print(f"Copied {file} to {dst}")
                    
                    # Get file size for validation
                    size_mb = os.path.getsize(dst) / (1024 * 1024)
                    print(f"File size: {size_mb:.1f} MB")
        
        return path
        
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        return None

if __name__ == "__main__":
    # Download latest version
    path = kagglehub.dataset_download("asaniczka/amazon-uk-products-dataset-2023")
    print("Path to dataset files:", path)
    
    # Run the download function
    download_amazon_dataset()