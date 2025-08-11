"""
Category-based analysis for Amazon UK products dataset (Bonus Challenge)
"""
import os
import logging
import requests
import zipfile
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

from ..storage.duckdb_handler import DuckDBHandler

logger = logging.getLogger(__name__)

class CategoryAnalyzer:
    """Category-based product analysis using DuckDB"""
    
    def __init__(self, duckdb_path: str = "data/analytics.duckdb"):
        self.duckdb_handler = DuckDBHandler(duckdb_path)
        self.dataset_url = "https://www.kaggle.com/datasets/asaniczka/amazon-uk-products-dataset-2023"
        self.dataset_path = "data/amazon-uk-products-dataset-2023.csv"
    
    def download_dataset(self) -> bool:
        """
        Download Amazon UK dataset from Kaggle
        Note: This requires Kaggle API setup or manual download
        """
        logger.info("Dataset download instructions:")
        logger.info("1. Go to: https://www.kaggle.com/datasets/asaniczka/amazon-uk-products-dataset-2023")
        logger.info("2. Download the CSV file")
        logger.info(f"3. Place it at: {self.dataset_path}")
        logger.info("4. Alternatively, use Kaggle API: kaggle datasets download -d asaniczka/amazon-uk-products-dataset-2023")
        
        # Check if dataset already exists
        if os.path.exists(self.dataset_path):
            logger.info(f"Dataset found at {self.dataset_path}")
            return True
        
        # Try to find dataset in common locations
        possible_paths = [
            "data/amazon-uk-products-dataset-2023.csv",
            "amazon-uk-products-dataset-2023.csv",
            "data/products.csv",
            "products.csv"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Found dataset at {path}")
                self.dataset_path = path
                return True
        
        logger.warning("Dataset not found. Please download manually.")
        return False
    
    def load_dataset(self) -> bool:
        """Load the Amazon UK dataset into DuckDB"""
        if not os.path.exists(self.dataset_path):
            logger.error(f"Dataset not found at {self.dataset_path}")
            return False
        
        try:
            logger.info(f"Loading dataset from {self.dataset_path}")
            success = self.duckdb_handler.load_amazon_dataset(self.dataset_path)
            
            if success:
                logger.info("Dataset loaded successfully into DuckDB")
                return True
            else:
                logger.error("Failed to load dataset")
                return False
                
        except Exception as e:
            logger.error(f"Error loading dataset: {e}")
            return False
    
    def perform_category_analysis(self) -> Dict[str, Any]:
        """Perform comprehensive category-based analysis"""
        logger.info("Starting category-based analysis...")
        
        try:
            # Get category statistics
            category_stats = self.duckdb_handler.get_category_analysis()
            
            if category_stats.empty:
                logger.warning("No data available for analysis")
                return {"error": "No data available"}
            
            # Get statistical insights
            insights = self.duckdb_handler.get_statistical_insights()
            
            # Get rating distribution
            rating_distribution = self.duckdb_handler.get_rating_distribution()
            
            # Generate summary
            summary = self._generate_analysis_summary(category_stats, insights)
            
            results = {
                "analysis_timestamp": datetime.now().isoformat(),
                "summary": summary,
                "category_statistics": category_stats.to_dict('records'),
                "statistical_insights": insights,
                "rating_distribution": rating_distribution.to_dict('records') if not rating_distribution.empty else [],
                "recommendations": self._generate_recommendations(category_stats, insights)
            }
            
            logger.info("Category analysis completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"Error in category analysis: {e}")
            return {"error": str(e)}
    
    def _generate_analysis_summary(self, category_stats: pd.DataFrame, insights: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a summary of the analysis"""
        if category_stats.empty:
            return {"error": "No data to summarize"}
        
        # Find categories with highest and lowest ratings
        highest_rated = category_stats.loc[category_stats['avg_rating'].idxmax()]
        lowest_rated = category_stats.loc[category_stats['avg_rating'].idxmin()]
        
        # Find categories with highest and lowest variability
        highest_variability = category_stats.loc[category_stats['rating_stddev'].idxmax()]
        lowest_variability = category_stats.loc[category_stats['rating_stddev'].idxmin()]
        
        return {
            "total_categories_analyzed": len(category_stats),
            "overall_average_rating": insights.get("overall_average_rating", 0),
            "highest_rated_category": {
                "category": highest_rated['category'],
                "average_rating": round(highest_rated['avg_rating'], 3),
                "product_count": int(highest_rated['product_count'])
            },
            "lowest_rated_category": {
                "category": lowest_rated['category'],
                "average_rating": round(lowest_rated['avg_rating'], 3),
                "product_count": int(lowest_rated['product_count'])
            },
            "highest_variability_category": {
                "category": highest_variability['category'],
                "standard_deviation": round(highest_variability['rating_stddev'], 3),
                "variance": round(highest_variability['rating_variance'], 3)
            },
            "lowest_variability_category": {
                "category": lowest_variability['category'],
                "standard_deviation": round(lowest_variability['rating_stddev'], 3),
                "variance": round(lowest_variability['rating_variance'], 3)
            }
        }
    
    def _generate_recommendations(self, category_stats: pd.DataFrame, insights: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on analysis"""
        recommendations = []
        
        if category_stats.empty:
            return ["No data available for recommendations"]
        
        # Overall rating threshold
        overall_avg = insights.get("overall_average_rating", 4.0)
        
        # High performing categories
        high_performers = category_stats[category_stats['avg_rating'] > overall_avg + 0.2]
        if not high_performers.empty:
            top_category = high_performers.iloc[0]['category']
            recommendations.append(f"Focus on '{top_category}' category - consistently high ratings ({high_performers.iloc[0]['avg_rating']:.2f})")
        
        # Low performing categories
        low_performers = category_stats[category_stats['avg_rating'] < overall_avg - 0.2]
        if not low_performers.empty:
            bottom_category = low_performers.iloc[-1]['category']
            recommendations.append(f"Investigate '{bottom_category}' category - below average ratings ({low_performers.iloc[-1]['avg_rating']:.2f})")
        
        # High variability categories (inconsistent quality)
        high_variability = category_stats[category_stats['rating_stddev'] > 1.0]
        if not high_variability.empty:
            variable_category = high_variability.iloc[0]['category']
            recommendations.append(f"Review quality control for '{variable_category}' - high rating variability (σ={high_variability.iloc[0]['rating_stddev']:.2f})")
        
        # Low variability categories (consistent quality)
        low_variability = category_stats[category_stats['rating_stddev'] < 0.5]
        if not low_variability.empty:
            consistent_category = low_variability.iloc[0]['category']
            recommendations.append(f"'{consistent_category}' shows consistent quality - good benchmark for other categories")
        
        # Z-score insights
        z_score_analysis = insights.get("z_score_analysis", [])
        significant_categories = [cat for cat in z_score_analysis if cat.get("significance_level") == "Significant"]
        
        for cat in significant_categories[:2]:  # Top 2 significant categories
            if cat.get("z_score", 0) > 2:
                recommendations.append(f"'{cat['category']}' significantly outperforms average (z-score: {cat['z_score']:.2f})")
            elif cat.get("z_score", 0) < -2:
                recommendations.append(f"'{cat['category']}' significantly underperforms average (z-score: {cat['z_score']:.2f})")
        
        return recommendations[:5]  # Limit to top 5 recommendations
    
    def generate_insights_report(self, results: Dict[str, Any], output_file: str = "data/analysis_report.txt") -> bool:
        """Generate a human-readable insights report"""
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            
            with open(output_file, 'w') as f:
                f.write("=" * 60 + "\n")
                f.write("AMAZON UK PRODUCTS - CATEGORY ANALYSIS REPORT\n")
                f.write("=" * 60 + "\n\n")
                
                # Analysis timestamp
                timestamp = results.get("analysis_timestamp", "Unknown")
                f.write(f"Analysis Date: {timestamp}\n\n")
                
                # Summary section
                summary = results.get("summary", {})
                if summary and "error" not in summary:
                    f.write("EXECUTIVE SUMMARY\n")
                    f.write("-" * 20 + "\n")
                    f.write(f"Total Categories Analyzed: {summary.get('total_categories_analyzed', 0)}\n")
                    f.write(f"Overall Average Rating: {summary.get('overall_average_rating', 0):.3f}\n\n")
                    
                    f.write("TOP PERFORMING CATEGORY\n")
                    highest = summary.get("highest_rated_category", {})
                    f.write(f"Category: {highest.get('category', 'N/A')}\n")
                    f.write(f"Average Rating: {highest.get('average_rating', 0):.3f}\n")
                    f.write(f"Product Count: {highest.get('product_count', 0):,}\n\n")
                    
                    f.write("LOWEST PERFORMING CATEGORY\n")
                    lowest = summary.get("lowest_rated_category", {})
                    f.write(f"Category: {lowest.get('category', 'N/A')}\n")
                    f.write(f"Average Rating: {lowest.get('average_rating', 0):.3f}\n")
                    f.write(f"Product Count: {lowest.get('product_count', 0):,}\n\n")
                    
                    f.write("RATING VARIABILITY\n")
                    f.write("-" * 20 + "\n")
                    high_var = summary.get("highest_variability_category", {})
                    f.write(f"Most Variable: {high_var.get('category', 'N/A')} (σ={high_var.get('standard_deviation', 0):.3f})\n")
                    low_var = summary.get("lowest_variability_category", {})
                    f.write(f"Most Consistent: {low_var.get('category', 'N/A')} (σ={low_var.get('standard_deviation', 0):.3f})\n\n")
                
                # Recommendations
                recommendations = results.get("recommendations", [])
                if recommendations:
                    f.write("KEY RECOMMENDATIONS\n")
                    f.write("-" * 20 + "\n")
                    for i, rec in enumerate(recommendations, 1):
                        f.write(f"{i}. {rec}\n")
                    f.write("\n")
                
                # Statistical insights
                insights = results.get("statistical_insights", {})
                if insights:
                    f.write("STATISTICAL ANALYSIS\n")
                    f.write("-" * 20 + "\n")
                    f.write(f"Total Products: {insights.get('total_products', 0):,}\n")
                    f.write(f"Overall Standard Deviation: {insights.get('overall_stddev', 0):.3f}\n")
                    
                    z_analysis = insights.get("z_score_analysis", [])
                    if z_analysis:
                        f.write("\nZ-Score Analysis (Categories vs. Overall Mean):\n")
                        for cat in z_analysis[:5]:  # Top 5
                            significance = cat.get("significance_level", "Normal")
                            z_score = cat.get("z_score", 0)
                            f.write(f"  {cat.get('category', 'Unknown')}: z={z_score:.2f} ({significance})\n")
                
                # Category statistics table
                category_stats = results.get("category_statistics", [])
                if category_stats:
                    f.write("\n" + "=" * 60 + "\n")
                    f.write("DETAILED CATEGORY STATISTICS\n")
                    f.write("=" * 60 + "\n")
                    
                    # Header
                    f.write(f"{'Category':<20} {'Count':<8} {'Avg':<6} {'StdDev':<8} {'Min':<6} {'Max':<6} {'High%':<8}\n")
                    f.write("-" * 70 + "\n")
                    
                    # Sort by average rating
                    sorted_stats = sorted(category_stats, key=lambda x: x.get('avg_rating', 0), reverse=True)
                    
                    for stat in sorted_stats:
                        category = stat.get('category', 'Unknown')[:19]  # Truncate long names
                        count = stat.get('product_count', 0)
                        avg = stat.get('avg_rating', 0)
                        stddev = stat.get('rating_stddev', 0)
                        min_rating = stat.get('min_rating', 0)
                        max_rating = stat.get('max_rating', 0)
                        high_pct = stat.get('high_rating_percentage', 0)
                        
                        f.write(f"{category:<20} {count:<8,} {avg:<6.2f} {stddev:<8.3f} {min_rating:<6.1f} {max_rating:<6.1f} {high_pct:<8.1f}\n")
                
                f.write("\n" + "=" * 60 + "\n")
                f.write("End of Report\n")
                f.write("=" * 60 + "\n")
            
            logger.info(f"Analysis report generated: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error generating report: {e}")
            return False
    
    def run_analysis(self, generate_report: bool = True, export_csv: bool = True) -> Dict[str, Any]:
        """Run the complete category analysis pipeline"""
        logger.info("Starting Amazon UK products category analysis...")
        
        # Step 1: Check/download dataset
        if not self.download_dataset():
            return {"error": "Dataset not available. Please download manually."}
        
        # Step 2: Load dataset into DuckDB
        if not self.load_dataset():
            return {"error": "Failed to load dataset into DuckDB"}
        
        # Step 3: Perform analysis
        results = self.perform_category_analysis()
        
        if "error" in results:
            return results
        
        # Step 4: Generate report
        if generate_report:
            self.generate_insights_report(results)
        
        # Step 5: Export CSV files
        if export_csv:
            self.duckdb_handler.export_analysis_results()
        
        logger.info("Category analysis completed successfully!")
        
        # Add file paths to results
        results["output_files"] = {
            "report": "data/analysis_report.txt",
            "category_analysis": "data/analysis_results/category_analysis.csv",
            "rating_distribution": "data/analysis_results/rating_distribution.csv",
            "statistical_insights": "data/analysis_results/statistical_insights.json"
        }
        
        return results
    
    def cleanup(self):
        """Clean up resources"""
        if self.duckdb_handler:
            self.duckdb_handler.close()

# Convenience function for running analysis
def run_category_analysis(duckdb_path: str = "data/analytics.duckdb") -> Dict[str, Any]:
    """Run category analysis with default settings"""
    analyzer = CategoryAnalyzer(duckdb_path)
    try:
        return analyzer.run_analysis()
    finally:
        analyzer.cleanup()
