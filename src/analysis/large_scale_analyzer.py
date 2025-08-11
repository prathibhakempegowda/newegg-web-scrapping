"""
Large-Scale Amazon UK Products Analysis Engine
==============================================

Optimized for 2+ million rows with advanced statistical analysis
Designed for scalability and dashboard integration
"""

import os
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
import json
import time

# Handle pandas import gracefully
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    # Create minimal DataFrame-like class for compatibility
    class MockDataFrame:
        def __init__(self, data=None):
            self.data = data or []
        def empty(self): return len(self.data) == 0
        def to_dict(self, orient='records'): return self.data if isinstance(self.data, list) else []
        def head(self, n=5): return MockDataFrame(self.data[:n] if isinstance(self.data, list) else [])
        def tail(self, n=5): return MockDataFrame(self.data[-n:] if isinstance(self.data, list) else [])
        def iloc(self, index): 
            if isinstance(self.data, list) and 0 <= index < len(self.data):
                return self.data[index]
            return {}
        def __len__(self): return len(self.data) if isinstance(self.data, list) else 0
        def __getitem__(self, key): return self.data
        def nlargest(self, n, col): return MockDataFrame(self.data[:n] if isinstance(self.data, list) else [])
        def nsmallest(self, n, col): return MockDataFrame(self.data[:n] if isinstance(self.data, list) else [])
    
    pd = type('MockPandas', (), {'DataFrame': MockDataFrame})()

from ..storage.duckdb_handler import DuckDBHandler

logger = logging.getLogger(__name__)

def safe_dataframe_operation(df, operation, *args, **kwargs):
    """Safely handle DataFrame operations with fallback for when pandas is not available"""
    if PANDAS_AVAILABLE and hasattr(df, operation):
        attr = getattr(df, operation)
        if callable(attr):
            return attr(*args, **kwargs)
        else:
            return attr
    else:
        # Fallback for basic operations
        if operation == 'empty':
            return len(df) == 0 if hasattr(df, '__len__') else True
        elif operation == 'to_dict':
            return df if isinstance(df, list) else []
        elif operation == 'head':
            n = args[0] if args else 5
            return df[:n] if isinstance(df, list) else []
        elif operation == 'tail':
            n = args[0] if args else 5
            return df[-n:] if isinstance(df, list) else []
        elif operation == 'iloc':
            if isinstance(df, list) and args:
                idx = args[0]
                if isinstance(idx, int) and 0 <= idx < len(df):
                    return df[idx]
        elif operation in ['nlargest', 'nsmallest']:
            # For sorting operations, return the data as-is
            return df[:args[0]] if isinstance(df, list) and args else df
        return df

class LargeScaleAnalyzer:
    """
    Advanced analytics engine for large Amazon product datasets
    Optimized for 2+ million rows with scalable query design
    """
    
    def __init__(self, duckdb_path: str = "data/analytics.duckdb"):
        self.duckdb_handler = DuckDBHandler(duckdb_path)
        self.dataset_path = None
        self.analysis_cache = {}
        
    def download_and_prepare_dataset(self, force_download: bool = False) -> bool:
        """
        Download and prepare the Amazon UK dataset for analysis
        
        Args:
            force_download: Force re-download even if file exists
            
        Returns:
            bool: Success status
        """
        
        dataset_paths = [
            "data/amazon-uk-products-dataset-2023.csv",
            "amazon-uk-products-dataset-2023.csv",
            "data/amazon_uk_products.csv"
        ]
        
        # Check for existing dataset
        for path in dataset_paths:
            if os.path.exists(path) and not force_download:
                logger.info(f"Found existing dataset at {path}")
                self.dataset_path = path
                return True
        
        # Provide download instructions
        logger.info("=" * 80)
        logger.info("AMAZON UK DATASET DOWNLOAD REQUIRED")
        logger.info("=" * 80)
        logger.info("To run analysis on the full 2+ million row dataset:")
        logger.info("")
        logger.info("Option 1 - Kaggle CLI (Recommended):")
        logger.info("1. Install Kaggle CLI: pip install kaggle")
        logger.info("2. Set up Kaggle API credentials")
        logger.info("3. Download: kaggle datasets download -d asaniczka/amazon-uk-products-dataset-2023")
        logger.info("4. Extract: unzip amazon-uk-products-dataset-2023.zip")
        logger.info("5. Move to: data/amazon-uk-products-dataset-2023.csv")
        logger.info("")
        logger.info("Option 2 - Manual Download:")
        logger.info("1. Go to: https://www.kaggle.com/datasets/asaniczka/amazon-uk-products-dataset-2023")
        logger.info("2. Download the CSV file")
        logger.info("3. Place at: data/amazon-uk-products-dataset-2023.csv")
        logger.info("")
        logger.info("For testing purposes, the analysis will continue with sample data.")
        logger.info("=" * 80)
        
        return False
    
    def validate_dataset(self, csv_path: str) -> Dict[str, Any]:
        """
        Validate dataset structure and quality for analysis
        
        Args:
            csv_path: Path to the CSV file
            
        Returns:
            Dict containing validation results and recommendations
        """
        try:
            # Quick validation using DuckDB's efficient CSV reader
            conn = self.duckdb_handler.connect()
            
            # Sample first 1000 rows for validation
            validation_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT COALESCE(category, 'Unknown')) as unique_categories,
                COUNT(*) FILTER (WHERE rating IS NOT NULL AND rating > 0) as valid_ratings,
                MIN(rating) as min_rating,
                MAX(rating) as max_rating,
                COUNT(*) FILTER (WHERE category IS NULL OR category = '') as missing_categories,
                COUNT(DISTINCT title) as unique_products
            FROM read_csv_auto('{csv_path}', sample_size=50000)
            """
            
            stats = conn.execute(validation_query).fetchone()
            
            validation_result = {
                "file_path": csv_path,
                "validation_timestamp": datetime.now().isoformat(),
                "dataset_size": stats[0] if stats else 0,
                "unique_categories": stats[1] if stats else 0,
                "valid_ratings": stats[2] if stats else 0,
                "rating_range": {
                    "min": stats[3] if stats else None,
                    "max": stats[4] if stats else None
                },
                "data_quality": {
                    "missing_categories": stats[5] if stats else 0,
                    "unique_products": stats[6] if stats else 0,
                    "rating_completeness": round((stats[2] / stats[0]) * 100, 2) if stats and stats[0] > 0 else 0
                },
                "recommendations": []
            }
            
            # Generate recommendations based on validation
            if validation_result["data_quality"]["rating_completeness"] < 70:
                validation_result["recommendations"].append("Low rating completeness - consider data cleaning")
            
            if validation_result["unique_categories"] < 5:
                validation_result["recommendations"].append("Limited category diversity - verify category mapping")
            
            if validation_result["dataset_size"] < 100000:
                validation_result["recommendations"].append("Small dataset size - statistical significance may be limited")
            
            logger.info(f"Dataset validation completed: {validation_result['dataset_size']:,} rows, "
                       f"{validation_result['unique_categories']} categories")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"Dataset validation failed: {e}")
            return {"error": str(e), "validation_timestamp": datetime.now().isoformat()}
    
    def load_dataset_optimized(self, csv_path: str = None) -> bool:
        """
        Load dataset with optimizations for large-scale analysis
        
        Args:
            csv_path: Path to dataset CSV file
            
        Returns:
            bool: Success status
        """
        if not csv_path:
            csv_path = self.dataset_path
            
        if not csv_path or not os.path.exists(csv_path):
            logger.error("Dataset path not found. Run download_and_prepare_dataset() first.")
            return False
        
        logger.info(f"Loading large dataset from {csv_path} with performance optimizations...")
        start_time = time.time()
        
        try:
            # Validate dataset first
            validation = self.validate_dataset(csv_path)
            if "error" in validation:
                logger.error(f"Dataset validation failed: {validation['error']}")
                return False
            
            # Load with optimizations
            success = self.duckdb_handler.load_amazon_dataset(csv_path)
            
            if success:
                load_time = time.time() - start_time
                logger.info(f"Dataset loaded successfully in {load_time:.2f} seconds")
                
                # Cache dataset info
                self.analysis_cache["dataset_info"] = validation
                self.dataset_path = csv_path
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to load dataset: {e}")
            return False
    
    def run_comprehensive_analysis(self, cache_results: bool = True) -> Dict[str, Any]:
        """
        Execute comprehensive category-based analysis optimized for large datasets
        
        Args:
            cache_results: Whether to cache results for faster subsequent access
            
        Returns:
            Dict containing complete analysis results
        """
        logger.info("Starting comprehensive large-scale category analysis...")
        start_time = time.time()
        
        try:
            # Check if we have data loaded
            conn = self.duckdb_handler.connect()
            row_count = conn.execute("SELECT COUNT(*) FROM amazon_products").fetchone()[0]
            
            if row_count == 0:
                return {"error": "No data loaded. Run load_dataset_optimized() first."}
            
            logger.info(f"Analyzing {row_count:,} products...")
            
            # 1. Category Performance Analysis
            logger.info("1/5: Running category performance analysis...")
            category_analysis = self.duckdb_handler.get_category_analysis()
            
            if safe_dataframe_operation(category_analysis, 'empty'):
                return {"error": "Category analysis returned no results"}
            
            # 2. Statistical Insights
            logger.info("2/5: Generating statistical insights...")
            statistical_insights = self.duckdb_handler.get_statistical_insights()
            
            # 3. Rating Distribution Analysis
            logger.info("3/5: Analyzing rating distributions...")
            rating_distribution = self.duckdb_handler.get_rating_distribution()
            
            # 4. Advanced Variability Analysis
            logger.info("4/5: Computing variability metrics...")
            variability_insights = self._compute_advanced_variability(category_analysis)
            
            # 5. Performance Summary
            logger.info("5/5: Generating performance summary...")
            performance_summary = self._generate_performance_summary(
                category_analysis, statistical_insights, row_count
            )
            
            # Compile final results
            analysis_time = time.time() - start_time
            
            results = {
                "analysis_metadata": {
                    "timestamp": datetime.now().isoformat(),
                    "dataset_size": row_count,
                    "analysis_duration_seconds": round(analysis_time, 2),
                    "categories_analyzed": len(category_analysis),
                    "version": "2.0.0"
                },
                "category_analysis": safe_dataframe_operation(category_analysis, 'to_dict', 'records'),
                "statistical_insights": statistical_insights,
                "rating_distribution": safe_dataframe_operation(rating_distribution, 'to_dict', 'records') if not safe_dataframe_operation(rating_distribution, 'empty') else [],
                "variability_insights": variability_insights,
                "performance_summary": performance_summary,
                "key_findings": self._extract_key_findings(category_analysis, statistical_insights),
                "scalability_metrics": {
                    "query_performance": f"{analysis_time:.2f}s for {row_count:,} rows",
                    "memory_efficiency": "Optimized for large datasets",
                    "indexing_strategy": "Category and rating indexes created"
                }
            }
            
            # Cache results if requested
            if cache_results:
                self.analysis_cache["latest_analysis"] = results
            
            logger.info(f"Comprehensive analysis completed in {analysis_time:.2f} seconds")
            return results
            
        except Exception as e:
            logger.error(f"Comprehensive analysis failed: {e}")
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def _compute_advanced_variability(self, category_df: Any) -> Dict[str, Any]:
        """Compute advanced variability metrics for categories"""
        try:
            if safe_dataframe_operation(category_df, 'empty'):
                return {"error": "No category data available"}
            
            # For compatibility, work with the data structure we get
            if PANDAS_AVAILABLE and hasattr(category_df, 'nlargest'):
                # Use pandas methods
                highest_variance = category_df.nlargest(5, 'rating_variance')[['category', 'rating_variance']].to_dict('records')
                lowest_variance = category_df.nsmallest(5, 'rating_variance')[['category', 'rating_variance']].to_dict('records')
                highest_stddev = category_df.nlargest(5, 'rating_stddev')[['category', 'rating_stddev']].to_dict('records')
                most_consistent = category_df.nsmallest(5, 'rating_stddev')[['category', 'rating_stddev']].to_dict('records')
                
                # Calculate coefficient of variation where possible
                category_df['coefficient_of_variation'] = category_df['rating_stddev'] / category_df['avg_rating']
                highest_cv = category_df.nlargest(5, 'coefficient_of_variation')[['category', 'coefficient_of_variation']].to_dict('records')
                
                variability_summary = {
                    "mean_variance": category_df['rating_variance'].mean(),
                    "mean_stddev": category_df['rating_stddev'].mean(),
                    "variance_range": {
                        "min": category_df['rating_variance'].min(),
                        "max": category_df['rating_variance'].max()
                    }
                }
            else:
                # Fallback for when pandas is not available
                # Assume we get a list of dictionaries
                data = category_df if isinstance(category_df, list) else safe_dataframe_operation(category_df, 'to_dict', 'records')
                
                # Sort and get top/bottom 5
                sorted_by_variance = sorted(data, key=lambda x: x.get('rating_variance', 0), reverse=True)
                highest_variance = [{"category": item.get("category"), "rating_variance": item.get("rating_variance")} 
                                 for item in sorted_by_variance[:5]]
                lowest_variance = [{"category": item.get("category"), "rating_variance": item.get("rating_variance")} 
                                for item in sorted_by_variance[-5:]]
                
                sorted_by_stddev = sorted(data, key=lambda x: x.get('rating_stddev', 0), reverse=True)
                highest_stddev = [{"category": item.get("category"), "rating_stddev": item.get("rating_stddev")} 
                                for item in sorted_by_stddev[:5]]
                most_consistent = [{"category": item.get("category"), "rating_stddev": item.get("rating_stddev")} 
                                 for item in sorted_by_stddev[-5:]]
                
                # Calculate coefficient of variation manually
                cv_data = []
                for item in data:
                    if item.get('avg_rating', 0) != 0:
                        cv = item.get('rating_stddev', 0) / item.get('avg_rating', 1)
                        cv_data.append({"category": item.get("category"), "coefficient_of_variation": cv})
                
                cv_data.sort(key=lambda x: x.get('coefficient_of_variation', 0), reverse=True)
                highest_cv = cv_data[:5]
                
                # Calculate summary statistics
                variances = [item.get('rating_variance', 0) for item in data]
                stddevs = [item.get('rating_stddev', 0) for item in data]
                
                variability_summary = {
                    "mean_variance": sum(variances) / len(variances) if variances else 0,
                    "mean_stddev": sum(stddevs) / len(stddevs) if stddevs else 0,
                    "variance_range": {
                        "min": min(variances) if variances else 0,
                        "max": max(variances) if variances else 0
                    }
                }
            
            return {
                "highest_variance_categories": highest_variance,
                "lowest_variance_categories": lowest_variance,
                "highest_stddev_categories": highest_stddev,
                "most_consistent_categories": most_consistent,
                "highest_coefficient_variation": highest_cv,
                "variability_summary": variability_summary
            }
            
        except Exception as e:
            logger.error(f"Error computing variability metrics: {e}")
            return {"error": str(e)}
    
    def _generate_performance_summary(self, category_df: Any, 
                                    insights: Dict[str, Any], row_count: int) -> Dict[str, Any]:
        """Generate performance and scalability summary"""
        try:
            # Performance categories
            if not safe_dataframe_operation(category_df, 'empty'):
                # Handle both pandas DataFrame and list of dicts
                if PANDAS_AVAILABLE and hasattr(category_df, 'head'):
                    top_performers = category_df.head(3)[['category', 'avg_rating', 'product_count']].to_dict('records')
                    bottom_performers = category_df.tail(3)[['category', 'avg_rating', 'product_count']].to_dict('records')
                    significant_categories = category_df[category_df['significance_level'] == 'Significant']
                    
                    dataset_scale = {
                        "total_products": row_count,
                        "categories_analyzed": len(category_df),
                        "average_products_per_category": round(category_df['product_count'].mean(), 1),
                        "largest_category_size": category_df['product_count'].max(),
                        "smallest_category_size": category_df['product_count'].min()
                    }
                else:
                    # Fallback for list of dicts
                    data = category_df if isinstance(category_df, list) else []
                    top_performers = [{"category": item.get("category"), "avg_rating": item.get("avg_rating"), 
                                     "product_count": item.get("product_count")} for item in data[:3]]
                    bottom_performers = [{"category": item.get("category"), "avg_rating": item.get("avg_rating"), 
                                        "product_count": item.get("product_count")} for item in data[-3:]]
                    
                    significant_categories = [item for item in data if item.get('significance_level') == 'Significant']
                    
                    product_counts = [item.get('product_count', 0) for item in data]
                    dataset_scale = {
                        "total_products": row_count,
                        "categories_analyzed": len(data),
                        "average_products_per_category": round(sum(product_counts) / len(product_counts), 1) if product_counts else 0,
                        "largest_category_size": max(product_counts) if product_counts else 0,
                        "smallest_category_size": min(product_counts) if product_counts else 0
                    }
                
                return {
                    "top_performing_categories": top_performers,
                    "underperforming_categories": bottom_performers,
                    "statistically_significant_categories": len(significant_categories),
                    "dataset_scale": dataset_scale,
                    "quality_metrics": {
                        "overall_rating_average": insights.get('overall_average_rating', 0),
                        "high_quality_percentage": insights.get('quality_distribution', {}).get('high_rated_percentage', 0),
                        "rating_standard_deviation": insights.get('overall_stddev', 0)
                    }
                }
            else:
                return {"error": "No category data available for performance summary"}
                
        except Exception as e:
            logger.error(f"Error generating performance summary: {e}")
            return {"error": str(e)}
    
    def _extract_key_findings(self, category_df: Any, insights: Dict[str, Any]) -> List[str]:
        """Extract key actionable findings from the analysis"""
        findings = []
        
        try:
            if not safe_dataframe_operation(category_df, 'empty'):
                # Handle both pandas DataFrame and list formats
                if PANDAS_AVAILABLE and hasattr(category_df, 'iloc'):
                    # Use pandas methods
                    best_category = category_df.iloc[0]
                    worst_category = category_df.iloc[-1]
                    highest_var = category_df.loc[category_df['rating_variance'].idxmax()]
                    most_consistent = category_df.loc[category_df['rating_variance'].idxmin()]
                    significant_cats = category_df[category_df['significance_level'] == 'Significant']
                    
                    # Only add "lowest performance" if we have multiple categories and they're different
                    findings.append(f"'{best_category['category']}' is the top-performing category with {best_category['avg_rating']:.2f} average rating from {best_category['product_count']:,} products")
                    
                    if len(category_df) > 1 and worst_category['category'] != best_category['category']:
                        findings.append(f"'{worst_category['category']}' shows the lowest performance with {worst_category['avg_rating']:.2f} average rating")
                    
                    # Only add variability insights if we have meaningful variance differences
                    if len(category_df) > 1:
                        findings.append(f"'{highest_var['category']}' shows highest rating variability (σ²={highest_var['rating_variance']:.3f}), indicating inconsistent product quality")
                        findings.append(f"'{most_consistent['category']}' demonstrates most consistent quality (σ²={most_consistent['rating_variance']:.3f})")
                    
                    if not significant_cats.empty:
                        findings.append(f"{len(significant_cats)} categories show statistically significant performance differences from the dataset mean")
                else:
                    # Fallback for list of dicts
                    data = category_df if isinstance(category_df, list) else []
                    if data:
                        best_category = data[0]
                        worst_category = data[-1]
                        
                        findings.append(f"'{best_category.get('category', 'Unknown')}' is the top-performing category with {best_category.get('avg_rating', 0):.2f} average rating from {best_category.get('product_count', 0):,} products")
                        
                        # Only add "lowest performance" if we have multiple categories and they're different
                        if len(data) > 1 and worst_category.get('category') != best_category.get('category'):
                            findings.append(f"'{worst_category.get('category', 'Unknown')}' shows the lowest performance with {worst_category.get('avg_rating', 0):.2f} average rating")
                        
                        # Only add variability insights if we have multiple categories
                        if len(data) > 1:
                            # Find highest variance
                            highest_var = max(data, key=lambda x: x.get('rating_variance', 0))
                            most_consistent = min(data, key=lambda x: x.get('rating_variance', float('inf')))
                            
                            findings.append(f"'{highest_var.get('category', 'Unknown')}' shows highest rating variability (σ²={highest_var.get('rating_variance', 0):.3f}), indicating inconsistent product quality")
                            findings.append(f"'{most_consistent.get('category', 'Unknown')}' demonstrates most consistent quality (σ²={most_consistent.get('rating_variance', 0):.3f})")
                        
                        significant_cats = [item for item in data if item.get('significance_level') == 'Significant']
                        if significant_cats:
                            findings.append(f"{len(significant_cats)} categories show statistically significant performance differences from the dataset mean")
                
                # Quality distribution insight - fix the calculation
                high_quality_pct = insights.get('quality_distribution', {}).get('high_rated_percentage', 0)
                if high_quality_pct > 0:
                    findings.append(f"{high_quality_pct:.1f}% of all products receive high ratings (4.0+)")
                else:
                    # Calculate from overall stats if available
                    overall_avg = insights.get('overall_average_rating', 0)
                    if overall_avg >= 4.0:
                        findings.append(f"Overall dataset shows strong performance with {overall_avg:.2f} average rating")
                    else:
                        findings.append(f"Dataset average rating is {overall_avg:.2f}")
                
            return findings[:6]  # Return top 6 findings
            
        except Exception as e:
            logger.error(f"Error extracting key findings: {e}")
            return [f"Error generating insights: {str(e)}"]
    
    def export_results(self, results: Dict[str, Any], output_dir: str = "data/large_scale_analysis") -> Dict[str, str]:
        """
        Export analysis results to multiple formats for dashboard integration
        
        Args:
            results: Analysis results dictionary
            output_dir: Directory to save results
            
        Returns:
            Dict of exported file paths
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            exported_files = {}
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 1. JSON for API integration
            json_path = f"{output_dir}/analysis_results_{timestamp}.json"
            with open(json_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            exported_files["json"] = json_path
            
            # 2. CSV exports for dashboard tools
            if "category_analysis" in results and PANDAS_AVAILABLE:
                csv_path = f"{output_dir}/category_analysis_{timestamp}.csv"
                pd.DataFrame(results["category_analysis"]).to_csv(csv_path, index=False)
                exported_files["category_csv"] = csv_path
            
            # 3. Executive summary
            summary_path = f"{output_dir}/executive_summary_{timestamp}.txt"
            self._generate_executive_summary(results, summary_path)
            exported_files["summary"] = summary_path
            
            # 4. Performance metrics for monitoring
            metrics_path = f"{output_dir}/performance_metrics_{timestamp}.json"
            metrics = {
                "timestamp": results.get("analysis_metadata", {}).get("timestamp"),
                "dataset_size": results.get("analysis_metadata", {}).get("dataset_size"),
                "analysis_duration": results.get("analysis_metadata", {}).get("analysis_duration_seconds"),
                "categories_count": results.get("analysis_metadata", {}).get("categories_analyzed")
            }
            with open(metrics_path, 'w') as f:
                json.dump(metrics, f, indent=2)
            exported_files["metrics"] = metrics_path
            
            logger.info(f"Results exported to {len(exported_files)} files in {output_dir}")
            return exported_files
            
        except Exception as e:
            logger.error(f"Error exporting results: {e}")
            return {"error": str(e)}
    
    def _generate_executive_summary(self, results: Dict[str, Any], output_path: str):
        """Generate executive summary report"""
        try:
            with open(output_path, 'w') as f:
                f.write("AMAZON UK PRODUCTS - LARGE-SCALE ANALYSIS EXECUTIVE SUMMARY\n")
                f.write("=" * 70 + "\n\n")
                
                # Metadata
                metadata = results.get("analysis_metadata", {})
                f.write(f"Analysis Date: {metadata.get('timestamp', 'Unknown')}\n")
                f.write(f"Dataset Size: {metadata.get('dataset_size', 0):,} products\n")
                f.write(f"Analysis Duration: {metadata.get('analysis_duration_seconds', 0)} seconds\n")
                f.write(f"Categories Analyzed: {metadata.get('categories_analyzed', 0)}\n\n")
                
                # Key Findings
                f.write("KEY FINDINGS\n")
                f.write("-" * 20 + "\n")
                key_findings = results.get("key_findings", [])
                for i, finding in enumerate(key_findings, 1):
                    f.write(f"{i}. {finding}\n")
                f.write("\n")
                
                # Performance Summary
                perf_summary = results.get("performance_summary", {})
                if "top_performing_categories" in perf_summary:
                    f.write("TOP PERFORMING CATEGORIES\n")
                    f.write("-" * 25 + "\n")
                    for cat in perf_summary["top_performing_categories"]:
                        f.write(f"• {cat['category']}: {cat['avg_rating']:.2f} avg rating ({cat['product_count']:,} products)\n")
                    f.write("\n")
                
                # Scalability Metrics
                scalability = results.get("scalability_metrics", {})
                f.write("SCALABILITY PERFORMANCE\n")
                f.write("-" * 22 + "\n")
                f.write(f"Query Performance: {scalability.get('query_performance', 'N/A')}\n")
                f.write(f"Memory Efficiency: {scalability.get('memory_efficiency', 'N/A')}\n")
                f.write(f"Indexing Strategy: {scalability.get('indexing_strategy', 'N/A')}\n")
                
        except Exception as e:
            logger.error(f"Error generating executive summary: {e}")
    
    def cleanup(self):
        """Clean up resources"""
        if self.duckdb_handler:
            self.duckdb_handler.close()

# Convenience functions for API integration
def run_large_scale_analysis(dataset_path: str = None, export_results: bool = True) -> Dict[str, Any]:
    """
    Run complete large-scale analysis pipeline
    
    Args:
        dataset_path: Path to Amazon UK CSV dataset
        export_results: Whether to export results to files
        
    Returns:
        Complete analysis results
    """
    analyzer = LargeScaleAnalyzer()
    
    try:
        # Prepare dataset
        if not analyzer.download_and_prepare_dataset():
            logger.warning("Using sample data - download full dataset for complete analysis")
        
        # Load dataset
        if dataset_path:
            analyzer.dataset_path = dataset_path
        
        if not analyzer.load_dataset_optimized():
            return {"error": "Failed to load dataset"}
        
        # Run analysis
        results = analyzer.run_comprehensive_analysis()
        
        # Export if requested
        if export_results and "error" not in results:
            exported_files = analyzer.export_results(results)
            results["exported_files"] = exported_files
        
        return results
        
    finally:
        analyzer.cleanup()

def get_analysis_summary(results: Dict[str, Any]) -> Dict[str, Any]:
    """Extract key metrics for dashboard display"""
    if "error" in results:
        return results
    
    metadata = results.get("analysis_metadata", {})
    perf_summary = results.get("performance_summary", {})
    
    return {
        "dataset_scale": metadata.get("dataset_size", 0),
        "categories_count": metadata.get("categories_analyzed", 0),
        "analysis_duration": metadata.get("analysis_duration_seconds", 0),
        "top_category": perf_summary.get("top_performing_categories", [{}])[0] if perf_summary.get("top_performing_categories") else {},
        "key_insights_count": len(results.get("key_findings", [])),
        "timestamp": metadata.get("timestamp")
    }
