"""
Analysis package initialization
"""
try:
    from .category_analyzer import CategoryAnalyzer, run_category_analysis
    ANALYSIS_AVAILABLE = True
except ImportError:
    CategoryAnalyzer = None
    run_category_analysis = None
    ANALYSIS_AVAILABLE = False

__all__ = []

if ANALYSIS_AVAILABLE:
    __all__.extend(['CategoryAnalyzer', 'run_category_analysis'])
