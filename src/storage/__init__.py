"""
Storage package initialization
"""
from .sqlite_handler import SQLiteHandler

try:
    from .duckdb_handler import DuckDBHandler
    DUCKDB_AVAILABLE = True
except ImportError:
    DuckDBHandler = None
    DUCKDB_AVAILABLE = False

__all__ = ['SQLiteHandler']

if DUCKDB_AVAILABLE:
    __all__.append('DuckDBHandler')
