import sys
print(f"Python version: {sys.version}")
try:
    import duckdb
    print("DuckDB imported successfully")
except ImportError as e:
    print(f"Failed to import duckdb: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
