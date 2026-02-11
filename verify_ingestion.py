
import duckdb
import pandas as pd

DB_PATH = 'knust_engineering_new.duckdb'

try:
    conn = duckdb.connect(DB_PATH, read_only=True)
    
    print("--- Admission Year Distribution ---")
    df_adm = conn.execute("SELECT admission_year, COUNT(DISTINCT student_id) as count FROM student_performance GROUP BY admission_year ORDER BY admission_year").df()
    print(df_adm)
    
    print("\n--- Status Distribution ---")
    df_status = conn.execute("SELECT status, COUNT(DISTINCT student_id) as count FROM student_performance GROUP BY status").df()
    print(df_status)
    
    print("\n--- Sample Withdrawn/Deferred Students ---")
    print("\n--- Program 'geol' Admission Year 2018 Count (Expected ~148) ---")
    df_geol = conn.execute("SELECT COUNT(DISTINCT student_id) as count FROM student_performance WHERE admission_year = 2018 AND program = 'geol'").df()
    print(df_geol)
    
    conn.close()

except Exception as e:
    print(f"Error: {e}")
