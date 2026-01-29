import duckdb
import pandas as pd

try:
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    print("--- Database Verification ---")
    
    # Check Counts
    try:
        n_summary = con.execute("SELECT COUNT(*) FROM course_summary").fetchone()[0]
        print(f"Course Summary Rows: {n_summary}")
    except:
        print("Course Summary Table not found or empty.")

    try:
        n_perf = con.execute("SELECT COUNT(*) FROM student_performance").fetchone()[0]
        print(f"Student Performance Rows: {n_perf}")
        
        # Check Credits
        print("\nCredits Distribution (Head):")
        print(con.execute("SELECT credits, COUNT(*) FROM course_summary GROUP BY credits ORDER BY credits").df().to_string())
        
        # Check Gender
        print("\nGender Distribution:")
        print(con.execute("SELECT gender, COUNT(*) FROM student_performance GROUP BY gender").df().to_string())
        
        # Check CWA
        n_cwa = con.execute("SELECT COUNT(cwa) FROM student_performance").fetchone()[0]
        print(f"\nNon-Null CWA Records: {n_cwa}")
        
    except Exception as e:
        print(f"Error querying student_performance: {e}")
        
except Exception as e:
    print(f"Could not connect to DB: {e}")
