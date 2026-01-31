import duckdb
import pandas as pd

try:
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    print("\n=== Ingestion Verification ===\n")
    
    # 1. Check Course Summary
    print("--- Table: course_summary ---")
    summary_count = con.execute("SELECT COUNT(*) FROM course_summary").fetchone()[0]
    print(f"Total Rows: {summary_count}")
    if summary_count > 0:
        print(con.execute("SELECT * FROM course_summary LIMIT 5").df())
    
    # 2. Check Student Marks
    print("\n--- Table: student_performance ---")
    marks_count = con.execute("SELECT COUNT(*) FROM student_performance").fetchone()[0]
    print(f"Total Rows: {marks_count}")
    if marks_count > 0:
        print(con.execute("SELECT * FROM student_performance LIMIT 5").df())
        
    # 3. Aggregations
    print("\n--- Summary by Faculty ---")
    print(con.execute("""
        SELECT faculty, COUNT(*) as files_processed 
        FROM (SELECT DISTINCT faculty, source_file FROM course_summary) 
        GROUP BY faculty
    """).df())

    print("\n--- Distinct Academic Years ---")
    print(con.execute("SELECT DISTINCT academic_year FROM course_summary ORDER BY academic_year").df())

    con.close()

except Exception as e:
    print(f"Error: {e}")
