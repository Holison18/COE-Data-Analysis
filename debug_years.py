
import duckdb
import pandas as pd

try:
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    print("--- Checking Course Level Distribution in course_summary ---")
    
    q_levels = """
        SELECT 
            CASE 
                WHEN course_code LIKE '%1__' THEN 'Year 1'
                WHEN course_code LIKE '%2__' THEN 'Year 2'
                WHEN course_code LIKE '%3__' THEN 'Year 3'
                WHEN course_code LIKE '%4__' THEN 'Year 4'
                ELSE 'Other'
            END as level_est,
            COUNT(*) as count
        FROM course_summary
        GROUP BY level_est
        ORDER BY level_est
    """
    df_levels = con.execute(q_levels).df()
    print(df_levels)
    
    print("\n--- Sample 'Year 4' Courses (if any) ---")
    q_sample = "SELECT course_code FROM course_summary WHERE course_code LIKE '%4__' LIMIT 10"
    print(con.execute(q_sample).df())

    print("\n--- Sample 'Other' Courses (Potential Misses) ---")
    q_other = """
        SELECT course_code 
        FROM course_summary 
        WHERE course_code NOT LIKE '%1__' 
          AND course_code NOT LIKE '%2__' 
          AND course_code NOT LIKE '%3__' 
          AND course_code NOT LIKE '%4__'
        LIMIT 10
    """
    print(con.execute(q_other).df())

except Exception as e:
    print(f"Error: {e}")
