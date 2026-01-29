
import duckdb
import pandas as pd

try:
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    # 1. Identify students with high trail counts
    print("--- Students with > 10 Trails ---")
    q_high_trails = """
        SELECT student_id, COUNT(*) as trail_count
        FROM student_performance
        WHERE mark < 50
        GROUP BY student_id
        HAVING trail_count > 10
        ORDER BY trail_count DESC
        LIMIT 5
    """
    df_high = con.execute(q_high_trails).df()
    print(df_high)
    
    if not df_high.empty:
        student_id = df_high.iloc[0]['student_id']
        print(f"\n--- Detailed Trail History for Student {student_id} ---")
        
        # 2. Inspect the specific trails for the top student
        q_detail = f"""
            SELECT academic_year, semester, course_code, mark
            FROM student_performance
            WHERE student_id = '{student_id}' AND mark < 50
            ORDER BY academic_year, semester
        """
        df_detail = con.execute(q_detail).df()
        print(df_detail)
        
        # 3. Check for duplicates in course_summary for a specific course
        print(f"\n--- Checking for potential course duplicates for {df_detail.iloc[0]['course_code']} ---")
        q_dup = f"""
            SELECT * FROM course_summary WHERE course_code = '{df_detail.iloc[0]['course_code']}'
        """
        print(con.execute(q_dup).df())

except Exception as e:
    print(f"Error: {e}")
