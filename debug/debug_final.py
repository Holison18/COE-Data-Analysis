import duckdb
import pandas as pd

def debug_full_app_query():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    target_fac = 'Faculty of Electrical and Computer Engineering'
    sel_cohort = '2020/2021'
    
    # Simulate App Logic
    cohort_where = f"faculty = '{target_fac}'"
    
    q_ids = f"""
        WITH starters AS (
            SELECT DISTINCT student_id 
            FROM student_performance
            WHERE academic_year = '{sel_cohort}'
            AND REPLACE(course_code, '\n', ' ') LIKE '%1__'
        ),
        matches AS (
            SELECT DISTINCT student_id 
            FROM student_performance 
            WHERE {cohort_where}
        )
        SELECT s.student_id 
        FROM starters s
        JOIN matches m ON s.student_id = m.student_id
    """
    
    q_prog = f"""
        WITH cohort_list AS ({q_ids})
        SELECT 
            academic_year,
            COUNT(DISTINCT student_id) as active_students
        FROM (
            SELECT sp.student_id, sp.academic_year
            FROM student_performance sp
            JOIN cohort_list cl ON sp.student_id = cl.student_id
        )
        GROUP BY academic_year
        ORDER BY academic_year
    """
    
    print("Running Full Query...")
    df = con.execute(q_prog).df()
    print(df.to_string())
    
    if not df.empty:
        initial_count = df.iloc[0]['active_students']
        print(f"Initial Count (Row 0): {initial_count}")

    con.close()

if __name__ == "__main__":
    debug_full_app_query()
