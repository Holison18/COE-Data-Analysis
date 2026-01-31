import duckdb
import pandas as pd

def debug_fix_logic():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    target_fac = 'Faculty of Electrical and Computer Engineering'
    sel_cohort = '2020/2021'
    
    # 1. New Logic with Exclusion
    cohort_where = f"faculty = '{target_fac}'"
    
    q_ids_fixed = f"""
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
        ),
        priors AS (
            SELECT DISTINCT student_id
            FROM student_performance
            WHERE academic_year < '{sel_cohort}'
        )
        SELECT s.student_id 
        FROM starters s
        JOIN matches m ON s.student_id = m.student_id
        WHERE s.student_id NOT IN (SELECT student_id FROM priors)
    """
    
    q_prog = f"""
        WITH cohort_list AS ({q_ids_fixed})
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
    
    print("Running Fixed Query...")
    df = con.execute(q_prog).df()
    print(df.to_string())
    
    if not df.empty:
        # Check if row 0 is now 2020/2021
        print(f"Start Year: {df.iloc[0]['academic_year']}")
        print(f"Start Count: {df.iloc[0]['active_students']}")

    con.close()

if __name__ == "__main__":
    debug_fix_logic()
