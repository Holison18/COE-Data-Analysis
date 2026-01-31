import duckdb
import pandas as pd

def debug_deep_dive():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    target_fac = 'Faculty of Electrical and Computer Engineering'
    target_year = '2020/2021'
    
    # 1. Who are the 7? (Mimic App Logic exactly)
    print(f"\n--- 1. Who are the students found by strict app logic? ---")
    q_app = f"""
        WITH starters AS (
            SELECT DISTINCT student_id 
            FROM student_performance
            WHERE academic_year = '{target_year}'
            AND REPLACE(course_code, '\n', ' ') LIKE '%1__'
        ),
        matches AS (
            SELECT DISTINCT student_id 
            FROM student_performance 
            WHERE faculty = '{target_fac}'
        )
        SELECT s.student_id, 'Found' as status
        FROM starters s
        JOIN matches m ON s.student_id = m.student_id
    """
    try:
        df_7 = con.execute(q_app).df()
        print(f"Count found: {len(df_7)}")
        print(df_7.head(10))
        if not df_7.empty:
            example_ids = ",".join([f"'{x}'" for x in df_7['student_id'].head(5).tolist()])
            # See what courses these guys took
            q_what = f"SELECT student_id, course_code FROM student_performance WHERE student_id IN ({example_ids}) AND academic_year = '{target_year}'"
            print(con.execute(q_what).df())
    except Exception as e:
        print(e)
        
    # 2. Are there Student IDs ending in '20'? (Standard 2020 intake naming?)
    print(f"\n--- 2. Are there students with IDs ending in '20' in this faculty? ---")
    q_20 = f"""
        SELECT student_id, academic_year, course_code, faculty
        FROM student_performance
        WHERE student_id LIKE '%20' 
        AND faculty = '{target_fac}'
        LIMIT 10
    """
    print(con.execute(q_20).df())

    # 3. Where is the bulk of the 2020 cohort?
    print(f"\n--- 3. What courses did '...20' students take in 2020/2021? ---")
    q_bulk = f"""
        SELECT course_code, COUNT(*) as cnt
        FROM student_performance
        WHERE student_id LIKE '%20'
        AND academic_year = '{target_year}'
        AND faculty = '{target_fac}'
        GROUP BY course_code
        ORDER BY cnt DESC
        LIMIT 10
    """
    print(con.execute(q_bulk).df())

    con.close()

if __name__ == "__main__":
    debug_deep_dive()
