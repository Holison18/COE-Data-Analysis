import duckdb
import pandas as pd

def inspect_cohort_issue():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    print("--- 1. Check Faculty Names ---")
    faculties = con.execute("SELECT DISTINCT faculty FROM student_performance").df()
    print(faculties)
    
    # Assuming the user means 'Faculty of Electrical and Computer Engineering'
    # Let's find the closest match
    target_fac = 'Faculty of Electrical and Computer Engineering'
    
    print(f"\n--- 2. Total Students in {target_fac} for 2020/2021 ---")
    query_total = f"""
        SELECT COUNT(DISTINCT student_id) 
        FROM student_performance 
        WHERE faculty = '{target_fac}' AND academic_year = '2020/2021'
    """
    try:
        total = con.execute(query_total).fetchone()[0]
        print(f"Total Unique Students: {total}")
    except Exception as e:
        print(f"Error querying specific faculty: {e}")

    print(f"\n--- 3. Check Course Levels for {target_fac} in 2020/2021 ---")
    # See what courses are recorded for this year/faculty to see if they look like Level 100
    query_courses = f"""
        SELECT DISTINCT course_code, semester
        FROM student_performance
        WHERE faculty = '{target_fac}' AND academic_year = '2020/2021'
        LIMIT 20
    """
    try:
        courses = con.execute(query_courses).df()
        print(courses)
    except:
        pass

    print(f"\n--- 4. why only 7? Check the intersection ---")
    # Our logic: students in 2020/2021 AND took a Level 100 course
    q_logic = f"""
        SELECT student_id, LIST(DISTINCT course_code) as courses
        FROM student_performance
        WHERE faculty = '{target_fac}' AND academic_year = '2020/2021'
        GROUP BY student_id
        HAVING SUM(CASE WHEN course_code LIKE '%1__' THEN 1 ELSE 0 END) > 0
    """
    try:
        df_logic = con.execute(q_logic).df()
        print(f"Students meeting Cohort Logic: {len(df_logic)}")
        print(df_logic.head(10))
    except:
        pass

    con.close()

if __name__ == "__main__":
    inspect_cohort_issue()
