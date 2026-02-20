import duckdb
import pandas as pd

con = duckdb.connect('knust_engineering_new.duckdb', read_only=True)

print("--- 1. OVERALL SUMMARY ---")
# Count students, courses, years
q_summary = """
    SELECT 
        COUNT(DISTINCT student_id) as total_students,
        COUNT(DISTINCT course_code) as total_courses,
        COUNT(DISTINCT academic_year) as total_years
    FROM student_performance
"""
print(con.execute(q_summary).df())

print("\n--- 2. ACADEMIC YEAR TRENDS (Peak & Dip) ---")
q_trend = """
    SELECT academic_year, AVG(mark) as avg_mark
    FROM student_performance
    GROUP BY academic_year
    ORDER BY academic_year
"""
print(con.execute(q_trend).df())

print("\n--- 3. TOP 5 DIFFICULT COURSES (Lowest Pass Rate) ---")
q_diff = """
    SELECT course_code, course_name, 
           AVG((num_passed * 100.0) / NULLIF((num_passed + num_trailed), 0)) as pass_rate
    FROM course_summary
    GROUP BY course_code, course_name
    HAVING COUNT(*) > 5
    ORDER BY pass_rate ASC
    LIMIT 5
"""
print(con.execute(q_diff).df())

print("\n--- 4. TOP 5 PERFORMING COURSES (Highest Pass Rate) ---")
q_easy = """
    SELECT course_code, course_name, 
           AVG((num_passed * 100.0) / NULLIF((num_passed + num_trailed), 0)) as pass_rate
    FROM course_summary
    GROUP BY course_code, course_name
    HAVING COUNT(*) > 5
    ORDER BY pass_rate DESC
    LIMIT 5
"""
print(con.execute(q_easy).df())

print("\n--- 5. RETENTION/ATTRITION (Proxy via Trails) ---")
q_risk = """
    SELECT academic_year, 
           SUM(num_trailed) as trails, 
           SUM(num_passed) as passes,
           (SUM(num_trailed) * 100.0 / NULLIF(SUM(num_trailed + num_passed), 0)) as failure_rate
    FROM course_summary 
    GROUP BY academic_year
    ORDER BY academic_year
"""
print(con.execute(q_risk).df())
