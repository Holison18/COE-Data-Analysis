
import duckdb
import pandas as pd

# Connect to database
conn = duckdb.connect(database='knust_engineering_new.duckdb', read_only=True)

print("--- Verifying Course Performance: Geological Engineering 2018/2019 Cohort ---")

# 1. Define Cohort (Same logic as app.py)
q_ids = """
SELECT DISTINCT student_id 
FROM student_performance
WHERE academic_year = '2018/2019'
AND level = 1
AND program = 'geol'
"""

# 2. Avg Mark per Course
print("\n--- Top 5 Best Performing Courses ---")
q_course_perf = f"""
    WITH cohort_students AS ({q_ids})
    SELECT 
        sp.course_code,
        AVG(sp.mark) as avg_mark,
        COUNT(sp.mark) as num_students
    FROM student_performance sp
    JOIN cohort_students cs ON sp.student_id = cs.student_id
    WHERE sp.mark IS NOT NULL
    GROUP BY sp.course_code
    HAVING num_students > 10
    ORDER BY avg_mark DESC
    LIMIT 5
"""
df_top = conn.execute(q_course_perf).df()
print(df_top)

print("\n--- Bottom 5 Performing Courses ---")
q_course_bottom = f"""
    WITH cohort_students AS ({q_ids})
    SELECT 
        sp.course_code,
        AVG(sp.mark) as avg_mark,
        COUNT(sp.mark) as num_students
    FROM student_performance sp
    JOIN cohort_students cs ON sp.student_id = cs.student_id
    WHERE sp.mark IS NOT NULL
    GROUP BY sp.course_code
    HAVING num_students > 10
    ORDER BY avg_mark ASC
    LIMIT 5
"""
df_bottom = conn.execute(q_course_bottom).df()
print(df_bottom)

# 3. CWA Impact (Correlation)
print("\n--- Top 5 High Impact Courses (CWA Correlation) ---")
q_cwa_corr = f"""
    WITH cohort_students AS ({q_ids})
    SELECT 
        sp.course_code,
        CORR(sp.mark, sp.cwa) as correlation,
        COUNT(sp.student_id) as num_students
    FROM student_performance sp
    JOIN cohort_students cs ON sp.student_id = cs.student_id
    WHERE sp.mark IS NOT NULL AND sp.cwa IS NOT NULL
    GROUP BY sp.course_code
    HAVING num_students > 10
    ORDER BY correlation DESC
    LIMIT 5
"""
try:
    df_corr = conn.execute(q_cwa_corr).df()
    print(df_corr)
except Exception as e:
    print(f"Error calculating correlation: {e}")

conn.close()
