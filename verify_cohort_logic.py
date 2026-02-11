
import duckdb
import pandas as pd

# Connect to database
conn = duckdb.connect(database='knust_engineering_new.duckdb', read_only=True)

print("--- Verifying Cohort: Geological Engineering 2018/2019 (Year 1) ---")

# 1. Total Class Size (Fresh + Repeaters) in 2018 Year 1
q_total = """
SELECT COUNT(DISTINCT student_id) 
FROM student_performance
WHERE academic_year = '2018/2019'
AND level = 1
AND program = 'geol'
"""
total = conn.execute(q_total).fetchone()[0]
print(f"Total Class Size (2018/2019 Level 1): {total} (Expected ~148)")

# 2. Composition (Fresh vs Repeating)
q_comp = """
SELECT 
    CASE 
        WHEN admission_year = 2018 THEN 'Fresh' 
        ELSE 'Repeating/Previous' 
    END as type,
    COUNT(DISTINCT student_id) as count
FROM student_performance
WHERE academic_year = '2018/2019'
AND level = 1
AND program = 'geol'
GROUP BY type
"""
df_comp = conn.execute(q_comp).df()
print("\nComposition:")
print(df_comp)

# 3. Origin Breakdown
q_origin = """
SELECT admission_year, COUNT(DISTINCT student_id) as count
FROM student_performance
WHERE academic_year = '2018/2019'
AND level = 1
AND program = 'geol'
GROUP BY admission_year
ORDER BY admission_year
"""
df_origin = conn.execute(q_origin).df()
print("\nOrigin Breakdown (Admission Year):")
print(df_origin)


# 4. Attrition Status of THIS group
q_attrition = """
WITH start_cohort AS (
    SELECT DISTINCT student_id
    FROM student_performance
    WHERE academic_year = '2018/2019'
    AND level = 1
    AND program = 'geol'
),
latest_status AS (
    SELECT sp.student_id, sp.status
    FROM student_performance sp
    JOIN start_cohort sc ON sp.student_id = sc.student_id
)
SELECT status, COUNT(DISTINCT student_id) as count
FROM latest_status
GROUP BY status
"""
df_attr = conn.execute(q_attrition).df()
print("\nAttrition Status (Latest Status of 2018 Class):")
print(df_attr)

# 5. Progression (Retention Trend)
print("\n--- Progression (Retention Trend) ---")
q_progression = """
WITH cohort_students AS (
    SELECT DISTINCT student_id 
    FROM student_performance
    WHERE academic_year = '2018/2019'
    AND level = 1
    AND program = 'geol'
)
SELECT 
    sp.academic_year,
    COUNT(DISTINCT sp.student_id) as active_students
FROM student_performance sp
JOIN cohort_students cs ON sp.student_id = cs.student_id
WHERE sp.academic_year >= '2018/2019'
GROUP BY sp.academic_year
ORDER BY sp.academic_year
"""
df_prog = conn.execute(q_progression).df()
print(df_prog)

conn.close()
