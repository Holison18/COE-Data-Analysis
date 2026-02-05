import duckdb
import pandas as pd

con = duckdb.connect('knust_engineering_new.duckdb', read_only=True)

print("--- 1. Testing New Level-Based Cohort Logic ---")
print("Target: 2018/2019 COE (Level 1)")

# Find exact program code for COE
progs = con.execute("SELECT DISTINCT program FROM student_performance WHERE program LIKE 'coe%'").df()
print(f"Programs found: {progs.values.flatten()}")
prog_code = 'coe' 

q_cohort = f"""
    SELECT COUNT(DISTINCT student_id) 
    FROM student_performance
    WHERE academic_year = '2018/2019'
    AND level = 1
    AND program = '{prog_code}'
"""
print(f"Cohort Size (Starters): {con.execute(q_cohort).df().iloc[0,0]}")

print("\n--- 2. Testing Joiners Logic (Stream - Starters) ---")
# Stream: 2018(L1) + 2019(L2) + 2020(L3) + 2021(L4)

q_joiners = f"""
    WITH starters AS (
        SELECT DISTINCT student_id 
        FROM student_performance
        WHERE academic_year = '2018/2019' AND level = 1 AND program = '{prog_code}'
    ),
    stream AS (
        SELECT DISTINCT student_id FROM student_performance 
        WHERE program = '{prog_code}'
        AND (
            (academic_year = '2018/2019' AND level = 1) OR
            (academic_year = '2019/2020' AND level = 2) OR
            (academic_year = '2020/2021' AND level = 3) OR
            (academic_year = '2021/2022' AND level = 4)
        )
    )
    SELECT COUNT(DISTINCT student_id) 
    FROM stream
    WHERE student_id NOT IN (SELECT student_id FROM starters)
"""
print(f"Joiners Count: {con.execute(q_joiners).df().iloc[0,0]}")
