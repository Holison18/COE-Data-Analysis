import duckdb
import pandas as pd

con = duckdb.connect('knust_engineering.duckdb', read_only=True)

print("--- Gender Verification ---")
print(con.execute("SELECT gender, COUNT(*) as count FROM student_performance GROUP BY gender").df())

print("\n--- Course Code Pattern Check ---")
print(con.execute("SELECT DISTINCT course_code FROM student_performance WHERE course_code LIKE 'EE%' OR course_code LIKE 'MATH%' OR course_code LIKE 'COE%' OR course_code LIKE 'ME%' LIMIT 20").df())

print("\n--- Program Check ---")
print(con.execute("SELECT DISTINCT program FROM student_performance").df())
