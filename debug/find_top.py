import duckdb
con = duckdb.connect('knust_engineering.duckdb', read_only=True)
print(con.execute("""
    SELECT REPLACE(course_code, '\n', ' ') as code, 
           MAX(course_name) as name,
           AVG((num_passed * 100.0) / NULLIF((num_passed + num_trailed), 0)) as rate 
    FROM course_summary 
    GROUP BY code 
    ORDER BY rate DESC 
    LIMIT 5
""").df())
