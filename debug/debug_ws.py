import duckdb

def check_whitespace():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    print("--- Checking 'ME 161' variants ---")
    query = """
        SELECT course_code, 
               length(course_code) as len_raw,
               REPLACE(course_code, '\n', ' ') as rep,
               length(REPLACE(course_code, '\n', ' ')) as len_rep,
               REPLACE(course_code, '\n', ' ') LIKE '%1__' as matches_like,
               TRIM(REPLACE(course_code, '\n', ' ')) LIKE '%1__' as matches_like_trimmed
        FROM student_performance
        WHERE course_code LIKE '%ME%161%'
        AND academic_year = '2020/2021'
        LIMIT 10
    """
    df = con.execute(query).df()
    print(df.to_string())

    con.close()

if __name__ == "__main__":
    check_whitespace()
