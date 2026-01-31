import duckdb
import pandas as pd

def inspect_data():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    print("--- Student ID Samples ---")
    df = con.execute("SELECT student_id, academic_year, course_code, semester FROM student_performance LIMIT 20").df()
    print(df.to_string())

    print("\n--- Distinct Years per Student ---")
    # Check if we can see students progressing
    q = """
        SELECT student_id, LIST(DISTINCT academic_year) as years
        FROM student_performance
        GROUP BY student_id
        HAVING len(years) > 1
        LIMIT 10
    """
    df_prog = con.execute(q).df()
    print(df_prog.to_string())

    con.close()

if __name__ == "__main__":
    inspect_data()
