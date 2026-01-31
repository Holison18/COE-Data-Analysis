import duckdb
import pandas as pd

def debug_compare():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    courses = ['COE 181', 'TE 474']
    targets_sql = ",".join([f"'{t}'" for t in courses])
    
    q_prob = f"""
        SELECT academic_year, REPLACE(course_code, '\n', ' ') as clean_code, AVG(avg_mark) as avg_mark 
        FROM course_summary 
        WHERE REPLACE(course_code, '\n', ' ') IN ({targets_sql})
        GROUP BY academic_year, clean_code
        ORDER BY academic_year
    """
    
    df = con.execute(q_prob).df()
    print(df.to_string())
    
    con.close()

if __name__ == "__main__":
    debug_compare()
