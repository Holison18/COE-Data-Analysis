import duckdb

def debug_split():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    target_fac = 'Faculty of Electrical and Computer Engineering'
    target_year = '2020/2021'

    print(f"Target Faculty: '{target_fac}'")
    print(f"Target Year: '{target_year}'")

    # 1. Count Starters (Year + L100)
    q_start = f"""
        SELECT COUNT(DISTINCT student_id) 
        FROM student_performance
        WHERE academic_year = '{target_year}'
        AND REPLACE(course_code, '\n', ' ') LIKE '%1__'
    """
    count_start = con.execute(q_start).fetchone()[0]
    print(f"Count of Starters: {count_start}")

    # 2. Count Matches (Faculty)
    q_match = f"""
        SELECT COUNT(DISTINCT student_id) 
        FROM student_performance 
        WHERE faculty = '{target_fac}'
    """
    count_match = con.execute(q_match).fetchone()[0]
    print(f"Count of Matches: {count_match}")

    # 3. Intersection
    q_inter = f"""
        WITH starters AS (
            SELECT DISTINCT student_id 
            FROM student_performance
            WHERE academic_year = '{target_year}'
            AND REPLACE(course_code, '\n', ' ') LIKE '%1__'
        ),
        matches AS (
            SELECT DISTINCT student_id 
            FROM student_performance 
            WHERE faculty = '{target_fac}'
        )
        SELECT COUNT(*)
        FROM starters s
        JOIN matches m ON s.student_id = m.student_id
    """
    count_inter = con.execute(q_inter).fetchone()[0]
    print(f"Intersection: {count_inter}")
    
    # 4. Check if we missed the faculty match?
    # Maybe the L100 rows have a DIFFERENT faculty?
    # e.g. student X has rows in 'Faculty A' for L100, but rows in 'Faculty B' for proper engineering?
    # But 'matches' selects ANY student who has ANY row in 'Faculty of Electrical...'.
    # If the student exists there, they are in the set.
    
    # Let's check a sample student from the "Starters" group who is NOT in intersection
    # ie. took L100 in 2020/21 but does NOT have any row in 'Faculty of Electrical and Computer Engineering'
    # Wait, if they are studying Electrical Engineering, they SHOULD have rows there.
    
    print("\n--- Inspecting a Missing Student ---")
    q_missing = f"""
        SELECT student_id
        FROM student_performance
        WHERE academic_year = '{target_year}'
        AND REPLACE(course_code, '\n', ' ') LIKE '%1__'
        EXCEPT
        SELECT student_id 
        FROM student_performance 
        WHERE faculty = '{target_fac}'
        LIMIT 5
    """
    missing_ids = [x[0] for x in con.execute(q_missing).fetchall()]
    print(f"Sample Matching Starters NOT in Faculty: {missing_ids}")
    
    if missing_ids:
        # Check one student's faculty
        sid = missing_ids[0]
        print(f"Faculty for {sid}:")
        print(con.execute(f"SELECT DISTINCT faculty FROM student_performance WHERE student_id = '{sid}'").df())

    con.close()

if __name__ == "__main__":
    debug_split()
