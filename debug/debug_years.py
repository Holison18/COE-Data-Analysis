import duckdb

def debug_years():
    con = duckdb.connect('knust_engineering.duckdb', read_only=True)
    
    # Check all distinct sorted
    years = con.execute("SELECT DISTINCT academic_year FROM course_summary ORDER BY academic_year").fetchall()
    print("Distinct Years (Sorted in DB):")
    for y in years:
        print(f"'{y[0]}' (len={len(y[0])}) - bytes: {y[0].encode('utf-8')}")

    # Check specifically for 2021/2022 variants
    print("\nLikelike '2021%2022':")
    variants = con.execute("SELECT DISTINCT academic_year FROM course_summary WHERE academic_year LIKE '%2021%2022%'").fetchall()
    for v in variants:
        print(f"'{v[0]}' (len={len(v[0])})")

    con.close()

if __name__ == "__main__":
    debug_years()
