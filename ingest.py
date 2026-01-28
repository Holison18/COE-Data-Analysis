import os
import re
import pandas as pd
import duckdb
import numpy as np

# --- Configuration ---
DB_PATH = 'knust_engineering.duckdb'
DATA_DIR = 'data'

# --- Database Setup ---
def connect_db():
    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS course_summary (
            faculty VARCHAR,
            department VARCHAR, 
            program VARCHAR,
            academic_year VARCHAR,
            semester INT,
            course_code VARCHAR,
            course_name VARCHAR,
            avg_mark DOUBLE,
            std_dev DOUBLE,
            pass_rate DOUBLE,
            num_passed INT,
            num_trailed INT,
            source_file VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS student_marks (
            faculty VARCHAR,
            department VARCHAR,
            program VARCHAR,
            academic_year VARCHAR,
            semester INT,
            student_id VARCHAR,
            course_code VARCHAR,
            mark DOUBLE,
            source_file VARCHAR
        )
    """)
    return conn

# --- Helper Functions ---

def clean_mark(val):
    """
    Handles mixed types and 'Mark\nGrade' formatting.
    Returns float or None.
    """
    if pd.isna(val) or val == '' or val == '-':
        return None
    
    val_str = str(val).strip()
    
    # Handle "75\nA" format
    if '\n' in val_str:
        val_str = val_str.split('\n')[0]
    
    # Remove any non-numeric chars except dot
    # Sometimes there are typos, but let's try basic float conversion
    try:
        return float(val_str)
    except ValueError:
        return None

def extract_metadata(file_path):
    """
    Extracts metadata from file path and filename.
    Path format: .../Faculty/Program/filename.xlsx
    Filename example: tel2_ss_20_21.xlsx -> Year: 2020/2021, Sem: 2
    """
    abs_path = os.path.abspath(file_path)
    parts = abs_path.split(os.sep)
    
    # Assuming data/Faculty/Program/file.xlsx
    # We need to find where 'data' is or just look at the last few folders
    # Let's rely on the relative structure from the crawl
    
    filename = os.path.basename(file_path)
    directory = os.path.dirname(file_path)
    
    # Naive directory walk back
    program = os.path.basename(directory)
    faculty = os.path.basename(os.path.dirname(directory))
    
    # Rename Faculty/Department if needed. 
    # User said: Faculty -> Department -> Program
    # But checking the `list_dir` output earlier: 
    # data/Faculty.../ce/ce1.xlsx
    # So it looks like Faculty -> Program(or Dept) -> file
    # Let's just capture the immediate parent as Program and grand-parent as Faculty
    
    # Regex for Year and Semester
    # Patterns observed: ce1_fs_22_23 (First Sem), tel2_ss_20_21 (Second Sem)
    # _fs_ -> First Semester
    # _ss_ -> Second Semester
    # 20_21 -> 2020/2021
    
    semester = None
    if '_fs_' in filename.lower():
        semester = 1
    elif '_ss_' in filename.lower():
        semester = 2
        
    year = None
    # Look for pattern like 20_21 or 22_23
    year_match = re.search(r'(\d{2})_(\d{2})', filename)
    if year_match:
        y1, y2 = year_match.groups()
        year = f"20{y1}/20{y2}"
        
    return {
        'faculty': faculty,
        'program': program,
        'academic_year': year,
        'semester': semester,
        'filename': filename
    }

def find_header_row(df, keywords):
    """Scans first 30 rows to find a row containing specific keywords."""
    for idx, row in df.iterrows():
        if idx > 30: break
        row_str = row.astype(str).str.lower().values
        # Check if ANY keyword is present in the row
        if any(k in ' '.join(row_str) for k in keywords):
            return idx
    return None

def process_summary_sheet(df, meta, conn):
    # Find header row with "Avg. Mark" or "Code"
    header_idx = find_header_row(df, ['avg', 'std dev', 'code'])
    if header_idx is None:
        return 0
    
    # Set header
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    
    # Identify necessary columns
    # We need: Course Code, Course Name, Avg Mark, Std Dev, No. Passed, No. Trailed
    # Column names might vary slightly, so we map them
    
    col_map = {}
    for col in df.columns:
        c = str(col).lower().strip()
        if 'code' in c: col_map['course_code'] = col
        elif 'title' in c or 'name' in c: col_map['course_name'] = col
        elif 'avg' in c: col_map['avg_mark'] = col
        elif 'std' in c: col_map['std_dev'] = col
        elif 'pass' in c and 'rate' not in c: col_map['num_passed'] = col # "No. Passed"
        elif 'trail' in c: col_map['num_trailed'] = col
        elif 'pass' in c and 'rate' in c: col_map['pass_rate'] = col

    if 'course_code' not in col_map:
        return 0

    records = []
    for _, row in df.iterrows():
        # Stop if course code is empty (end of table)
        if pd.isna(row[col_map['course_code']]):
            continue
            
        record = {
            'faculty': meta['faculty'],
            'program': meta['program'], # Department treated as Program loosely here
            'department': meta['faculty'], # Placeholder or derived
            'academic_year': meta['academic_year'],
            'semester': meta['semester'],
            'source_file': meta['filename'],
            'course_code': str(row[col_map['course_code']]),
            'course_name': str(row.get(col_map.get('course_name'), '')),
            'avg_mark': clean_mark(row.get(col_map.get('avg_mark'))),
            'std_dev': clean_mark(row.get(col_map.get('std_dev'))),
            'pass_rate': clean_mark(row.get(col_map.get('pass_rate'))),
            'num_passed': clean_mark(row.get(col_map.get('num_passed'))), # might be int, clean_mark returns float
            'num_trailed': clean_mark(row.get(col_map.get('num_trailed')))
        }
        records.append(record)

    if records:
        # Enforce column order to match DB schema
        # Schema: faculty, department, program, academic_year, semester, course_code, course_name, avg_mark, std_dev, pass_rate, num_passed, num_trailed, source_file
        cols = ['faculty', 'department', 'program', 'academic_year', 'semester', 'course_code', 'course_name', 
                'avg_mark', 'std_dev', 'pass_rate', 'num_passed', 'num_trailed', 'source_file']
        df_out = pd.DataFrame(records)[cols]
        conn.execute("INSERT INTO course_summary SELECT * FROM df_out")
        return len(records)
    return 0

def process_detailed_sheet(df, meta, conn):
    # Find header with 'Student ID' or 'Index No'
    header_idx = find_header_row(df, ['student id', 'index no', 'name', 'registration'])
    if header_idx is None:
        return 0
    
    # Check for Course Codes in row above header
    # Some sheets have Course Codes in row X-1 and Student ID in row X
    code_row_idx = None
    for r in range(header_idx - 1, max(-1, header_idx - 5), -1):
        row_vals = df.iloc[r].astype(str).tolist()
        # Count matches (allow \n in match)
        matches = sum(1 for v in row_vals if re.search(r'^[A-Za-z]{2,4}\s*\d{3}', str(v).strip(), re.DOTALL))
        if matches >= 2: # At least 2 course codes found
            code_row_idx = r
            break
            
    # Set header
    if code_row_idx is not None:
        # Merge headers
        base_cols = df.iloc[header_idx].astype(str).values
        code_cols = df.iloc[code_row_idx].astype(str).values
        
        new_cols = []
        for i in range(len(base_cols)):
            c_code = code_cols[i] if i < len(code_cols) else ''
            c_base = base_cols[i] if i < len(base_cols) else ''
            
            # If code matches regex, use it
            if re.search(r'^[A-Za-z]{2,4}\s*\d{3}', str(c_code).strip(), re.DOTALL):
                new_cols.append(str(c_code).strip())
            else:
                new_cols.append(str(c_base).strip())
        df.columns = new_cols
    else:
        df.columns = df.iloc[header_idx]
        
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    
    # Identify content columns vs metadata columns
    # Metadata: Index No, Name, CWA, etc.
    # Content: Course Codes (usually 3-4 letters + numbers)
    
    id_col = None
    cols_str = [str(c).lower().strip() for c in df.columns]
    
    # Better column finding logic
    possible_id_cols = ['student id', 'index no', 'registration number', 'index number']
    
    for i, col_name in enumerate(cols_str):
        if any(p in col_name for p in possible_id_cols):
             id_col = df.columns[i]
             break
            
    if not id_col:
        return 0
        
    # Melt (Unpivot)
    # We treat any column that looks like a course code (e.g., MATH 151) as a value column
    # Regex for course code: ^[A-Z]{2,4}\s*\d{3}
    
    course_cols = []
    for col in df.columns:
        if re.search(r'^[A-Za-z]{2,4}\s*\d{3}', str(col).strip(), re.DOTALL):
            course_cols.append(col)
            
    if not course_cols:
        return 0
        
    # Keep ID col
    df_subset = df[[id_col] + course_cols].copy()
    
    # Melt
    df_melted = df_subset.melt(id_vars=[id_col], value_vars=course_cols, 
                               var_name='course_code', value_name='raw_mark')
                               
    records = []
    for _, row in df_melted.iterrows():
        mark = clean_mark(row['raw_mark'])
        # Filter out empty marks to save space/noise
        if mark is not None:
            records.append({
                'faculty': meta['faculty'],
                'department': meta['faculty'], # Placeholder
                'program': meta['program'],
                'academic_year': meta['academic_year'],
                'semester': meta['semester'],
                'student_id': str(row[id_col]),
                'course_code': str(row['course_code']).strip(),
                'mark': mark,
                'source_file': meta['filename']
            })
            
    if records:
        # Schema: faculty, department, program, academic_year, semester, student_id, course_code, mark, source_file
        cols = ['faculty', 'department', 'program', 'academic_year', 'semester', 'student_id', 'course_code', 'mark', 'source_file']
        df_out = pd.DataFrame(records)[cols]
        conn.execute("INSERT INTO student_marks SELECT * FROM df_out")
        return len(records)
    return 0

def process_file(file_path, conn):
    print(f"Processing: {file_path}...")
    try:
        xl = pd.ExcelFile(file_path)
        meta = extract_metadata(file_path)
        
        counts = {'summary': 0, 'detailed': 0}
        
        for sheet_name in xl.sheet_names:
            # Read first 30 rows to classify
            df_preview = pd.read_excel(file_path, sheet_name=sheet_name, nrows=30, header=None)
            
            # Classification
            str_dump = df_preview.astype(str).to_string().lower()
            
            is_summary = ('avg' in str_dump and 'mark' in str_dump) or ('std' in str_dump and 'dev' in str_dump)
            is_detailed = ('student' in str_dump and 'id' in str_dump) or ('index' in str_dump and 'no' in str_dump)
            
            # Read full sheet now (optimization: could re-use preview if careful, but file read is safe)
            # Actually, to properly parse headers which might be at row 20, we need the whole thing or a larger chunk.
            # Reading whole sheet is safer for "Summary" which is small.
            # Detailed sheets might be large but usually under 1000 rows.
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            
            if is_summary:
                c = process_summary_sheet(df, meta, conn)
                counts['summary'] += c
            elif is_detailed:
                c = process_detailed_sheet(df, meta, conn)
                counts['detailed'] += c
                
        return counts
        
    except Exception as e:
        print(f"ERROR processing {file_path}: {e}")
        return {'summary': 0, 'detailed': 0, 'error': str(e)}

# --- Main Scraper ---
def main():
    conn = connect_db()
    
    total_summary_records = 0
    total_mark_records = 0
    
    files_found = 0
    files_processed = 0
    
    print("Starting Ingestion...")
    
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".xlsx") and not file.startswith('~$'):
                files_found += 1
                full_path = os.path.join(root, file)
                
                res = process_file(full_path, conn)
                total_summary_records += res.get('summary', 0)
                total_mark_records += res.get('detailed', 0)
                files_processed += 1
                
    print("\n--- Ingestion Complete ---")
    print(f"Files Processed: {files_processed}/{files_found}")
    print(f"Course Summary Rows: {total_summary_records}")
    print(f"Student Mark Rows: {total_mark_records}")
    
    conn.close()

if __name__ == "__main__":
    main()