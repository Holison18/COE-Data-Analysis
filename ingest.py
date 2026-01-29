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
    # Drop tables to ensure fresh schema
    conn.execute("DROP TABLE IF EXISTS course_summary")
    conn.execute("DROP TABLE IF EXISTS student_marks") 
    conn.execute("DROP TABLE IF EXISTS student_performance")
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS course_summary (
            faculty VARCHAR,
            department VARCHAR, 
            program VARCHAR,
            academic_year VARCHAR,
            semester INT,
            course_code VARCHAR,
            course_name VARCHAR,
            credits INT,
            avg_mark DOUBLE,
            std_dev DOUBLE,
            num_passed INT,
            num_trailed INT,
            source_file VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS student_performance (
            faculty VARCHAR,
            department VARCHAR,
            program VARCHAR,
            academic_year VARCHAR,
            semester INT,
            student_id VARCHAR,
            course_code VARCHAR,
            mark DOUBLE,
            cwa DOUBLE,
            gender VARCHAR,
            source_file VARCHAR
        )
    """)
    return conn

# --- Helper Functions ---

def clean_mark(val):
    """Handles mixed types and 'Mark\\nGrade' formatting."""
    if pd.isna(val) or val == '' or val == '-':
        return None
    
    val_str = str(val).strip()
    if '\n' in val_str:
        val_str = val_str.split('\n')[0]
    
    try:
        return float(val_str)
    except ValueError:
        return None

def determine_gender(name):
    """Derives gender from name prefix or suffix."""
    if not isinstance(name, str):
        return 'Male' 
    name_check = name.strip().lower()
    # Check for (Miss) suffix or Miss prefix
    if '(miss)' in name_check or name_check.startswith('miss') or name_check.startswith('mrs') or name_check.startswith('ms'):
        return 'Female'
    return 'Male'

def extract_metadata(file_path):
    """Extracts metadata from file path and filename."""
    path_parts = os.path.normpath(file_path).split(os.sep)
    filename = path_parts[-1]
    
    if len(path_parts) >= 3:
        program = path_parts[-2]
        faculty = path_parts[-3]
    else:
        program = "Unknown"
        faculty = "Engineering"

    semester = 1
    if '_ss_' in filename.lower() or 'second' in filename.lower():
        semester = 2
        
    year = None
    year_match = re.search(r'(\d{2})_(\d{2})', filename)
    if year_match:
        y1, y2 = year_match.groups()
        year = f"20{y1}/20{y2}"
    else:
        year_match_alt = re.search(r'20(\d{2})', filename)
        if year_match_alt:
             y = year_match_alt.group(1)
             year = f"20{y}/20{int(y)+1}"

    return {
        'faculty': faculty,
        'program': program,
        'department': program,
        'academic_year': year,
        'semester': semester,
        'filename': filename
    }

def find_header_row(df, keywords, max_rows=30):
    """Scans first 30 rows to find a row containing specific keywords."""
    for idx, row in df.iterrows():
        if idx > max_rows: break
        row_str = row.astype(str).str.lower().values
        if any(k in ' '.join(row_str) for k in keywords):
            return idx
    return None

def process_summary_sheet(df, meta, conn):
    # Strict check: Must have "Avg. Mark" or "Std Dev" in header
    header_idx = find_header_row(df, ['avg', 'std dev', 'code'])
    if header_idx is None:
        return 0
    
    # Set header
    df.columns = df.iloc[header_idx]
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    
    col_map = {}
    for col in df.columns:
        c = str(col).lower().strip()
        if 'code' in c: col_map['course_code'] = col
        elif 'title' in c or 'name' in c: col_map['course_name'] = col
        elif 'avg' in c: col_map['avg_mark'] = col
        elif 'std' in c: col_map['std_dev'] = col
        elif 'pass' in c and 'rate' not in c: col_map['num_passed'] = col
        elif 'trail' in c: col_map['num_trailed'] = col
        elif 'credit' in c or ('cr' in c and len(c) < 5): col_map['credits'] = col

    if 'course_code' not in col_map:
        return 0

    records = []
    for _, row in df.iterrows():
        if pd.isna(row[col_map['course_code']]): continue
            
        records.append({
            'faculty': meta['faculty'],
            'department': meta['department'],
            'program': meta['program'],
            'academic_year': meta['academic_year'],
            'semester': meta['semester'],
            'course_code': str(row[col_map['course_code']]),
            'course_name': str(row.get(col_map.get('course_name'), '')),
            'credits': int(clean_mark(row.get(col_map.get('credits'))) or 0),
            'avg_mark': clean_mark(row.get(col_map.get('avg_mark'))),
            'std_dev': clean_mark(row.get(col_map.get('std_dev'))),
            'num_passed': int(clean_mark(row.get(col_map.get('num_passed'))) or 0),
            'num_trailed': int(clean_mark(row.get(col_map.get('num_trailed'))) or 0),
            'source_file': meta['filename']
        })

    if records:
        df_out = pd.DataFrame(records)
        # Ensure column order matches table
        cols = ['faculty', 'department', 'program', 'academic_year', 'semester', 
                'course_code', 'course_name', 'credits', 'avg_mark', 'std_dev', 
                'num_passed', 'num_trailed', 'source_file']
        # Add missing cols if any (e.g. if num_passed wasn't found)
        for c in cols:
            if c not in df_out.columns: df_out[c] = None
        
        # Create a clean DataFrame with exact column order
        df_final = df_out[cols]
        conn.execute("INSERT INTO course_summary SELECT * FROM df_final")
        return len(records)
    return 0

def process_detailed_sheet(df, meta, conn):
    # Find header with Student ID/Index No
    header_idx = find_header_row(df, ['student id', 'index no', 'index number'])
    if header_idx is None:
        return 0
        
    # Check for Course Codes in row above header (for some formats)
    code_row_idx = None
    # Basic check for row above
    if header_idx > 0:
        prev_row = df.iloc[header_idx-1].astype(str).values
        # usage of Regex to see if many cells look like course codes
        matches = sum(1 for v in prev_row if re.search(r'[A-Za-z]{2,4}\s*\d{3}', str(v)))
        if matches >= 2:
            code_row_idx = header_idx - 1

    # Merge headers if needed
    if code_row_idx is not None:
        base_cols = df.iloc[header_idx].astype(str).values
        code_cols = df.iloc[code_row_idx].astype(str).values
        new_cols = []
        for i in range(len(base_cols)):
            c_code = code_cols[i] if i < len(code_cols) else ''
            c_base = base_cols[i] if i < len(base_cols) else ''
            if re.search(r'[A-Za-z]{2,4}\s*\d{3}', str(c_code)):
                new_cols.append(str(c_code).strip())
            else:
                new_cols.append(str(c_base).strip())
        df.columns = new_cols
    else:
        df.columns = df.iloc[header_idx]
        
    df = df.iloc[header_idx+1:].reset_index(drop=True)
    
    # Identify key columns
    id_col = None
    name_col = None
    cwa_col = None
    
    cols_lower = [str(c).lower().strip() for c in df.columns]
    
    for i, col in enumerate(cols_lower):
        if 'student id' in col or 'index no' in col: id_col = df.columns[i]
        elif 'name' in col and 'code' not in col: name_col = df.columns[i]
        elif 'cwa' in col: cwa_col = df.columns[i]
        
    if not id_col: return 0
    
    # Identify Course Columns
    course_cols = []
    for col in df.columns:
        if re.search(r'^[A-Za-z]{2,4}\s*\d{3}', str(col).strip()):
            course_cols.append(col)
            
    if not course_cols: return 0
    
    # Melt
    keep_cols = [id_col]
    if name_col: keep_cols.append(name_col)
    if cwa_col: keep_cols.append(cwa_col)
    
    df_subset = df[keep_cols + course_cols].copy()
    df_melted = df_subset.melt(id_vars=keep_cols, value_vars=course_cols, 
                               var_name='course_code', value_name='raw_mark')
                               
    records = []
    for _, row in df_melted.iterrows():
        mark = clean_mark(row['raw_mark'])
        if mark is not None:
            gender = 'Male'
            if name_col and pd.notna(row[name_col]):
                gender = determine_gender(str(row[name_col]))
                
            cwa = None
            if cwa_col and pd.notna(row[cwa_col]):
                cwa = clean_mark(row[cwa_col])
                
            records.append({
                'faculty': meta['faculty'],
                'department': meta['department'],
                'program': meta['program'],
                'academic_year': meta['academic_year'],
                'semester': meta['semester'],
                'student_id': str(row[id_col]),
                'course_code': str(row['course_code']).strip(),
                'mark': mark,
                'cwa': cwa,
                'gender': gender,
                'source_file': meta['filename']
            })
            
    if records:
        df_out = pd.DataFrame(records)
        cols = ['faculty', 'department', 'program', 'academic_year', 'semester', 
                'student_id', 'course_code', 'mark', 'cwa', 'gender', 'source_file']
        for c in cols:
            if c not in df_out.columns: df_out[c] = None
        
        # Create a clean DataFrame with exact column order
        df_final = df_out[cols]
        conn.execute("INSERT INTO student_performance SELECT * FROM df_final")
        return len(records)
    return 0

def process_file(file_path, conn):
    print(f"Processing: {file_path}...")
    # try:
    xl = pd.ExcelFile(file_path)
    meta = extract_metadata(file_path)
    counts = {'summary': 0, 'detailed': 0}
    
    for sheet_name in xl.sheet_names:
        # Read first 30 rows for classification
        df_preview = pd.read_excel(file_path, sheet_name=sheet_name, nrows=30, header=None)
        str_dump = df_preview.astype(str).to_string().lower()
        
        is_summary = ('avg' in str_dump and 'mark' in str_dump) or ('std' in str_dump and 'dev' in str_dump)
        is_detailed = ('student' in str_dump and 'id' in str_dump) or ('index' in str_dump and 'no' in str_dump)
        
        # Read full sheet
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
        
        if is_summary:
            counts['summary'] += process_summary_sheet(df, meta, conn)
        elif is_detailed:
            counts['detailed'] += process_detailed_sheet(df, meta, conn)
            
    return counts
    # except Exception as e:
    #     print(f"ERROR {file_path}: {e}")
    #     return {'summary': 0, 'detailed': 0}

def main():
    conn = connect_db()
    print(f"DEBUG: conn object is {conn} type: {type(conn)}")
    total_summary = 0
    total_perf = 0
    
    print("Starting Ingestion...")
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".xlsx") and not file.startswith('~$'):
                full_path = os.path.join(root, file)
                res = process_file(full_path, conn)
                total_summary += res['summary']
                total_perf += res['detailed']
                
    print(f"\nCompleted. Summary Rows: {total_summary}, Performance Rows: {total_perf}")
    conn.close()

if __name__ == "__main__":
    main()