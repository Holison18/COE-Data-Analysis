import os
import re
import pandas as pd
import duckdb
import numpy as np

# --- Configuration ---
DB_PATH = 'knust_engineering_new.duckdb'
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
            level INT,
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
            level INT,
            student_id VARCHAR,
            course_code VARCHAR,
            mark DOUBLE,
            cwa DOUBLE,
            gender VARCHAR,
            admission_year INT,
            status VARCHAR DEFAULT 'Active',
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
             
    # Extract Level from filename, e.g., coe1_... -> 1
    level = None
    # Look for digit straight after program code which is usually at start
    # Simplified regex: look for digit before '_fs_' or '_ss_'
    level_match = re.search(r'([1-6])_(fs|ss)_', filename.lower())
    if level_match:
        level = int(level_match.group(1))
    else:
        # Fallback: look for generic digit if structure differs
        # This assumes the first digit in the name corresponds to level, which holds for provided examples (coe1, bme3)
        # But be careful of years.
        # Let's try matching [alpha]+[digit] e.g. coe1
        level_match_alt = re.search(r'^[a-zA-Z]+(\d)', filename)
        if level_match_alt:
            level = int(level_match_alt.group(1))

    return {
        'faculty': faculty,
        'program': program,
        'department': program,
        'academic_year': year,
        'semester': semester,
        'level': level,
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
            'level': meta['level'],
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
        cols = ['faculty', 'department', 'program', 'academic_year', 'semester', 'level',
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
    # 1. Parsing Optimization: Detect Header Row
    # We look for "Index No" specifically as it is the most reliable identifier
    header_idx = find_header_row(df, ['index no', 'index number', 'student id'])
    if header_idx is None:
        return 0
        
    # Check for Course Codes in row above header (for some formats)
    code_row_idx = None
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
        
    # Slice the dataframe to get data AND potential footer info
    # We don't drop NaNs immediately because footer info might be there
    df_raw = df.iloc[header_idx+1:].copy()
    
    # Identify key columns
    id_col = None
    name_col = None
    cwa_col = None
    
    cols_lower = [str(c).lower().strip() for c in df_raw.columns]
    
    for i, col in enumerate(cols_lower):
        if 'index no' in col or 'student id' in col: id_col = df_raw.columns[i]
        elif 'name' in col and 'code' not in col: name_col = df_raw.columns[i]
        elif 'cwa' in col: cwa_col = df_raw.columns[i]
        
    if not id_col: return 0
    
    # 2. Extract Withdrawn/Deferred Information (from Footer/Remarks)
    # Scan for headers and subsequent list of students
    footer_records = []
    current_footer_status = None
    
    # We iterate through the raw rows
    for idx, row in df_raw.iterrows():
        row_str = " ".join(row.astype(str).values).lower()
        
        # Detect Status Header
        if 'abandon' in row_str or 'withdrawn' in row_str:
            current_footer_status = 'Withdrawn'
            continue 
        elif 'defer' in row_str or 'suspended' in row_str:
            current_footer_status = 'Deferred'
            continue
            
        # If we are in a status section, look for students
        if current_footer_status:
            # Look for patterns: 2 distinct numbers (ID & Index) or at least 1
            # We assume valid IDs are 5-10 digits
            numbers = re.findall(r'\b\d{5,10}\b', row_str)
            
            if numbers:
                # Heuristic:
                # If 2 numbers, one is ID, one is Index.
                # Index usually determines year.
                # Student ID is the identifier.
                
                # Pick the longest number as Student ID? 
                # Or just the first one?
                # Usually Student ID (8 digits) > Index (7 digits) or similar.
                # Let's take the first one as Student ID for now.
                s_id = numbers[0]
                
                # Try to parse Admission Year from ANY number in the row
                adm_year = None
                for n in numbers:
                    if len(n) >= 2:
                        try:
                            suffix = int(n[-2:])
                            # Valid window 2000-2030
                            if 10 <= suffix <= 40:
                                val = 2000 + suffix
                                # Heuristic: Admission year should be close to academic_year of file
                                # e.g. file is 2018/2019. Admission could be 2018, 2017, 2016...
                                # If file is 2018, adm_year 2025 is impossible.
                                # Parse file year:
                                file_year_start = None
                                try:
                                    file_year_start = int(meta['academic_year'].split('/')[0])
                                except:
                                    pass
                                    
                                if file_year_start and val <= file_year_start:
                                     adm_year = val
                        except:
                            pass
                
                if s_id:
                    footer_records.append({
                        'faculty': meta['faculty'],
                        'department': meta['department'],
                        'program': meta['program'],
                        'academic_year': meta['academic_year'],
                        'semester': meta['semester'],
                        'level': meta['level'],
                        'student_id': s_id,
                        'course_code': 'STATUS_RECORD', # Dummy course code
                        'mark': None,
                        'cwa': None,
                        'gender': 'Unknown', # Could parse name if needed
                        'admission_year': adm_year,
                        'status': current_footer_status,
                        'source_file': meta['filename']
                    })
    
    # 3. Process Main Student Data
    # Filter for valid rows (must have a valid Student ID)
    # Valid ID assumption: digits, at least 5 chars
    if id_col:
        mask_valid_id = df_raw[id_col].astype(str).str.contains(r'\d{5,}', na=False)
        df = df_raw[mask_valid_id].copy()
    else:
        df = df_raw # Should not happen based on check above
    
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
        # We allow None mark to capture the student existence if they are in the list
        # But usually we only want distinct student/course combos
        
        # Capture Student ID
        s_id = str(row[id_col]).strip()
        
        gender = 'Male'
        if name_col and pd.notna(row[name_col]):
            gender = determine_gender(str(row[name_col]))
            
        cwa = None
        if cwa_col and pd.notna(row[cwa_col]):
            cwa = clean_mark(row[cwa_col])
            
        # Parse Admission Year from Index (last 2 digits)
        # e.g. 52718 -> 18 -> 2018
        # e.g. 207019 -> 19 -> 2019
        adm_year = None
        try:
            # simple digit extraction
            digits = re.findall(r'\d+', s_id)
            if digits:
                full_num = digits[0]
                if len(full_num) >= 2:
                    suffix = full_num[-2:]
                    adm_year = int(f"20{suffix}")
        except:
             pass
             
        # Determine Status
        # Default Active
        status = 'Active'
        # Check if this student explicitly marked in footer?
        # (Usually footer students are REMOVED from main table, but if not, we prioritize footer status)
        # But we don't have a dict anymore.
        
        if mark is not None:
            records.append({
                'faculty': meta['faculty'],
                'department': meta['department'],
                'program': meta['program'],
                'academic_year': meta['academic_year'],
                'semester': meta['semester'],
                'level': meta['level'],
                'student_id': s_id,
                'course_code': str(row['course_code']).strip(),
                'mark': mark,
                'cwa': cwa,
                'gender': gender,
                'admission_year': adm_year,
                'status': status,
                'source_file': meta['filename']
            })
            
    # Add Footer Records
    # Filter duplicates? If a student is in main table AND footer?
    # Usually they are mutually exclusive in these sheets.
    # But if they are in both, footer status (Withdrawn) should probably take precedence?
    # For now, just append them.
    records.extend(footer_records)
            
    if records:
        df_out = pd.DataFrame(records)
        cols = ['faculty', 'department', 'program', 'academic_year', 'semester', 'level',
                'student_id', 'course_code', 'mark', 'cwa', 'gender', 'admission_year', 'status', 'source_file']
        for c in cols:
            if c not in df_out.columns: df_out[c] = None
        
        # Create a clean DataFrame with exact column order
        df_final = df_out[cols]
        try:
            conn.execute("INSERT INTO student_performance SELECT * FROM df_final")
        except Exception as e:
            print(f"Error inserting: {e}")
            return 0
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