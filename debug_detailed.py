import pandas as pd
import re

def find_header_row(df, keywords):
    """Scans first 30 rows to find a row containing specific keywords."""
    for idx, row in df.iterrows():
        if idx > 30: break
        row_str = row.astype(str).str.lower().values
        # Check if ANY keyword is present in the row
        if any(k in ' '.join(row_str) for k in keywords):
            return idx
    return None

def debug_detailed(file_path, sheet_name):
    print(f"DEBUGGING {file_path} - {sheet_name}")
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    
    
    # 1. Header Finding
    header_idx = find_header_row(df, ['student id', 'index no', 'name', 'registration'])
    print(f"Header Row Index: {header_idx}")
    
    if header_idx is None:
        print("FAIL: Header not found.")
        return

    # Print context around expected header - CLEANER
    start = max(0, header_idx - 5)
    end = header_idx + 2
    print(f"\n--- Checking Rows {start} to {end} for Course Codes ---")
    for r in range(start, end):
        row_vals = df.iloc[r].dropna().astype(str).tolist()
        print(f"Row {r}: {row_vals}")
    id_col = None
    cols_str = [str(c).lower().strip() for c in df.columns]
    possible_id_cols = ['student id', 'index no', 'registration number', 'index number']
    
    for i, col_name in enumerate(cols_str):
        if any(p in col_name for p in possible_id_cols):
             id_col = df.columns[i]
             break
             
    if not id_col:
        print("FAIL: ID Column not found.")
        return

    # 3. Look for Course Code Row
    code_row_idx = None
    for r in range(header_idx - 1, max(-1, header_idx - 5), -1):
        row_vals = df.iloc[r].astype(str).tolist()
        # Count matches
        matches = sum(1 for v in row_vals if re.match(r'^[A-Za-z]{2,4}\s*\d{3}', str(v).strip()))
        if matches > 2: # Arbitrary threshold: at least 3 course codes
            code_row_idx = r
            print(f"FOUND Course Code Row at {r} with {matches} matches.")
            break
            
    if code_row_idx is not None:
        # Merge Headers
        # We take the ID row (header_idx) as base.
        # We overwrite column names with Course Codes where present in code_row_idx
        
        base_cols = df.iloc[header_idx].astype(str).values
        code_cols = df.iloc[code_row_idx].astype(str).values
        
        new_cols = []
        for i in range(len(base_cols)):
            c_code = code_cols[i] if i < len(code_cols) else ''
            c_base = base_cols[i] if i < len(base_cols) else 'nan'
            
            if re.match(r'^[A-Za-z]{2,4}\s*\d{3}', str(c_code).strip()):
                new_cols.append(c_code.strip())
            else:
                new_cols.append(c_base.strip())
                
        print("Merged Columns:", new_cols)
        
        # Check if regex works on NEW cols
        course_cols = [c for c in new_cols if re.match(r'^[A-Za-z]{2,4}\s*\d{3}', str(c))]
        print(f"Course Columns after merge ({len(course_cols)}):", course_cols)

    else:
        print("FAIL: No Course Code row found above header.")

target_file = r"data\Faculty of Civil and Geo-Engineering_\ce\ce1_fs_18_19.xlsx"
debug_detailed(target_file, "Sheet2")
