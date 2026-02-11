
import pandas as pd
import re
import os

file_path = r"c:\Users\USER\Desktop\My Desktop\Projects\COE Data Analysis\Data\Faculty of Civil and Geo-Engineering\geol\geol1_fs_18_19.xlsx"

print(f"Debugging Status Extraction on: {file_path}")

try:
    df = pd.read_excel(file_path, sheet_name='Sheet2', header=None)
    
    print(f"Total rows: {len(df)}")
    
    # Simulate finding header row
    header_idx = None
    for idx, row in df.iterrows():
        row_str = row.astype(str).str.lower().values
        if 'index' in row_str or 'name' in row_str:
            header_idx = idx
            break
            
    if header_idx is None:
        print("Header not found")
        exit()
        
    print(f"Header Index: {header_idx}")
    
    # Slice
    df_raw = df.iloc[header_idx+1:].copy()
    
    special_status = {}
    keywords = ['withdrawn', 'defer', 'abandon', 'suspended', 'trail']
    
    found_rows = []
    
    print("\n--- Scanning for keywords ---")
    for idx, row in df_raw.iterrows():
        row_str = " ".join(row.astype(str).values).lower()
        
        # Check for keywords
        found_kw = [k for k in keywords if k in row_str]
        
        if found_kw:
             print(f"\n--- MATCH at Row {idx} ({found_kw}) ---")
             # Print 3 rows before and 3 after
             start = max(0, idx - 3)
             end = min(len(df_raw) + header_idx + 1, idx + 4) # absolute index in df
             
             # df_raw index is preserved from df
             
             # Print columns 0-15 for context
             subset = df.loc[max(0, idx-5):min(len(df), idx+5), 0:15]
             print(subset.to_string())
             
             ids = re.findall(r'\b\d{5,10}\b', row_str)
             print(f"  > Extracted IDs in row: {ids}")
             
             # If this is a header like "ABANDONED", look below
             if 'abandon' in row_str or 'withdraw' in row_str:
                 print("   !!! Potential Header found. Printing next 10 rows...")
                 subset_below = df.loc[idx+1:idx+10, 0:10]
                 print(subset_below.to_string())
             
except Exception as e:
    print(f"Error: {e}")
