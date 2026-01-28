import pandas as pd
import os

target_file = r"c:\Users\USER\Desktop\My Desktop\Projects\COE Data Analysis\data\Faculty of Civil and Geo-Engineering_\ce\ce1_fs_22_23.xlsx"

try:
    xls = pd.ExcelFile(target_file)
    print(f"Sheet Names: {xls.sheet_names}")
    
    keywords = ['student id', 'index no', 'index number', 'id', 'reference no', 'student no']
    
    for sheet in xls.sheet_names:
        print(f"\n--- Sheet: {sheet} (Rows 20-60) ---")
        df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=60)
        # Slicing is on the dataframe logic, but we can just skip printing first 20
        for i, row in df.iterrows():
            if i < 20: continue
            print(f"Row {i}: {row.tolist()}")
        
        print(f"Searching keywords in {sheet}...")
        for i, row in df.iterrows():
            row_str = " ".join([str(x).lower() for x in row.tolist()])
            found = [k for k in keywords if k in row_str]
            if found:
                print(f"  FOUND {found} at Row {i}")
except Exception as e:
    print(e)
