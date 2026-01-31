import pandas as pd
import os

target_file = r"data\Faculty of Civil and Geo-Engineering_\ce\ce1_fs_18_19.xlsx"

print(f"Inspecting: {target_file}")
xl = pd.ExcelFile(target_file)
print("Sheets:", xl.sheet_names)

for sheet in xl.sheet_names:
    print(f"\n--- Sheet: {sheet} ---")
    df = pd.read_excel(target_file, sheet_name=sheet, header=None)
    
    # Print first 25 rows as string to see what we are scanning
    print(df.head(25).to_string())
    
    # Test our detection logic
    str_dump = df.head(30).astype(str).to_string().lower()
    is_summary = ('avg' in str_dump and 'mark' in str_dump) or ('std' in str_dump and 'dev' in str_dump)
    is_detailed = ('student' in str_dump and 'id' in str_dump) or ('index' in str_dump and 'no' in str_dump)
    
    print(f"Detected as Summary? {is_summary}")
    print(f"Detected as Detailed? {is_detailed}")
