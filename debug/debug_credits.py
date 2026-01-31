import pandas as pd
import os

# Find a file to test
target_file = None
for root, dirs, files in os.walk('data'):
    for file in files:
        if file.endswith('.xlsx') and not file.startswith('~$'):
            target_file = os.path.join(root, file)
            break
    if target_file: break

if target_file:
    print(f"Inspecting: {target_file}")
    xl = pd.ExcelFile(target_file)
    
    # Check first sheet (Summary)
    df = pd.read_excel(target_file, sheet_name=0, header=None)
    print("\n--- Sheet 1 Raw (First 40 rows) ---")
    # Print rows to find header
    for i, row in df.head(40).iterrows():
        print(f"Row {i}: {row.astype(str).tolist()}")
else:
    print("No excel file found.")
