
import pandas as pd
import os

file_path = r"c:\Users\USER\Desktop\My Desktop\Projects\COE Data Analysis\Data\Faculty of Civil and Geo-Engineering\geol\geol1_fs_18_19.xlsx"

print(f"Inspecting: {file_path}")

try:
    # Load Sheet2 directly as it seems to be the main sheet
    df = pd.read_excel(file_path, sheet_name='Sheet2', header=None)
    
    # 1. Find Header Row
    header_idx = None
    for idx, row in df.iterrows():
        row_str = row.astype(str).str.lower().values
        if 'index' in row_str or 'name' in row_str:
            header_idx = idx
            break
            
    print(f"\nHeader Row Index: {header_idx}")
    if header_idx is not None:
        headers = df.iloc[header_idx]
        print("Headers:")
        print(headers.to_string())
        
        # Set headers
        df.columns = headers
        df = df.iloc[header_idx+1:]
        
        # 2. Inspect Index Numbers
        # Find column with 'Index'
        index_col = None
        for col in df.columns:
            if 'index' in str(col).lower():
                index_col = col
                break
        
        if index_col:
            print(f"\n--- Index Column: {index_col} ---")
            print(df[index_col].head(10).to_string())
            print("...")
            print(df[index_col].tail(10).to_string())
            
        # 3. Inspect Status/Remarks
        # Check for keywords in ANY column
        keywords = ['withdrawn', 'defer', 'abandon', 'suspended']
        print(f"\n--- Rows with keywords {keywords} ---")
        
        df_str = df.astype(str).apply(lambda x: x.str.lower())
        mask = df_str.apply(lambda x: x.str.contains('|'.join(keywords), na=False)).any(axis=1)
        
        if mask.any():
            subset = df[mask]
            print(subset.to_string())
        else:
            print("No keywords found in remaining rows.")

except Exception as e:
    print(f"Error: {e}")
