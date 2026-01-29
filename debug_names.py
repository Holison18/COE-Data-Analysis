import pandas as pd
import os

print("Scanning for names...")
count = 0
for root, dirs, files in os.walk('data'):
    for file in files:
        if file.endswith('.xlsx') and not file.startswith('~$'):
            # Check detailed sheets (Student IDs)
            fpath = os.path.join(root, file)
            try:
                xl = pd.ExcelFile(fpath)
                for sheet in xl.sheet_names:
                    df = pd.read_excel(fpath, sheet_name=sheet, nrows=50, header=None)
                    s = df.astype(str).to_string().lower()
                    if 'student' in s or 'index' in s:
                        # Likely detailed
                        # Find header
                        for idx, row in df.iterrows():
                            rstr = row.astype(str).str.lower().tolist()
                            if any('name' in x for x in rstr):
                                # found header
                                df.columns = df.iloc[idx]
                                df = df.iloc[idx+1:]
                                # Find name col
                                name_col = [c for c in df.columns if 'name' in str(c).lower() and 'code' not in str(c).lower()]
                                if name_col:
                                    print(f"--- File: {file} Sheet: {sheet} ---")
                                    print(df[name_col[0]].head(10).tolist())
                                    count += 1
                                    break
                    if count >= 3: break
            except: pass
        if count >= 3: break
    if count >= 3: break
