from ingest import process_file
import os

target_file = r"c:\Users\USER\Desktop\My Desktop\Projects\COE Data Analysis\data\Faculty of Civil and Geo-Engineering_\ce\ce1_fs_22_23.xlsx"

print(f"Testing ingestion on: {target_file}")
data = process_file(target_file)

print(f"\n--- Result ---")
print(f"Extracted {len(data)} records.")
if data:
    print("First Record:", data[0])
else:
    print("FAILED to extract data.")
