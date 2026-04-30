import duckdb
import pandas as pd
import numpy as np

def generate_report():
    # Database path
    db_path = 'knust_engineering_new.duckdb'
    output_file = 'Academic_Performance_Report.xlsx'
    
    print(f"Connecting to database: {db_path}...")
    # Connect to DuckDB database in read-only mode (allows concurrent access)
    con = duckdb.connect(db_path, read_only=True)
    
    # Query data: Aggregate passing and trailing students per program, course, and year
    query = """
    SELECT 
        UPPER(program) as program,
        course_code,
        MAX(course_name) as course_name,
        MAX(level) as level,
        academic_year,
        SUM(num_passed) as num_passed,
        SUM(num_trailed) as num_trailed
    FROM course_summary
    WHERE program IS NOT NULL AND academic_year IS NOT NULL
    GROUP BY program, course_code, academic_year
    """
    df = con.execute(query).df()
    
    if df.empty:
        print("No data found in the course_summary table.")
        return

    print("Processing data...")
    # Calculate Pass Rate: Total Passing / (Total Passing + Total Trailing)
    df['enrolled'] = df['num_passed'] + df['num_trailed']
    # Handle division by zero safely
    df['pass_rate'] = np.where(df['enrolled'] > 0, df['num_passed'] / df['enrolled'], np.nan)
    
    # Pivot the data to have academic years as columns
    pivot_df = df.pivot(index=['program', 'course_code', 'course_name', 'level'], 
                        columns='academic_year', 
                        values='pass_rate').reset_index()
                        
    # Sort columns to ensure years are in order
    year_cols = sorted([col for col in pivot_df.columns if '/' in str(col)])
    cols = ['program', 'course_code', 'course_name', 'level'] + year_cols
    pivot_df = pivot_df[cols]
    
    # Rename base columns for presentation
    rename_dict = {
        'course_code': 'Course Code',
        'course_name': 'Course Name',
        'level': 'Level'
    }
    pivot_df.rename(columns=rename_dict, inplace=True)
    
    print(f"Writing to Excel workbook: {output_file}...")
    # Use xlsxwriter engine for professional formatting
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'vcenter',
            'align': 'center',
            'fg_color': '#4F81BD',
            'font_color': 'white',
            'border': 1
        })
        
        percent_format = workbook.add_format({
            'num_format': '0.0%',
            'border': 1,
            'align': 'center'
        })
        
        text_format = workbook.add_format({
            'border': 1,
            'valign': 'vcenter'
        })
        
        # We replace NaNs with 'N/A' before writing so they appear explicitly as N/A
        # However, to let Excel treat percentages as numbers, we shouldn't convert the whole column to string.
        # pandas to_excel with na_rep='N/A' writes empty cells as 'N/A' string.
        
        programs = pivot_df['program'].dropna().unique()
        
        for prog in sorted(programs):
            # Filter data for the specific program
            prog_df = pivot_df[pivot_df['program'] == prog].copy()
            
            # Sort logically by Level then Course Code
            prog_df.sort_values(by=['Level', 'Course Code'], inplace=True)
            
            # Drop the program column as it serves as the sheet name
            prog_df.drop(columns=['program'], inplace=True)
            
            # Clean sheet name (Excel limits sheet names to 31 chars and no special chars like '/', '\', '?', '*', '[', ']')
            sheet_name = str(prog)[:31].replace('/', '_').replace('\\', '_')
            if not sheet_name:
                continue
                
            # Write to excel sheet, representing nulls as 'N/A'
            prog_df.to_excel(writer, sheet_name=sheet_name, index=False, na_rep='N/A')
            
            # Get the worksheet to apply specific formats
            worksheet = writer.sheets[sheet_name]
            
            # Freeze the top row
            worksheet.freeze_panes(1, 0)
            
            # Overwrite headers with our custom format
            for col_num, value in enumerate(prog_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                
            # Set column widths and formats
            # Course Code
            worksheet.set_column(0, 0, 15, text_format)
            # Course Name
            worksheet.set_column(1, 1, 45, text_format)
            # Level
            worksheet.set_column(2, 2, 10, text_format)
            
            # Pass Rate columns (years)
            for i in range(3, len(prog_df.columns)):
                worksheet.set_column(i, i, 15, percent_format)

    print("Successfully generated publication-ready Excel workbook.")

if __name__ == '__main__':
    generate_report()
