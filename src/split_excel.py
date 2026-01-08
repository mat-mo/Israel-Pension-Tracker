import pandas as pd
import os

# Configuration
EXCEL_FILE = "*.xlsx"  # Make sure this matches your file name

print(f"Loading {EXCEL_FILE}...")

# Load the Excel file
try:
    xls = pd.ExcelFile(EXCEL_FILE)
    
    # Iterate through each sheet and save as CSV
    for sheet_name in xls.sheet_names:
        print(f"Processing sheet: {sheet_name}")
        
        # Read the sheet
        df = pd.read_excel(xls, sheet_name=sheet_name)
        
        # Create a filename that matches the pattern expected by the generator script
        # Pattern: "Filename - Sheetname.csv"
        csv_filename = f"{EXCEL_FILE} - {sheet_name}.csv"
        
        # Save to CSV
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"Saved: {csv_filename}")

    print("\nSuccess! All sheets have been converted to CSVs.")

except FileNotFoundError:
    print(f"Error: Could not find file '{EXCEL_FILE}' in the current directory.")
except Exception as e:
    print(f"An error occurred: {e}")
