#!/usr/bin/env python3
"""
Process BP terminal gate pricing Excel file into clean CSV with history tracking.
"""

import pandas as pd
import os
from datetime import datetime
import hashlib
import sys

def get_file_hash(filepath):
    """Calculate MD5 hash of a file."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def parse_bp_pricing(excel_file):
    """Parse the messy BP pricing Excel file into a clean dataframe."""
    df_raw = pd.read_excel(excel_file, sheet_name=0, header=None)
    
    data_records = []
    
    # Find state sections and parse them
    current_state = None
    header_row = None
    
    for i, row in df_raw.iterrows():
        # Skip empty rows
        if pd.isna(row[0]):
            continue
        
        # Detect state headers (they're alone in column 0)
        if isinstance(row[0], str) and row[0].strip() and all(pd.isna(row[j]) for j in range(1, len(row))):
            # Skip the metadata lines and title
            if row[0] not in ['BP terminal gate pricing by state', 
                             'These prices are current and displayed in Australian cents per litre with GST included.',
                             'Fuels are sold at temperature-corrected volumes as legislated by the federal government.']:
                current_state = row[0].strip()
            continue
        
        # Detect header row (has "Effective Date", "Terminal", fuel types)
        if isinstance(row[2], str) and row[2].strip() == 'Terminal':
            header_row = {
                'date_col': 0,
                'terminal_col': 2,
                'fuel_cols': {row[j].strip(): j for j in range(3, len(row)) if not pd.isna(row[j]) and isinstance(row[j], str)}
            }
            continue
        
        # Parse data rows
        if current_state and header_row and isinstance(row[0], (pd.Timestamp, datetime)):
            effective_date = row[0]
            terminal = str(row[header_row['terminal_col']]).strip() if not pd.isna(row[header_row['terminal_col']]) else None
            
            if terminal and terminal != 'nan':
                for fuel, col_idx in header_row['fuel_cols'].items():
                    price = row[col_idx]
                    if not pd.isna(price):
                        data_records.append({
                            'state': current_state,
                            'effective_date': effective_date.date(),
                            'terminal': terminal,
                            'fuel_type': fuel,
                            'price_cents_per_litre': float(price),
                            'scraped_timestamp': datetime.now().isoformat()
                        })
    
    return pd.DataFrame(data_records)

def main():
    # Find the latest .xlsx file (should be the downloaded one)
    xlsx_files = [f for f in os.listdir('.') if f.endswith('.xlsx') and not f.endswith('.html.xlsx')]
    
    if not xlsx_files:
        print("No .xlsx file found")
        sys.exit(1)
    
    latest_xlsx = max(xlsx_files, key=os.path.getctime)
    current_hash = get_file_hash(latest_xlsx)
    
    # Check if we have a hash record
    hash_file = '.bp_pricing_hash'
    history_file = 'bp_pricing_history.csv'
    
    if os.path.exists(hash_file):
        with open(hash_file, 'r') as f:
            last_hash = f.read().strip()
        
        if current_hash == last_hash:
            print(f"File unchanged (hash: {current_hash}). Skipping update.")
            return
    
    # Parse the file
    print(f"Processing {latest_xlsx}...")
    df_new = parse_bp_pricing(latest_xlsx)
    
    if df_new.empty:
        print("No data extracted from file")
        sys.exit(1)
    
    # Append to history or create new
    if os.path.exists(history_file):
        df_history = pd.read_csv(history_file, parse_dates=['effective_date', 'scraped_timestamp'])
        df_combined = pd.concat([df_history, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=['state', 'effective_date', 'terminal', 'fuel_type'], keep='last')
    else:
        df_combined = df_new
    
    # Sort by state, effective_date, terminal for readability
    df_combined = df_combined.sort_values(['state', 'effective_date', 'terminal', 'fuel_type'], ascending=[True, False, True, True])
    
    # Write history
    df_combined.to_csv(history_file, index=False)
    print(f"Updated {history_file} with {len(df_combined)} records")
    
    # Write clean recent data (latest date only)
    latest_date = df_combined['effective_date'].max()
    df_latest = df_combined[df_combined['effective_date'] == latest_date].copy()
    df_latest = df_latest.sort_values(['state', 'terminal', 'fuel_type'])
    
    df_latest.to_csv('bp_pricing_latest.csv', index=False)
    print(f"Updated bp_pricing_latest.csv with {len(df_latest)} records (as of {latest_date})")
    
    # Save hash
    with open(hash_file, 'w') as f:
        f.write(current_hash)
    
    print(f"Hash saved: {current_hash}")

if __name__ == '__main__':
    main()
