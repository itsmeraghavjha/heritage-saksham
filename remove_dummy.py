import pandas as pd
from pathlib import Path

PARQUET_DIR = Path(r"C:\AnalyticsPortal\parquet")

def clean_parquet_files():
    if not PARQUET_DIR.exists():
        print(f"Error: Directory {PARQUET_DIR} not found.")
        return

    files = list(PARQUET_DIR.glob("proc_*.parquet"))
    print(f"Found {len(files)} parquet files. Scanning for dummy farmer '9999'...")
    
    total_removed = 0
    files_modified = 0

    for f in files:
        try:
            # Read the parquet file
            df = pd.read_parquet(f)
            initial_len = len(df)
            
            # Ensure farmer_code is a string and check for 9999
            # We use endswith to catch both '9999' and padded versions like '000009999'
            is_dummy = df['farmer_code'].astype(str).str.endswith('9999')
            
            if is_dummy.any():
                removed_count = is_dummy.sum()
                
                # Filter out the dummy rows
                df_clean = df[~is_dummy]
                
                # Save it back to the exact same file, overwriting the old one
                df_clean.to_parquet(f, index=False, compression="snappy")
                
                print(f"  - {f.name}: Removed {removed_count} rows")
                total_removed += removed_count
                files_modified += 1
                
        except Exception as e:
            print(f"  - Error processing {f.name}: {e}")

    print("\n" + "="*40)
    print(f"Cleanup Complete!")
    print(f"Files modified: {files_modified} out of {len(files)}")
    print(f"Total dummy rows permanently deleted: {total_removed}")
    print("="*40)

if __name__ == "__main__":
    clean_parquet_files()