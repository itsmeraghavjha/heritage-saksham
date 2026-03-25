import pandas as pd
import sqlite3
import os

SQLITE_PATH = r"C:\AnalyticsPortal\portal.db"

def upload_contacts_to_db(txt_file_path):
    print(f"Reading {txt_file_path}...")
    
    # 1. Read the pipe-delimited text file
    # sep='|' tells pandas how to split the columns
    df = pd.read_csv(txt_file_path, sep='|', dtype=str)
    
    # Strip any accidental whitespace from column headers
    df.columns = df.columns.str.strip()
    
    # 2. Extract and format the columns we need
    # Drop rows where HPC_Code is empty
    df = df.dropna(subset=['HPC_Code'])
    
    # Construct the hpc_plant_key
    # Note: If your database uses a different format (like "1202-2100" instead of "1202_2100"),
    # just change the "_" to a "-" in the line below!
    df['hpc_plant_key'] = df['Plant_Code'] + "_" + df['HPC_Code']
    
    # Create a clean dataframe that exactly matches your SQLite table columns
    contacts_df = pd.DataFrame({
        'hpc_plant_key': df['hpc_plant_key'],
        'hpr_name': df['HPR_Name'],
        'mobile_no': df['Mobile No']
    })
    
    # Clean up: Replace 'nan' or empty values with actual blanks
    contacts_df = contacts_df.fillna('')
    
    # 3. Connect to SQLite and insert
    conn = sqlite3.connect(SQLITE_PATH)
    
    print(f"Inserting into table 'hpr_contacts'...")
    # if_exists='replace' drops the old table and inserts this fresh list
    contacts_df.to_sql('hpr_contacts', conn, if_exists='replace', index=False)
    
    conn.close()
    print(f"✅ Successfully inserted {len(contacts_df)} contacts into the database!")

# --- RUN THE IMPORTER ---
if __name__ == "__main__":
    # Update this path to wherever your HPC_Master.TXT is saved
    file_path = r"C:\Users\raghavkumar.j\Desktop\demo\Heritage Saksham\HPC_Master.TXT"
    
    if os.path.exists(file_path):
        upload_contacts_to_db(file_path)
    else:
        print(f"❌ Could not find file at: {file_path}")