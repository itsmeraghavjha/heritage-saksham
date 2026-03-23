import sqlite3
import csv
import logging

logging.basicConfig(level=logging.INFO)

# --- UPDATE THESE PATHS IF NEEDED ---
DB_PATH = r"C:\AnalyticsPortal\portal.db"
TXT_PATH = r"C:\Users\raghavkumar.j\Desktop\demo\Heritage Saksham\HPC_Master copy.TXT" 

def import_hpr_contacts():
    conn = sqlite3.connect(DB_PATH)
    
    # 1. Add the new columns to the snapshot table safely
    try:
        conn.execute("ALTER TABLE proc_period_snapshot ADD COLUMN hpr_name TEXT;")
        conn.execute("ALTER TABLE proc_period_snapshot ADD COLUMN mobile_no TEXT;")
        logging.info("Added new columns: hpr_name, mobile_no")
    except sqlite3.OperationalError:
        logging.info("Columns already exist, proceeding to update.")

    # 2. Read the text file and update the database
    updated_count = 0
    with open(TXT_PATH, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        
        for row in reader:
            hpc_code = row.get('HPC_Code', '').strip()
            hpr_name = row.get('HPR_Name', '').strip()
            mobile = row.get('Mobile No', '').strip()

            if hpc_code and (hpr_name or mobile):
                # We use LIKE '%code' to match safely in case hpc_plant_key is stored as '1202-2100' or just '2100'
                cursor = conn.execute("""
                    UPDATE proc_period_snapshot
                    SET hpr_name = ?, mobile_no = ?
                    WHERE hpc_plant_key LIKE ? OR hpc_plant_key = ?
                """, (hpr_name, mobile, f"%{hpc_code}", hpc_code))
                
                updated_count += cursor.rowcount

    conn.commit()
    conn.close()
    logging.info(f"Success! Updated contact info for {updated_count} HPCs.")

if __name__ == "__main__":
    import_hpr_contacts()