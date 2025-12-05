# init_db.py
# Initialize DuckDB database from CSV files

import duckdb
import pandas as pd
import os
from pathlib import Path


def init_database(db_path: str = "iphone_gold.duckdb", force_recreate: bool = False):
    """
    Initialize DuckDB database from CSV files.
    
    Args:
        db_path: Path to DuckDB database file
        force_recreate: If True, recreate database even if it exists
    
    Returns:
        bool: True if database was created/updated, False if already exists
    """
    
    # Check if database already exists and is valid
    if os.path.exists(db_path) and not force_recreate:
        try:
            con = duckdb.connect(db_path, read_only=True)
            # Test if we can query the database
            con.execute("SELECT COUNT(*) FROM dim_product").fetchone()
            con.close()
            print(f"✅ Database already exists and is valid: {db_path}")
            return False
        except Exception as e:
            print(f"⚠️ Existing database is corrupted: {e}")
            print("🔄 Recreating database...")
            if os.path.exists(db_path):
                os.remove(db_path)
    
    # CSV files mapping
    csv_files = {
        "fact_contract": "fact_contract.csv",
        "fact_registration": "fact_registration.csv",
        "fact_inventory_snapshot": "fact_inventory_snapshot.csv",
        "dim_date": "dim_date.csv",
        "dim_product": "dim_product.csv",
        "dim_branch": "dim_branch.csv",
    }
    
    # Check if all CSV files exist
    missing_files = []
    for table_name, csv_file in csv_files.items():
        if not os.path.exists(csv_file):
            missing_files.append(csv_file)
    
    if missing_files:
        raise FileNotFoundError(
            f"Missing CSV files: {', '.join(missing_files)}\n"
            f"Please ensure all CSV files are in the same directory as this script."
        )
    
    print(f"🔧 Creating DuckDB database: {db_path}")
    
    # Create new database
    con = duckdb.connect(db_path)
    
    try:
        # Load each CSV and create table
        for table_name, csv_file in csv_files.items():
            print(f"  📥 Loading {csv_file} -> {table_name}...")
            df = pd.read_csv(csv_file)
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"    ✓ Loaded {row_count} rows")
        
        # Verify all tables
        print("\n✅ Database created successfully!")
        print("📊 Table summary:")
        tables = con.execute("SHOW TABLES").fetchall()
        for table in tables:
            table_name = table[0]
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  - {table_name}: {count} rows")
        
        con.close()
        return True
        
    except Exception as e:
        con.close()
        # Clean up failed database file
        if os.path.exists(db_path):
            os.remove(db_path)
        raise Exception(f"Failed to create database: {e}")


if __name__ == "__main__":
    # Run standalone
    init_database(force_recreate=False)
```

4. **กด "Commit changes"**

---

### ขั้นที่ 3: สร้างไฟล์ `.gitignore`

**`.gitignore` คืออะไร?**
- เป็นไฟล์ที่บอก Git ว่า**ไฟล์ไหนไม่ต้อง upload** ขึ้น GitHub
- ใช้กับไฟล์ที่ไม่จำเป็น เช่น `.duckdb` (จะสร้างใหม่ทุกครั้งอยู่แล้ว)

1. **ใน GitHub repository** กดปุ่ม **"Add file" -> "Create new file"**
2. **ตั้งชื่อไฟล์:** `.gitignore` (ต้องมีจุดข้างหน้า)
3. **Copy code นี้ใส่:**
```
# Python
__pycache__/
*.py[cod]
*.pyc

# DuckDB (will be created from CSVs)
*.duckdb
*.duckdb.wal

# Streamlit secrets
.streamlit/secrets.toml

# Environment
.env

# OS files
.DS_Store
