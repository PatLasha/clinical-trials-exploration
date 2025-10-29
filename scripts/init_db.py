import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.db_connection import DBConnection

def init_db():
    db = DBConnection()
    
    print("Initializing database schema...")
    if not db.test_connection():
        print("Cannot initialize database schema: Database connection failed.")
        return
    
    print("Executing schema.sql...")

    sql_file_paths = [
        "db/schemas/create_schemas.sql",
        "db/schemas/create_staging_tables.sql",
        "db/schemas/create_staging_tables.sql",
    ]

    for sql_file in sql_file_paths:
        if os.path.exists(sql_file):
            print(f"Executing {sql_file}...")
            db.execute_sql_file(sql_file)
            print(f"Executed {sql_file} successfully.")
        else:
            print(f"SQL file {sql_file} does not exist. Skipping.")

    print("Database schema initialization completed.")

if __name__ == "__main__":
    init_db()