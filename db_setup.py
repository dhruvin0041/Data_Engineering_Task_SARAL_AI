import os
import csv
import json
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///candidates.db")
CSV_FILE = "missing_duration_candidates.csv"

def setup_db():
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS candidates (
                id TEXT PRIMARY KEY,
                full_name TEXT,
                linkedin_url TEXT,
                current_role TEXT,
                current_company TEXT,
                issue TEXT,
                created_at TEXT,
                experience TEXT
            )
        """))
        
        # Check if table is empty
        result = conn.execute(text("SELECT COUNT(*) FROM candidates")).scalar()
        if result == 0:
            print("Populating initial database from CSV...")
            with open(CSV_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    conn.execute(
                        text("""
                            INSERT INTO candidates (id, full_name, linkedin_url, current_role, current_company, issue, created_at, experience)
                            VALUES (:id, :full_name, :linkedin_url, :current_role, :current_company, :issue, :created_at, :experience)
                        """),
                        {
                            "id": row["id"],
                            "full_name": row["full_name"],
                            "linkedin_url": row["linkedin_url"],
                            "current_role": row["current_role"],
                            "current_company": row["current_company"],
                            "issue": row["issue"],
                            "created_at": row["created_at"],
                            "experience": None
                        }
                    )
            print("Database setup complete.")
        else:
            print(f"Database already contains {result} rows.")

if __name__ == "__main__":
    setup_db()
