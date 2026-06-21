from fastapi import FastAPI, HTTPException
import os
import json
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///candidates.db")

app = FastAPI()
engine = create_engine(DATABASE_URL)

@app.get("/api/saral/profile/{profile_id}")
def get_profile(profile_id: str):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT * FROM candidates WHERE id = :id"),
            {"id": profile_id}
        ).fetchone()
        
    if not result:
        raise HTTPException(status_code=404, detail="Profile not found")
        
    row = dict(result._mapping)
    raw_exp = json.loads(row["experience"]) if row["experience"] else []
    
    # Group by company to match engine API shape
    grouped = {}
    for pos in raw_exp:
        comp_name = pos.get("company", "Unknown")
        if comp_name not in grouped:
            grouped[comp_name] = []
        
        pos_formatted = {
            "role": pos.get("role"),
            "start_date": pos.get("start_date"),
            "end_date": pos.get("end_date"),
            "is_current": pos.get("is_current"),
            "duration_months": pos.get("duration_months"),
            "duration_text": pos.get("duration_text")
        }
        grouped[comp_name].append(pos_formatted)
        
    formatted_exp = []
    for comp, positions in grouped.items():
        formatted_exp.append({
            "company": {"company_name": comp},
            "positions": positions
        })
    
    profile_data = {
        "id": row["id"],
        "name": row["full_name"],
        "linkedin_url": row["linkedin_url"],
        "current_role": row["current_role"],
        "current_company": row["current_company"],
        "created_at": row["created_at"],
        "experience": formatted_exp
    }
    
    return {
        "status": "success",
        "profile": profile_data
    }

# Run with: uvicorn app:app --reload

