import os
import re
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from linkedin_api import Linkedin

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load Environment Variables
load_dotenv()
LINKEDIN_EMAIL = os.getenv("LINKEDIN_EMAIL")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///candidates.db")

engine = create_engine(DATABASE_URL)

def extract_public_id(url):
    """Extracts the public ID from a LinkedIn URL."""
    if not url:
        return None
    match = re.search(r'linkedin\.com/in/([^/]+)', url)
    if match:
        return match.group(1).split('?')[0]
    return None

def calculate_duration(start_date, end_date, is_current):
    """Calculates duration in months and generates duration text."""
    now = datetime.now()
    
    start_year = start_date.get('year')
    start_month = start_date.get('month', 1)
    if not start_year:
        return 0, "0 mos"
    
    end_year = end_date.get('year') if end_date else None
    end_month = end_date.get('month', 1) if end_date else 1
    
    if is_current or not end_year:
        end_year = now.year
        end_month = now.month
        
    duration_months = (end_year - start_year) * 12 + (end_month - start_month)
    if duration_months < 0:
        duration_months = 0
        
    years = duration_months // 12
    months = duration_months % 12
    
    parts = []
    if years > 0:
        parts.append(f"{years} yr{'s' if years > 1 else ''}")
    if months > 0 or years == 0:
        parts.append(f"{months} mo{'s' if months > 1 else ''}")
        
    duration_text = " ".join(parts)
    
    start_str = f"{datetime(start_year, start_month, 1).strftime('%b %Y')}"
    if is_current:
        end_str = "Present"
    else:
        end_str = f"{datetime(end_year, end_month, 1).strftime('%b %Y')}"
        
    full_text = f"{start_str} - {end_str} \u00b7 {duration_text}"
    return duration_months, full_text

def fetch_and_update_profiles():
    if not LINKEDIN_EMAIL or not LINKEDIN_PASSWORD:
        logger.error("Please set LINKEDIN_EMAIL and LINKEDIN_PASSWORD in your .env file.")
        return

    logger.info("Authenticating with LinkedIn...")
    try:
        api = Linkedin(LINKEDIN_EMAIL, LINKEDIN_PASSWORD)
    except Exception as e:
        logger.error(f"Failed to authenticate: {e}")
        return

    failures = {}

    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, linkedin_url FROM candidates"))
        candidates = result.fetchall()
        
        logger.info(f"Found {len(candidates)} candidates in the database.")
        
        for index, candidate in enumerate(candidates):
            cand_id = candidate[0]
            url = candidate[1]
            public_id = extract_public_id(url)
            
            if not public_id:
                failures[cand_id] = "Invalid LinkedIn URL"
                continue
                
            logger.info(f"[{index+1}/{len(candidates)}] Scraping {public_id}...")
            
            try:
                profile = api.get_profile(public_id)
                if not profile:
                    failures[cand_id] = "Profile not found or inaccessible."
                    continue
                
                exp_data = profile.get('experience', [])
                parsed_experiences = []
                
                # Transform experience data
                for exp in exp_data:
                    company_name = exp.get('companyName')
                    title = exp.get('title')
                    
                    if not company_name or not title:
                        continue
                        
                    start = exp.get('timePeriod', {}).get('startDate', {})
                    end = exp.get('timePeriod', {}).get('endDate')
                    is_current = end is None
                    
                    duration_months, duration_text = calculate_duration(start, end, is_current)
                    
                    parsed_experiences.append({
                        "role": title,
                        "company": company_name,
                        "start_date": f"{start.get('year')}-{start.get('month', 1):02d}-01" if start.get('year') else None,
                        "end_date": f"{end.get('year')}-{end.get('month', 1):02d}-01" if end else None,
                        "is_current": is_current,
                        "duration_months": duration_months,
                        "duration_text": duration_text
                    })
                
                # Update database
                conn.execute(
                    text("UPDATE candidates SET experience = :exp WHERE id = :id"),
                    {"exp": json.dumps(parsed_experiences), "id": cand_id}
                )
                conn.commit()
                logger.info(f"Successfully updated {public_id} with {len(parsed_experiences)} roles.")
                
            except Exception as e:
                logger.error(f"Failed to scrape {public_id}: {e}")
                failures[cand_id] = str(e)

    # Log failures
    with open('failures_log.json', 'w') as f:
        json.dump(failures, f, indent=2)
    
    logger.info(f"Processing complete. Success: {len(candidates) - len(failures)}, Failures: {len(failures)}")
    if len(failures) > 50:
        logger.warning(f"Failure rate is above 5%. Please review failures_log.json.")

if __name__ == "__main__":
    fetch_and_update_profiles()
