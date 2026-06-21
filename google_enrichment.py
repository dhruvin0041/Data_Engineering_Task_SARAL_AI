import pandas as pd
import logging
import time
import random
import requests
from bs4 import BeautifulSoup
import urllib.parse

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
logger = logging.getLogger(__name__)

def main():
    input_file = "enriched_dataset.csv"
    output_file = "enriched_dataset.csv"
    df = pd.read_csv(input_file)
    mask = df["current_role"].isna() | df["current_company"].isna() | (df["current_company"] == "Unknown")
    candidates = df[mask]
    logger.info(f"Found {len(candidates)} profiles to scrape via Google")
    
    updated_count = 0
    for idx, row in candidates.iterrows():
        url = str(row["linkedin_url"]).strip()
        name = str(row.get("full_name", ""))
        if "linkedin.com" not in url:
            continue
            
        slug = url.rstrip("/").split("/")[-1]
        query = f"site:linkedin.com/in {name} {slug}"
        logger.info(f"Searching: {query}")
        
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            res = requests.get(f"https://www.google.com/search?q={urllib.parse.quote(query)}", headers=headers)
            soup = BeautifulSoup(res.text, "html.parser")
            blocks = soup.find_all("div", class_="g")
            
            role = None
            company = None
            first_name = name.split()[0].lower() if name else ""
            
            for block in blocks:
                text = block.get_text(separator=" | ")
                if first_name in text.lower():
                    if " - " in text:
                        parts = text.split(" - ")
                        for p in parts:
                            if " at " in p:
                                role, company = p.split(" at ", 1)
                                role = role.split("|")[-1].strip()
                                company = company.split("|")[0].strip()
                                break
                    if role: break
            
            if not role:
                for block in blocks:
                    text = block.get_text(separator=" | ")
                    if " | " in text and first_name in text.lower():
                        parts = text.split(" | ")
                        for p in parts:
                            if " at " in p:
                                role, company = p.split(" at ", 1)
                                role = role.strip()
                                company = company.strip()
                                break
                    if role: break

            if role:
                df.at[idx, "current_role"] = role
                df.at[idx, "current_company"] = company
                updated_count += 1
                logger.info(f"Found: {role} at {company}")
            else:
                logger.warning(f"Not found: {url} -> {query}")
                
            time.sleep(random.uniform(1.5, 3.5))
        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(5)
            
    df.to_csv(output_file, index=False)
    logger.info(f"Saved {updated_count} updated rows to {output_file}")

if __name__ == "__main__":
    main()
