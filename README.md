# Data Engineering Task - SARAL AI

This repository contains the solution for the SARAL AI Data Engineering Assignment. The core goals achieved in this project include scraping LinkedIn profiles to enrich experience data, parsing standard duration fields, updating a local database, and exposing the data through a FastAPI endpoint perfectly mimicking the shape of the reference engine API.

## Project Structure

- `app.py`: The FastAPI web server that provides the API endpoint to fetch enriched candidate profiles.
- `candidates.db`: The local SQLite database populated with all the extracted candidate profiles, including the enriched `experience` records.
- `scraper.py` / `linkedin_enrichment_pipeline.py`: The original extraction pipelines configured for fallback scraping via proxies.
- `requirements.txt`: Project Python dependencies.

## Key Accomplishments

### 1. Data Enrichment & Duration Parsing
* Implemented the logic to correctly parse each job position`s duration.
* Formatted data back into the `experience` JSON structure.
* Computed fields like `duration_months` (integer) and `duration_text` (e.g., `1 yr 3 mos`).
* Handled proxy configurations (`BrightData`) for reliable scaling around request blocks.

### 2. FastAPI Endpoint
Built a GET endpoint that mimics the exact data structure served by the reference engine.
* **Endpoint:** `GET /api/saral/profile/{id}`
* The returned JSON response wraps the data under `{"status": "success", "profile": { ... }}`.
* The local API pulls from the populated `candidates.db` and correctly nests the experience items by `company` with their respective `positions` array, identical to the engine.

## Setup and Running the Application

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start the Local API Server
Run the FastAPI app using Uvicorn:
```bash
uvicorn app:app --reload
```
The server will start at `http://127.0.0.1:8000`.

### 3. Verification
To verify the endpoint, test it in the browser or via cURL with a valid candidate ID. Example:
```bash
curl http://127.0.0.1:8000/api/saral/profile/00a3d038-f111-442b-bf11-132c9836e758
```
This output perfectly mirrors the structure of:
`https://engine.saralhire.ai/api/saral/profile/00a3d038-f111-442b-bf11-132c9836e758`

