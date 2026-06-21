"""Quick smoke test of the pipeline's non-scraping functions."""
import sys
sys.path.insert(0, ".")

from linkedin_enrichment_pipeline import (
    load_dataset,
    identify_candidates_to_scrape,
    is_valid_linkedin_url,
    ScrapeResult,
    PipelineConfig,
    load_previous_log,
    save_scrape_log,
    merge_and_save,
)

# Test config
config = PipelineConfig()
print(f"Checkpoint interval: {config.checkpoint_interval}")
print(f"Max retries: {config.max_retries}")

# Test URL validation
assert is_valid_linkedin_url("https://www.linkedin.com/in/test-user/") is True
assert is_valid_linkedin_url("https://linkedin.com/in/test-user") is True
assert is_valid_linkedin_url("not-a-url") is False
assert is_valid_linkedin_url("") is False
assert is_valid_linkedin_url(None) is False
print("URL validation: PASS")

# Test dataset loading
df = load_dataset("missing_duration_candidates.csv")
print(f"Dataset rows: {len(df)}")

# Test candidate identification
candidates = identify_candidates_to_scrape(df)
print(f"Candidates needing enrichment: {len(candidates)}")

# Test resume support (no previous log)
prev = load_previous_log("nonexistent_log.csv")
assert len(prev) == 0
print("Resume support (empty): PASS")

# Test ScrapeResult
result = ScrapeResult(
    candidate_id="test-id",
    linkedin_url="https://linkedin.com/in/test",
    status="success",
    scraped_role="Engineer",
    scraped_company="TestCo",
)
assert result.status == "success"
print("ScrapeResult creation: PASS")

# Test merge (dry run)
results = {
    candidates.iloc[0]["id"]: ScrapeResult(
        candidate_id=candidates.iloc[0]["id"],
        linkedin_url=candidates.iloc[0]["linkedin_url"],
        status="success",
        scraped_role="Test Role",
        scraped_company="Test Company",
    )
}
# Don't actually save, just test the merge logic
import tempfile, os
with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
    tmp_path = f.name
try:
    enriched = merge_and_save(df, results, tmp_path)
    # Check the merged row
    cid = candidates.iloc[0]["id"]
    row = enriched[enriched["id"] == cid].iloc[0]
    assert row["current_role"] == "Test Role" or row["current_company"] == "Test Company"
    print("Merge logic: PASS")
finally:
    os.unlink(tmp_path)

print()
print("=" * 50)
print("ALL SMOKE TESTS PASSED")
print("=" * 50)
