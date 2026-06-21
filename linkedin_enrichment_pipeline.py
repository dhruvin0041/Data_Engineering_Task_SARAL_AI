#!/usr/bin/env python3
"""
LinkedIn Profile Enrichment Pipeline
=====================================
Scrapes LinkedIn profiles to fill missing current_role and current_company
fields using the linkedin_scraper library (v3.x, Playwright-based).

Features:
    - Async Playwright-based scraping via linkedin_scraper
    - Exponential backoff with jitter on retries
    - Checkpoint saves every N profiles
    - Resume from previous runs via scrape_log.csv
    - Random delays between requests to reduce detection
    - Comprehensive error handling and logging
    - Outputs: enriched_dataset.csv, scrape_log.csv, failed_profiles.csv

Usage:
    1. First run:  python create_session.py      (creates session.json)
    2. Then run:   python linkedin_enrichment_pipeline.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for the enrichment pipeline."""

    # Paths
    input_csv: str = "missing_duration_candidates.csv"
    output_csv: str = "enriched_dataset.csv"
    scrape_log_csv: str = "scrape_log.csv"
    failed_csv: str = "failed_profiles.csv"
    session_file: str = "session.json"

    # Scraping behaviour
    checkpoint_interval: int = 25          # save progress every N profiles
    max_retries: int = 3                   # per-profile retry attempts
    base_delay: float = 5.0               # base seconds for exponential backoff
    min_request_delay: float = 25.0       # minimum seconds between requests
    max_request_delay: float = 45.0       # maximum seconds between requests
    rate_limit_cooldown: float = 300.0    # seconds to wait on rate-limit error
    page_load_timeout: float = 30.0       # seconds to wait for page load
    headless: bool = True                 # run browser in headless mode

    # Proxy settings
    proxy_server: Optional[str] = None    # e.g., "http://user:pass@pr.brightdata.com:22225"


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_file: str = "enrichment_pipeline.log") -> logging.Logger:
    """Configure structured logging to both console and file."""
    logger = logging.getLogger("enrichment_pipeline")
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers on re-import
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler (INFO+)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler (DEBUG+)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


logger = setup_logging()

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ScrapeResult:
    """Result of scraping a single LinkedIn profile."""

    candidate_id: str
    linkedin_url: str
    status: str = "pending"                      # success | failed | skipped
    scraped_role: Optional[str] = None
    scraped_company: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


# ---------------------------------------------------------------------------
# URL validation
# ---------------------------------------------------------------------------

_LINKEDIN_URL_RE = re.compile(
    r"^https?://(?:www\.)?linkedin\.com/in/[\w\-%.]+/?",
    re.IGNORECASE,
)


def is_valid_linkedin_url(url: Optional[str]) -> bool:
    """Return True if *url* looks like a valid LinkedIn profile URL."""
    if not url or not isinstance(url, str):
        return False
    return bool(_LINKEDIN_URL_RE.match(url.strip()))


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def load_dataset(path: str) -> pd.DataFrame:
    """Load the candidate CSV into a DataFrame with consistent NaN handling."""
    logger.info("Loading dataset from %s", path)
    df = pd.read_csv(path, dtype=str)

    # Normalise empties
    df.replace({"": pd.NA, " ": pd.NA}, inplace=True)

    logger.info(
        "Loaded %d rows  |  %d missing current_role  |  %d missing current_company",
        len(df),
        df["current_role"].isna().sum(),
        df["current_company"].isna().sum(),
    )
    return df


def identify_candidates_to_scrape(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows where current_role or current_company is missing."""
    mask = df["current_role"].isna() | df["current_company"].isna()
    candidates = df.loc[mask].copy()
    logger.info("Identified %d candidates needing enrichment", len(candidates))
    return candidates


def load_previous_log(path: str) -> dict[str, ScrapeResult]:
    """Load scrape_log.csv from a previous run for resume support."""
    results: dict[str, ScrapeResult] = {}
    if not Path(path).exists():
        return results

    log_df = pd.read_csv(path, dtype=str)
    for _, row in log_df.iterrows():
        cid = str(row["candidate_id"])
        results[cid] = ScrapeResult(
            candidate_id=cid,
            linkedin_url=str(row.get("linkedin_url", "")),
            status=str(row.get("status", "pending")),
            scraped_role=row.get("scraped_role") if pd.notna(row.get("scraped_role")) else None,
            scraped_company=row.get("scraped_company") if pd.notna(row.get("scraped_company")) else None,
            error_message=row.get("error_message") if pd.notna(row.get("error_message")) else None,
            retry_count=int(row.get("retry_count", 0)),
            timestamp=str(row.get("timestamp", "")),
        )

    successful = sum(1 for r in results.values() if r.status == "success")
    logger.info(
        "Loaded %d previous results (%d successful) from %s",
        len(results), successful, path,
    )
    return results


def save_scrape_log(results: dict[str, ScrapeResult], path: str) -> None:
    """Persist scrape results to CSV."""
    rows = [
        {
            "candidate_id": r.candidate_id,
            "linkedin_url": r.linkedin_url,
            "status": r.status,
            "scraped_role": r.scraped_role,
            "scraped_company": r.scraped_company,
            "error_message": r.error_message,
            "retry_count": r.retry_count,
            "timestamp": r.timestamp,
        }
        for r in results.values()
    ]
    pd.DataFrame(rows).to_csv(path, index=False)
    logger.debug("Saved scrape log (%d entries) to %s", len(rows), path)


def save_failed_profiles(results: dict[str, ScrapeResult], path: str) -> None:
    """Write failed profiles to a separate CSV for easy review."""
    failed = [
        {
            "candidate_id": r.candidate_id,
            "linkedin_url": r.linkedin_url,
            "error_message": r.error_message,
            "retry_count": r.retry_count,
            "timestamp": r.timestamp,
        }
        for r in results.values()
        if r.status == "failed"
    ]
    pd.DataFrame(failed).to_csv(path, index=False)
    logger.info("Saved %d failed profiles to %s", len(failed), path)


def merge_and_save(
    df: pd.DataFrame,
    results: dict[str, ScrapeResult],
    output_path: str,
) -> pd.DataFrame:
    """Merge scraped data back into the original dataset.

    Only overwrites fields that are currently null/NA.
    """
    enriched = df.copy()

    filled_role = 0
    filled_company = 0

    for cid, result in results.items():
        if result.status != "success":
            continue

        mask = enriched["id"] == cid

        if not mask.any():
            continue

        idx = enriched.loc[mask].index[0]

        # Only fill if currently missing
        if pd.isna(enriched.at[idx, "current_role"]) and result.scraped_role:
            enriched.at[idx, "current_role"] = result.scraped_role
            filled_role += 1

        if pd.isna(enriched.at[idx, "current_company"]) and result.scraped_company:
            enriched.at[idx, "current_company"] = result.scraped_company
            filled_company += 1

    enriched.to_csv(output_path, index=False)
    logger.info(
        "Saved enriched dataset to %s  |  Filled %d roles, %d companies",
        output_path, filled_role, filled_company,
    )
    return enriched


# ---------------------------------------------------------------------------
# Scraping logic
# ---------------------------------------------------------------------------

async def scrape_single_profile(
    scraper: "PersonScraper",  # noqa: F821 – imported at runtime
    url: str,
    config: PipelineConfig,
) -> tuple[Optional[str], Optional[str]]:
    """Scrape a single LinkedIn profile and return (title, company).

    Raises on any unrecoverable error so the caller can log it.
    """
    person = await scraper.scrape(url)

    title: Optional[str] = None
    company: Optional[str] = None

    # The Person model has `experiences: List[Experience]`.
    # Each Experience has `title` (str|None) and `company` (str|None).
    # We want the *most recent* (first) experience that looks current.
    if person and hasattr(person, "experiences") and person.experiences:
        for exp in person.experiences:
            exp_title = getattr(exp, "title", None)
            exp_company = getattr(exp, "company", None)
            if exp_title or exp_company:
                title = exp_title
                company = exp_company
                break  # first (most recent) experience

    # Fallback: use headline if no experience found
    if not title and person and hasattr(person, "headline") and person.headline:
        title = person.headline

    return title, company


async def scrape_with_retries(
    scraper: "PersonScraper",
    candidate_id: str,
    url: str,
    config: PipelineConfig,
) -> ScrapeResult:
    """Attempt to scrape a profile with exponential backoff retries."""
    # Lazy imports so module loads even without linkedin_scraper installed
    from linkedin_scraper import (  # type: ignore[import-untyped]
        AuthenticationError,
        ProfileNotFoundError,
        RateLimitError,
    )

    result = ScrapeResult(candidate_id=candidate_id, linkedin_url=url)

    for attempt in range(1, config.max_retries + 1):
        result.retry_count = attempt
        try:
            title, company = await scrape_single_profile(scraper, url, config)

            if title or company:
                result.status = "success"
                result.scraped_role = title
                result.scraped_company = company
                logger.info(
                    "  [+] Successfully scraped: role=%s  company=%s",
                    title or "(none)", company or "(none)",
                )
            else:
                result.status = "success"
                result.scraped_role = None
                result.scraped_company = None
                logger.warning("  [!] Profile scraped but no experience data found")

            result.timestamp = datetime.now(timezone.utc).isoformat()
            return result

        except ProfileNotFoundError:
            result.status = "failed"
            result.error_message = "Profile not found or private"
            logger.warning("  [X] Profile not found / private: %s", url)
            return result  # no point retrying

        except AuthenticationError as exc:
            result.status = "failed"
            result.error_message = f"Authentication error: {exc}"
            logger.error("  [X] Authentication error — session may be expired")
            return result  # can't fix mid-run

        except RateLimitError:
            wait = config.rate_limit_cooldown + random.uniform(0, 30)
            result.error_message = f"Rate limited (attempt {attempt})"
            logger.warning(
                "  [!] Rate limited — cooling down %.0fs (attempt %d/%d)",
                wait, attempt, config.max_retries,
            )
            await asyncio.sleep(wait)

        except (TimeoutError, asyncio.TimeoutError):
            wait = config.base_delay * (2 ** (attempt - 1)) + random.uniform(0, 2)
            result.error_message = f"Timeout (attempt {attempt})"
            logger.warning(
                "  [!] Timeout — retrying in %.1fs (attempt %d/%d)",
                wait, attempt, config.max_retries,
            )
            await asyncio.sleep(wait)

        except Exception as exc:  # noqa: BLE001
            wait = config.base_delay * (2 ** (attempt - 1)) + random.uniform(0, 2)
            result.error_message = f"{type(exc).__name__}: {exc}"
            logger.warning(
                "  [!] %s — retrying in %.1fs (attempt %d/%d)",
                result.error_message, wait, attempt, config.max_retries,
            )
            await asyncio.sleep(wait)

    # All retries exhausted
    result.status = "failed"
    result.timestamp = datetime.now(timezone.utc).isoformat()
    logger.error("  [X] All %d retries exhausted for %s", config.max_retries, url)
    return result


async def run_scraping_pipeline(
    candidates: pd.DataFrame,
    previous_results: dict[str, ScrapeResult],
    config: PipelineConfig,
) -> dict[str, ScrapeResult]:
    """Iterate over candidates, scrape profiles, checkpoint periodically."""
    from linkedin_scraper import BrowserManager, PersonScraper  # type: ignore[import-untyped]

    results = dict(previous_results)  # start with previous results
    processed_this_run = 0
    total = len(candidates)

    session_path = Path(config.session_file)
    if not session_path.exists():
        logger.error(
            "Session file '%s' not found. Run create_session.py first.", config.session_file
        )
        sys.exit(1)

    logger.info("Starting browser (headless=%s) …", config.headless)

    browser_kwargs = {"headless": config.headless}
    if config.proxy_server:
        browser_kwargs["proxy"] = {"server": config.proxy_server}
        logger.info("Using proxy server configuration")

    async with BrowserManager(**browser_kwargs) as browser:
        await browser.load_session(config.session_file)
        logger.info("Session loaded from %s", config.session_file)

        scraper = PersonScraper(browser.page)

        for idx, (_, row) in enumerate(candidates.iterrows(), start=1):
            cand_id: str = str(row["id"])
            url: str = str(row.get("linkedin_url", ""))
            name: str = str(row.get("full_name", "Unknown"))

            # ── Skip if already successfully scraped in a previous run ──
            if cand_id in results and results[cand_id].status == "success":
                logger.debug("Skipping %s (already scraped)", cand_id)
                continue

            logger.info(
                "[%d/%d] Scraping %s  (%s)", idx, total, name, url,
            )

            # ── Validate URL ──
            if not is_valid_linkedin_url(url):
                result = ScrapeResult(
                    candidate_id=cand_id,
                    linkedin_url=url,
                    status="failed",
                    error_message="Invalid LinkedIn URL",
                )
                results[cand_id] = result
                logger.warning("  [X] Invalid URL: %s", url)
                processed_this_run += 1
                continue

            # ── Scrape with retries ──
            result = await scrape_with_retries(scraper, cand_id, url, config)
            results[cand_id] = result
            processed_this_run += 1

            # ── Checkpoint save ──
            if processed_this_run % config.checkpoint_interval == 0:
                logger.info(
                    " [SAVE] Checkpoint — saving progress (%d processed this run)", processed_this_run,
                )
                save_scrape_log(results, config.scrape_log_csv)

            # ── Random delay between requests ──
            if idx < total:
                delay = random.uniform(config.min_request_delay, config.max_request_delay)
                logger.debug("Sleeping %.1fs before next request", delay)
                await asyncio.sleep(delay)

    # Final save
    save_scrape_log(results, config.scrape_log_csv)
    logger.info("Scraping complete — %d processed this run", processed_this_run)
    return results


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def print_summary(
    df_original: pd.DataFrame,
    df_enriched: pd.DataFrame,
    results: dict[str, ScrapeResult],
) -> None:
    """Display a summary of the enrichment run."""
    total_processed = len(results)
    successful = sum(1 for r in results.values() if r.status == "success")
    failed = sum(1 for r in results.values() if r.status == "failed")
    skipped = sum(1 for r in results.values() if r.status == "skipped")

    remaining_role = df_enriched["current_role"].isna().sum()
    remaining_company = df_enriched["current_company"].isna().sum()

    original_missing_role = df_original["current_role"].isna().sum()
    original_missing_company = df_original["current_company"].isna().sum()

    divider = "=" * 60
    logger.info(divider)
    logger.info("ENRICHMENT PIPELINE — SUMMARY")
    logger.info(divider)
    logger.info("Total records in dataset:        %d", len(df_original))
    logger.info("Profiles processed (all runs):   %d", total_processed)
    logger.info("  [+] Successfully enriched:       %d", successful)
    logger.info("  [-] Failed:                      %d", failed)
    logger.info("  [>] Skipped:                     %d", skipped)
    logger.info(divider)
    logger.info("Missing current_role:    %d → %d", original_missing_role, remaining_role)
    logger.info("Missing current_company: %d → %d", original_missing_company, remaining_company)
    logger.info(divider)
    logger.info("Output files:")
    logger.info("  • enriched_dataset.csv")
    logger.info("  • scrape_log.csv")
    logger.info("  • failed_profiles.csv")
    logger.info(divider)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

async def async_main(config: Optional[PipelineConfig] = None) -> None:
    """Async entry point for the enrichment pipeline."""
    if config is None:
        config = PipelineConfig()

    load_dotenv()

    start_time = time.monotonic()

    # 1. Load dataset
    df = load_dataset(config.input_csv)

    # 2. Identify candidates needing enrichment
    candidates = identify_candidates_to_scrape(df)

    if candidates.empty:
        logger.info("No candidates need enrichment — nothing to do.")
        return

    # 3. Load previous run results (resume support)
    previous_results = load_previous_log(config.scrape_log_csv)

    # 4. Run scraping pipeline
    results = await run_scraping_pipeline(candidates, previous_results, config)

    # 5. Merge and save enriched dataset
    df_enriched = merge_and_save(df, results, config.output_csv)

    # 6. Save failed profiles
    save_failed_profiles(results, config.failed_csv)

    # 7. Summary
    elapsed = time.monotonic() - start_time
    print_summary(df, df_enriched, results)
    logger.info("Total elapsed time: %.1f seconds (%.1f minutes)", elapsed, elapsed / 60)


def main() -> None:
    """Synchronous wrapper for the async pipeline."""
    config = PipelineConfig()
    asyncio.run(async_main(config))


if __name__ == "__main__":
    main()
