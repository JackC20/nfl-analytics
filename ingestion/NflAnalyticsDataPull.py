"""
NFL raw ingestion — AWS Lambda function, also runnable locally.

Pulls datasets via nflreadpy and lands them as raw parquet in S3.
Script performs no transformations, meant to just grab raw data and place them within datalake

Entry points:
    handler(event, context)                        Lambda
    python NflAnalyticsDataPull.py --BUCKET ...    local

Configuration, highest precedence first:
    1. Event payload keys (lowercase): bucket, seasons, raw_prefix, datasets
    2. Environment variables set on the function: BUCKET, SEASONS, RAW_PREFIX, DATASETS
    3. CLI flags, local runs only: --BUCKET, --SEASONS, --RAW_PREFIX, --DATASETS

BUCKET is required, everything else has a default:
    SEASONS      "all" for full history, or a comma list: "2026" / "2023,2024,2025"
                 Default: current season only.
    RAW_PREFIX   Key prefix inside the bucket. Default: raw
    DATASETS     Comma list to limit the run: "pbp,players", or "all" for every
                 dataset. Default: everything except participation, which only
                 updates once a year — ask for it by name or use "all".

Layout written:
    s3://<bucket>/<prefix>/<dataset>/season=YYYY/ingest_date=YYYY-MM-DD/<dataset>_<season>.parquet
    s3://<bucket>/<prefix>/<dataset>/ingest_date=YYYY-MM-DD/<dataset>.parquet   (season-less datasets)

Lambda caps a run at 15 minutes, so SEASONS=all does not fit there.
Run the full backfill locally instead.

The run fails — raising in Lambda, exiting non-zero locally — when a dataset
errors OR when it was pulled and wrote zero rows. The second case matters:
a source going quiet otherwise looks exactly like a healthy run.
"""

import io
import os
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional

import boto3
import nflreadpy as nfl
import polars as pl

# Set by the Lambda runtime, so its absence means we are running locally.
IN_LAMBDA = bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME"))

log = logging.getLogger(__name__)

if IN_LAMBDA:
    # Lambda already configured the root logger, so basicConfig would be a no-op
    # and force=True would drop the request id it prefixes onto every line.
    log.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Module scope so warm Lambda invocations reuse the client instead of rebuilding it.
s3 = boto3.client("s3")


# -----------------------------
# Config                    ---
# -----------------------------

DEFAULT_PREFIX = "raw"

CONFIG_KEYS = ["BUCKET", "SEASONS", "RAW_PREFIX", "DATASETS"]

# earliest season each sourcse has data for
PBP_START = 1999
PARTICIPATION_START = 2016
NGS_START = 2016
PFR_START = 2018

# participation trails by a season: nflreadpy caps it at current_season - 1
PARTICIPATION_LAG = 1

NGS_STAT_TYPES = ["passing", "receiving", "rushing"]
PFR_STAT_TYPES = ["pass", "rush", "rec", "def"]


# -----------------------------
# ARGS                      ---
# -----------------------------

def get_config(event: Optional[dict] = None) -> dict[str, str]:
    """
    Merge configuration from the environment, CLI flags and the event payload.

    Event wins so a one-off invoke can override the scheduled defaults without
    editing the function config. Absent keys are left out entirely so the
    .get(key, default) calls downstream behave as expected.
    """
    config: dict[str, str] = {}

    # lowest precedence: env vars set on the function, the scheduled defaults
    for key in CONFIG_KEYS:
        value = os.environ.get(key)
        if value:
            config[key] = value

    # local runs only: the same names as CLI flags
    if not IN_LAMBDA:
        parser = argparse.ArgumentParser(description="NFL raw ingestion (local run)")
        for key in CONFIG_KEYS:
            parser.add_argument(f"--{key}")
        parsed, _ = parser.parse_known_args()
        config.update({k: v for k, v in vars(parsed).items() if v})

    # highest precedence: this invocation's event
    for key in CONFIG_KEYS:
        value = (event or {}).get(key.lower())
        if value:
            config[key] = str(value)

    if not config.get("BUCKET"):
        raise ValueError(
            "BUCKET is required: event key 'bucket', env var BUCKET, or --BUCKET"
        )

    return config


def resolve_seasons(raw_value: Optional[str], start_year: int, end_offset: int = 0) -> list[int]:
    """
    'all' -> full history for this source, otherwise parse the comma list

    end_offset pulls the newest allowed season back by N years, for sources
    that always trail the current season (participation, which is published
    historically by FTN and is never available for the season in progress).
    """

    current = nfl.get_current_season() - end_offset
    value = (raw_value or "").strip()

    if not value:
        return [current]

    if value.lower() == "all":
        return list(range(start_year, current + 1 ))

    seasons =  [int(s.strip()) for s in value.split(",") if s.strip()]

    if not seasons:
        raise ValueError(f"SEASONS={raw_value} parsed to no usable seasons")

    # drop seasons the source doesn't cover so a full history run does not error

    return [s for s in seasons if start_year <= s <= current]


def resolve_datasets(raw_value: Optional[str]) -> list[str]:
    """Absent -> the daily set. "all" -> every ingestor. Otherwise the comma list."""
    requested = (raw_value or "").strip()

    if not requested:
        return DEFAULT_DATASETS
    if requested.lower() == "all":
        return list(INGESTORS)

    return [d.strip() for d in requested.split(",") if d.strip()]



# -----------------------------
# S3 write                  ---
# -----------------------------

def write_parquet(df: Optional[pl.DataFrame], bucket: str, key: str) -> int:
    """
    Polars dataframe -> parquet bytes -> S3. No pandas needed

    Returns the number of rows written, 0 if there was nothing to write. The
    caller adds these up so a run that writes nothing can be reported as a
    failure instead of a silent success.
    """
    if df is None or df.height == 0:
        log.warning(f"Empty dataframe, skipping {key}")
        return 0

    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    log.info(f"wrote {df.height} rows -> s3://{bucket}/{key}")

    return df.height


def build_key(prefix: str, dataset: str, ingest_date: str, season: Optional[int] = None, suffix: Optional[str] = None) -> str:
    """
    Season before ingest_date so all snapshots of one season sit together,
    and Athena can prune by season when reading the raw files directly.
    """
    parts = [prefix, dataset]

    if season is not None:
        parts.append(f"season={season}")
    parts.append(f"ingest_date={ingest_date}")

    name = dataset

    if suffix:
        name = f"{name}_{suffix}"
    if season is not None:
        name = f"{name}_{season}"

    parts.append(f"{name}.parquet")

    return "/".join(parts)



# -----------------------------
# Per dataset pulls         ---
# Pull one season at a time ---
# -----------------------------
#
# Each returns (attempts, rows): how many writes were tried, and how many rows
# landed. The two are separate so run() can tell apart a source with nothing
# applicable to pull (attempts == 0, fine) from one that was pulled and came
# back empty (attempts > 0, rows == 0, a problem worth alerting on).


def ingest_pbp(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> tuple[int, int]:
    attempts = rows = 0
    for season in resolve_seasons(seasons_arg, PBP_START):
        df = nfl.load_pbp(seasons=[season])
        attempts += 1
        rows += write_parquet(df, bucket, build_key(prefix, "pbp", ingest_date, season))
    return attempts, rows


def ingest_participation(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> tuple[int, int]:
    attempts = rows = 0
    for season in resolve_seasons(seasons_arg, PARTICIPATION_START, end_offset=PARTICIPATION_LAG):
        df = nfl.load_participation(seasons=[season])
        attempts += 1
        rows += write_parquet(df, bucket, build_key(prefix, "participation", ingest_date, season))
    return attempts, rows


def ingest_nextgen_stats(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> tuple[int, int]:
    attempts = rows = 0
    for season in resolve_seasons(seasons_arg, NGS_START):
        for stat_type in NGS_STAT_TYPES:
            df = nfl.load_nextgen_stats(seasons=[season], stat_type=stat_type)
            key = build_key(prefix, "nextgen_stats", ingest_date, season, suffix=stat_type)
            attempts += 1
            rows += write_parquet(df, bucket, key)
    return attempts, rows


def ingest_pfr_advstats(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> tuple[int, int]:
    attempts = rows = 0
    for season in resolve_seasons(seasons_arg, PFR_START):
        for stat_type in PFR_STAT_TYPES:
            df = nfl.load_pfr_advstats(
                seasons=[season],
                stat_type=stat_type,
                summary_level="week",
            )
            key = build_key(prefix, "pfr_advstats", ingest_date, season, suffix=stat_type)
            attempts += 1
            rows += write_parquet(df, bucket, key)
    return attempts, rows


def ingest_players(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> tuple[int, int]:
    # Not season-scoped — full table each run, snapshotted by ingest_date.
    df = nfl.load_players()
    return 1, write_parquet(df, bucket, build_key(prefix, "players", ingest_date))


def ingest_teams(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> tuple[int, int]:
    df = nfl.load_teams()
    return 1, write_parquet(df, bucket, build_key(prefix, "teams", ingest_date))



INGESTORS = {
    "pbp": ingest_pbp,
    "participation": ingest_participation,
    "players": ingest_players,
    "teams": ingest_teams,
    "nextgen_stats": ingest_nextgen_stats,
    "pfr_advstats": ingest_pfr_advstats,
}

# Sources that update at most once a year, so the daily schedule should not
# re-pull them. Request them by name, or with DATASETS=all.
OPT_IN_DATASETS = {"participation"}

# Derived from INGESTORS so a new ingestor joins the default run automatically.
DEFAULT_DATASETS = [name for name in INGESTORS if name not in OPT_IN_DATASETS]



# -----------------------------
# Run                       ---
# -----------------------------

def run(config: dict[str, str]) -> dict:
    """
    Do the ingestion and report what happened. Returns rather than raising so
    the caller decides what a partial failure means — see handler and main.
    """
    seasons_arg = config.get("SEASONS")
    bucket = config["BUCKET"]
    prefix = config.get("RAW_PREFIX", DEFAULT_PREFIX)

    # UTC, not local time, so the partition date is the same whether this runs
    # in Lambda or on a laptop.
    ingest_date = datetime.now(timezone.utc).date().isoformat()

    datasets = resolve_datasets(config.get("DATASETS"))

    log.info(f"run start | bucket={bucket} | seasons={seasons_arg or 'current'} | datasets={datasets} | ingest_date={ingest_date}")

    failures = []     # raised an exception
    empty = []        # pulled, but every write came back with zero rows
    skipped = []      # nothing applicable to pull, e.g. a season this source predates
    completed = []
    written = {}      # dataset -> rows written

    for name in datasets:
        ingestor = INGESTORS.get(name)
        if ingestor is None:
            log.error(f"unknown dataset {name}")
            failures.append(name)
            continue

        try:
            attempts, rows = ingestor(seasons_arg, bucket, prefix, ingest_date)

            if attempts == 0:
                log.warning(f"nothing applicable to pull for {name}")
                skipped.append(name)
            elif rows == 0:
                # Succeeding while writing nothing is the failure mode that
                # otherwise looks identical to a good run. Treat it as an error.
                log.error(f"{name} returned no rows across {attempts} attempts")
                empty.append(name)
            else:
                completed.append(name)
                written[name] = rows

        except Exception as e:
            log.exception(f"failed: {name} {e}")
            failures.append(name)

    log.info(f"run complete | written={written} | skipped={skipped} | empty={empty} | failed={failures}")

    return {
        "bucket": bucket,
        "prefix": prefix,
        "ingest_date": ingest_date,
        "completed": completed,
        "written": written,
        "skipped": skipped,
        "empty": empty,
        "failed": failures,
    }


def problems(summary: dict) -> list[str]:
    """Datasets that should stop the run: errored, or wrote nothing at all."""
    return summary["failed"] + summary["empty"]


def handler(event, context) -> dict:
    """
    Lambda entry point. Raising on failure is deliberate: it marks the
    invocation failed, which is what reaches CloudWatch alarms and any DLQ.
    """
    summary = run(get_config(event or {}))

    bad = problems(summary)
    if bad:
        raise RuntimeError(f"ingestion failed or wrote nothing for: {', '.join(bad)}")

    return summary


def main() -> None:
    summary = run(get_config())

    bad = problems(summary)
    if bad:
        raise SystemExit(f"ingestion failed or wrote nothing for: {', '.join(bad)}")



if __name__ == "__main__":
    main()
