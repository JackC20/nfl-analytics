"""
NFL raw ingestion — AWS Glue Python Shell job.
 
Pulls datasets via nflreadpy and lands them as raw parquet in S3.
Script performs no transformations, meant to just grab raw data and place them within datalake
 
Job parameters (BUCKET and JOB_NAME required, rest optional):
    --SEASONS      "all" for full history, or a comma list: "2026" / "2023,2024,2025"
                   Default: current season only.
    --BUCKET       Target bucket, default exists in glue job advanced settings
    --RAW_PREFIX   Key prefix inside the bucket. Default: raw
    --DATASETS     Comma list to limit the run: "pbp,players". Default: all.
 
Layout written:
    s3://<bucket>/<prefix>/<dataset>/season=YYYY/ingest_date=YYYY-MM-DD/<dataset>_<season>.parquet
    s3://<bucket>/<prefix>/<dataset>/ingest_date=YYYY-MM-DD/<dataset>.parquet   (season-less datasets)
"""

import io
import sys
import logging
from datetime import date
from typing import Optional

import boto3
import nflreadpy as nfl
import polars as pl
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

s3 = boto3.client("s3")


# -----------------------------
# Config                    ---
# -----------------------------

DEFAULT_PREFIX = "raw"

# earliest season each sourcse has data for
PBP_START = 1999
PARTICIPATION_START = 2016
NGS_START = 2016
PFR_START = 2018

NGS_STAT_TYPES = ["passing", "receiving", "rushing"]
PFR_STAT_TYPES = ["pass", "rush", "rec", "def"]


# -----------------------------
# ARGS                      ---
# -----------------------------

def get_args() -> dict[str, str]:
    """
    BUCKET is required and set on the Glue job, so it is always present
    The rest are optional and getResolvedOptions errors on absent args
    so check sys.args before asing for them
    """
    optional = ["SEASONS", "RAW_PREFIX", "DATASETS"]
    present = [name for name in optional if f"--{name}" in sys.argv]
    return getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"] + present)
    
    
def resolve_seasons(raw_value: Optional[str], start_year: int) -> list[int]:
    """ 'all' -> full history for this source, otherwise parse the comma list"""
    
    current = nfl.get_current_season()
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
    
    
    
# -----------------------------
# S3 write                  ---
# -----------------------------

def write_parquet(df: Optional[pl.DataFrame], bucket: str, key: str) -> None:
    """Polars dataframe -> parquet bytes -> S3. No pandas needed"""
    if df is None or df.height == 0:
        log.warning(f"Empty dataframe, skipping {key}")
        return
    
    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    log.info(f"wrote {df.height} rows -> s3://{bucket}/{key}")
    
    
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


def ingest_pbp(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> None:
    for season in resolve_seasons(seasons_arg, PBP_START):
        df = nfl.load_pbp(seasons=[season])
        write_parquet(df, bucket, build_key(prefix, "pbp", ingest_date, season))


def ingest_participation(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> None:
    for season in resolve_seasons(seasons_arg, PARTICIPATION_START):
        df = nfl.load_participation(seasons=[season])
        write_parquet(df, bucket, build_key(prefix, "participation", ingest_date, season))


def ingest_nextgen_stats(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> None:
    for season in resolve_seasons(seasons_arg, NGS_START):
        for stat_type in NGS_STAT_TYPES:
            df = nfl.load_nextgen_stats(seasons=[season], stat_type=stat_type)
            key = build_key(prefix, "nextgen_stats", ingest_date, season, suffix=stat_type)
            write_parquet(df, bucket, key)


def ingest_pfr_advstats(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> None:
    for season in resolve_seasons(seasons_arg, PFR_START):
        for stat_type in PFR_STAT_TYPES:
            df = nfl.load_pfr_advstats(
                seasons=[season],
                stat_type=stat_type,
                summary_level="week",
            )
            key = build_key(prefix, "pfr_advstats", ingest_date, season, suffix=stat_type)
            write_parquet(df, bucket, key)


def ingest_players(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> None:
    # Not season-scoped — full table each run, snapshotted by ingest_date.
    df = nfl.load_players()
    write_parquet(df, bucket, build_key(prefix, "players", ingest_date))


def ingest_teams(seasons_arg: Optional[str], bucket: str, prefix: str, ingest_date: str) -> None:
    df = nfl.load_teams()
    write_parquet(df, bucket, build_key(prefix, "teams", ingest_date))



INGESTORS = {
    "pbp": ingest_pbp,
    "participation": ingest_participation,
    "players": ingest_players,
    "teams": ingest_teams,
    "nextgen_stats": ingest_nextgen_stats,
    "pfr_advstats": ingest_pfr_advstats,
}

def main():
    args = get_args()
    
    seasons_arg = args.get("SEASONS")
    bucket = args["BUCKET"]
    prefix = args.get("RAW_PREFIX", DEFAULT_PREFIX)
    ingest_date = date.today().isoformat()
    
    requested = args.get("DATASETS")
    datasets = (
        [d.strip() for d in requested.split(",") if d.strip()]
        if requested
        else list(INGESTORS)
        )
        
        
    log.info(f"run start | bucket={bucket} | seasons={seasons_arg or 'current'} | datasets={datasets} | ingest_date={ingest_date}")
    
    failures = []
    for name in datasets:
        ingestor = INGESTORS.get(name)
        if ingestor is None:
            log.error(f"unknown dataset {name}")
            failures.append(name)
            continue
        
        try:
            ingestor(seasons_arg, bucket, prefix, ingest_date)
            
        except Exception as e:
            log.exception(f"failed: {name} {e}")
            failures.append(name)
            
    if failures:
        raise RuntimeError(f"ingestion failed for: {', '.join(failures)}")
    
    
    log.info("run complete")
    
    
    
if __name__ == "__main__":
    main()