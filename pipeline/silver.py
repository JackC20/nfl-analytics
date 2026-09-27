"""
Silver Layer: Lakeflow declarative pipeline.

Takes bronze streaming tables and makes them trustworthy:

  1. Deduplicate. The raw layer is append-only, so the same play appears once
     per ingest_date snapshot. Silver keeps the newest snapshot per grain.
  2. Enforce quality. Rows failing an expectation are dropped from the fact
     table and land in a matching quarantine table.
  3. Conform. Dimensions are split out so facts can join through shared keys.

All source columns are carried forward. Only the join keys and season/week are
cast, since those are what downstream joins and filters depend on. The rest
keep whatever type bronze inferred — including the ~193 pbp columns nflverse
stores as Float64 to carry nulls. Casting those is a gold-layer concern, done
per metric rather than wholesale.

Bronze metadata handling:
    ingest_date   KEPT  - orders the dedupe, and says which snapshot won
    source_file   dropped - reconstructable from ingest_date and the raw layout
    ingested_at   dropped - evaluated per micro-batch, so it does not order rows
    _rescued_data dropped - a bronze drift signal, not silver data
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Dropped on the way into silver. ingest_date is deliberately not here.
BRONZE_METADATA = ["source_file", "ingested_at", "_rescued_data"]


def drop_metadata(df: DataFrame) -> DataFrame:
    """
    Drop bronze's metadata columns.

    DataFrame.drop ignores names that are not present, so this is safe on a
    table that never gained a _rescued_data column.
    """
    return df.drop(*BRONZE_METADATA)


def latest_per(*keys: str) -> Window:
    """
    Window picking the newest snapshot per grain.

    Ordered by ingest_date rather than ingested_at: ingested_at comes from
    current_timestamp(), which is evaluated once per micro-batch, so a backfill
    gives thousands of rows the same value and nothing to order by. ingest_date
    is the partition the ingestion job wrote, which is the real version.
    """
    return Window.partitionBy(*keys).orderBy(col("ingest_date").desc())


# ---------------------------------------------------------------------------
# Deduplicated views
# ---------------------------------------------------------------------------
# Not materialized. They exist so the fact table and its quarantine table see
# exactly the same rows — a row is then either in one or the other, never both
# and never neither. Writing the dedupe twice would let them drift apart.

@dp.view(name="pbp_deduped")
def pbp_deduped() -> DataFrame:
    return (
        drop_metadata(spark.read.table("bronze_pbp"))
        .withColumn("_rn", row_number().over(latest_per("game_id", "play_id")))
        .filter("_rn = 1")
        .drop("_rn")
    )


# ---------------------------------------------------------------------------
# Quality rules
# ---------------------------------------------------------------------------
# Defined once and referenced by both the fact table and its quarantine table,
# so the two can never disagree about what "bad" means.

PLAY_RULES = {
    "game_id_present": "game_id IS NOT NULL",
    "play_id_present": "play_id IS NOT NULL",
    "season_in_range": "season BETWEEN 1999 AND 2100",
    "week_in_range": "week BETWEEN 1 AND 23",
}


def failing(rules: dict) -> str:
    """SQL predicate matching rows that fail at least one rule."""
    return "NOT (" + " AND ".join(rules.values()) + ")"


# ---------------------------------------------------------------------------
# Facts
# ---------------------------------------------------------------------------

@dp.table(
    name="fact_plays",
    comment="One row per play. Grain: game_id + play_id. Deduplicated to the newest snapshot.",
)
@dp.expect_all_or_drop(PLAY_RULES)
def fact_plays() -> DataFrame:
    return (
        spark.read.table("pbp_deduped")
        # Only the keys and the time grain are cast. play_id arrives as a float
        # because nflverse uses floats to carry nulls; a key should be an
        # integer so joins and sorts behave.
        .withColumn("play_id", col("play_id").cast("long"))
        .withColumn("season", col("season").cast("int"))
        .withColumn("week", col("week").cast("int"))
    )


@dp.table(
    name="fact_plays_quarantine",
    comment="Plays failing a PLAY_RULES expectation. Same source rows as fact_plays, inverted.",
)
def fact_plays_quarantine() -> DataFrame:
    return spark.read.table("pbp_deduped").filter(failing(PLAY_RULES))


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

@dp.table(
    name="dim_game",
    comment="One row per game. Derived from pbp, which repeats game attributes on every play.",
)
def dim_game() -> DataFrame:
    # Verified on 2025: 285 distinct game_id and 285 distinct rows across these
    # attributes, so game_id determines all of them and dropDuplicates is safe.
    return (
        spark.read.table("pbp_deduped")
        .select(
            "game_id",
            "season",
            "season_type",
            "week",
            "game_date",
            "start_time",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "result",
            "total",
            "spread_line",
            "total_line",
            "div_game",
            "location",
            "stadium",
            "stadium_id",
            "roof",
            "surface",
            "weather",
            "temp",
            "wind",
            "home_coach",
            "away_coach",
        )
        .dropDuplicates(["game_id"])
    )


# ---------------------------------------------------------------------------
# Remaining tables — uncomment as each bronze source is added
# ---------------------------------------------------------------------------
#
# dim_player is the crosswalk that makes the whole model join. The players
# table carries gsis_id, pfr_id, esb_id, espn_id and more, so it bridges the
# three different player identifiers the facts arrive with:
#
#     pbp                 43 role-playing *_player_id columns, all gsis
#     nextgen_stats       player_gsis_id
#     pfr_advstats        pfr_player_id
#
# Without it, PFR stats cannot be joined to anything else by player.
#
# @dp.view(name="players_deduped")
# def players_deduped() -> DataFrame:
#     return (
#         drop_metadata(spark.read.table("bronze_players"))
#         .withColumn("_rn", row_number().over(latest_per("gsis_id")))
#         .filter("_rn = 1")
#         .drop("_rn")
#     )
#
# PLAYER_RULES = {"gsis_id_present": "gsis_id IS NOT NULL"}
#
# @dp.table(name="dim_player", comment="One row per player. gsis_id is the key; pfr_id bridges PFR facts.")
# @dp.expect_all_or_drop(PLAYER_RULES)
# def dim_player() -> DataFrame:
#     return spark.read.table("players_deduped")
#
# @dp.table(name="dim_team", comment="One row per team. team_abbr is the key.")
# def dim_team() -> DataFrame:
#     return (
#         drop_metadata(spark.read.table("bronze_teams"))
#         .withColumn("_rn", row_number().over(latest_per("team_abbr")))
#         .filter("_rn = 1")
#         .drop("_rn")
#     )
#
# fact_participation shares fact_plays' grain exactly, but names the game key
# nflverse_game_id. Aliased here so the join is explicit rather than a surprise.
#
# @dp.table(name="fact_participation", comment="On-field personnel per play. Grain: game_id + play_id.")
# def fact_participation() -> DataFrame:
#     return (
#         drop_metadata(spark.read.table("bronze_participation"))
#         .withColumnRenamed("nflverse_game_id", "game_id")
#         .withColumn("_rn", row_number().over(latest_per("game_id", "play_id")))
#         .filter("_rn = 1")
#         .drop("_rn")
#     )
#
# The stat-type variants stay separate — different schemas, different measures.
# Grain differs between the two sources:
#     nextgen   season + week + player_gsis_id   (player-week)
#     pfr       game_id + pfr_player_id          (player-game)
#
# for _stat in ["passing", "receiving", "rushing"]:
#     ...  dedupe on ("season", "week", "player_gsis_id")
#
# for _stat in ["pass", "rush", "rec", "def"]:
#     ...  dedupe on ("game_id", "pfr_player_id")
