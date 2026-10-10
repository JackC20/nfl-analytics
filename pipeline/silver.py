"""
Silver layer: Lakeflow declarative pipeline.

Scope: play-by-play, plus the game, player and team tables it joins to.
Participation, Next Gen Stats and PFR stay in bronze until play-by-play is
settled.

Silver holds cleaned source data at source grain, named for what it is.
Dimensional modeling (dim_ / fact_) happens in gold.

HOW EVERY TABLE IS BUILT: LATEST SNAPSHOT + WINDOW

Every raw file is a complete snapshot: one season of plays or games, or the
whole players or teams table. Each run, every silver table is rebuilt from the
latest snapshot:

  1. Latest snapshot. Keep only rows from the newest ingest_date, per season
     for plays and games, overall for players and teams. Older snapshots are
     superseded history; they stay in bronze and are not quarantined.
  2. Rank. row_number() over the table's key picks exactly one winner per key,
     so no silver table can hold a duplicate key.
  3. Split. Winners are typed and go to the table. Losing copies of a
     duplicated key, rows missing a key, and rows failing a rule go to the
     table's quarantine, with the reason. Between the two, every row of the
     latest snapshot is accounted for.

Because each season is rebuilt from its newest snapshot, a row nflverse removed
(a deleted play) is not in that snapshot and drops out of silver. That is the
reason for rebuilding rather than merging: a merge only touches keys present in
the new data, so removed rows would stay forever.

What it costs: the tables are materialized views recomputed each run, rather
than merges that only touch new rows. The window only ever sees the latest
snapshots (~1.3M plays at most), because the snapshot is selected first with a
narrow aggregate and a join rather than a window over all of bronze.

What it trusts: the newest file per season is complete. A truncated pull would
remove rows until the next good one. The ingestion job never writes an empty
pull, so an empty response cannot wipe a season.

TYPES

Measured, not guessed. Every numeric column arrives from nflverse as Float64,
which is how Polars carries nulls, not a statement about the values. The lists
below come from profiling every column across every row, reviewed by hand.

Indicators are tinyint rather than boolean on purpose. These columns exist to
be aggregated: success rate is AVG(success), touchdowns are SUM(touchdown).
As booleans every one of those needs a CASE expression.

Bronze metadata handling:
    ingest_date   KEPT    - picks the latest snapshot, says which one won
    source_file   dropped - reconstructable from ingest_date and the raw layout
    ingested_at   dropped - evaluated per micro-batch, so it does not order rows
    _rescued_data dropped - watched by bronze's nothing_rescued expectation
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window

# Dropped on the way into silver. ingest_date is deliberately not here.
BRONZE_METADATA = ["source_file", "ingested_at", "_rescued_data"]

# Identifies a snapshot. Not ingested_at: that comes from current_timestamp(),
# evaluated once per micro-batch, so a backfill gives thousands of rows the
# same value. ingest_date is the partition the ingestion job wrote, which is
# the real version of the data.
SEQUENCE_BY = "ingest_date"


# ---------------------------------------------------------------------------
# Grains
# ---------------------------------------------------------------------------
# Declared once. The windows rank on these and the quarantines are defined by
# them, so the two can never disagree about what a row is.

GRAINS = {
    "pbp": ["game_id", "play_id"],
    "players": ["gsis_id"],
    "teams": ["team_abbr"],
    "schedules": ["game_id"],
}


# ---------------------------------------------------------------------------
# Type maps
# ---------------------------------------------------------------------------

def build_types(*, indicators=(), integers=(), longs=()) -> dict[str, str]:
    """Assemble one table's column -> cast-type map."""
    m: dict[str, str] = {c: "tinyint" for c in indicators}
    m.update({c: "int" for c in integers})
    m.update({c: "long" for c in longs})
    return m


# season and week are cast explicitly rather than trusted to whatever Auto
# Loader inferred. season arrives from the Hive partition path as well as the
# file, so it can differ in type between sources.
SEASON_WEEK = {"season": "int", "week": "int"}


PBP_INDICATORS = [
    "aborted_play", "assist_tackle", "complete_pass",
    "defensive_extra_point_attempt", "defensive_extra_point_conv",
    "defensive_two_point_attempt", "defensive_two_point_conv",
    "drive_ended_with_score", "drive_inside20", "extra_point_attempt",
    "field_goal_attempt", "first_down", "first_down_pass",
    "first_down_penalty", "first_down_rush", "fourth_down_converted",
    "fourth_down_failed", "fumble", "fumble_forced", "fumble_lost",
    "fumble_not_forced", "fumble_out_of_bounds", "goal_to_go",
    "home_opening_kickoff", "incomplete_pass", "interception",
    "kickoff_attempt", "kickoff_downed", "kickoff_fair_catch",
    "kickoff_in_endzone", "kickoff_inside_twenty", "kickoff_out_of_bounds",
    "lateral_reception", "lateral_recovery", "lateral_return", "lateral_rush",
    "no_huddle", "out_of_bounds", "own_kickoff_recovery",
    "own_kickoff_recovery_td", "pass", "pass_attempt", "pass_touchdown",
    "penalty", "play", "play_deleted", "punt_attempt", "punt_blocked",
    "punt_downed", "punt_fair_catch", "punt_in_endzone", "punt_inside_twenty",
    "punt_out_of_bounds", "qb_dropback", "qb_hit", "qb_kneel", "qb_scramble",
    "qb_spike", "quarter_end", "replay_or_challenge", "return_touchdown",
    "rush", "rush_attempt", "rush_touchdown", "sack", "safety",
    "series_success", "shotgun", "solo_tackle", "sp", "special",
    "special_teams_play", "success", "tackle_with_assist", "tackled_for_loss",
    "third_down_converted", "third_down_failed", "timeout", "touchback",
    "touchdown", "two_point_attempt",
]

PBP_INTEGERS = [
    "air_yards", "away_timeouts_remaining", "defteam_score",
    "defteam_score_post", "defteam_timeouts_remaining", "down", "drive",
    "drive_first_downs", "drive_play_count", "drive_play_id_ended",
    "drive_play_id_started", "drive_quarter_end", "drive_quarter_start",
    "drive_yards_penalized", "fixed_drive", "fumble_recovery_1_yards",
    "fumble_recovery_2_yards", "game_seconds_remaining",
    "half_seconds_remaining", "home_timeouts_remaining", "kick_distance",
    "lateral_receiving_yards", "lateral_rushing_yards", "passing_yards",
    "penalty_yards", "posteam_score", "posteam_score_post",
    "posteam_timeouts_remaining", "qtr", "quarter_seconds_remaining",
    "receiving_yards", "return_yards", "rushing_yards", "score_differential",
    "score_differential_post", "series", "total_away_score",
    "total_home_score", "yardline_100", "yards_after_catch", "yards_gained",
    "ydsnet", "ydstogo",
]

# play_id is a long, not an int. The value fits an int easily (max 41,380);
# participation joins on it later, and pinning the type keeps that join from
# depending on what nflverse ships next.
PBP_TYPES = {
    **build_types(indicators=PBP_INDICATORS, integers=PBP_INTEGERS,
                  longs=["play_id"]),
    **SEASON_WEEK,
}

# players and teams carry no floating-point columns, so they need no type map.


# Columns where an empty string is pretending to be a value. pbp carries
# posteam = '' on 104 plays in 1999 and 2000, which later seasons write as
# null. An empty string passes every IS NOT NULL check and then matches no
# team in a join, so it is rewritten to null.
BLANK_AS_NULL = {
    "pbp": ["posteam", "defteam", "home_team", "away_team"],
}

# ---------------------------------------------------------------------------
# Shared transforms
# ---------------------------------------------------------------------------

def drop_metadata(df: DataFrame) -> DataFrame:
    """Drop bronze's metadata columns. drop() ignores names not present."""
    return df.drop(*BRONZE_METADATA)


def apply_types(df: DataFrame, types: dict[str, str]) -> DataFrame:
    """
    Cast every column the map knows about, in one projection.

    A chain of withColumn calls would build a plan nested 130 levels deep on
    pbp. One select does the same work as a single node.
    """
    return df.select(*[
        col(c).cast(types[c]).alias(c) if c in types else col(c)
        for c in df.columns
    ])


def blank_to_null(df: DataFrame, columns: list[str]) -> DataFrame:
    """Rewrite '' as null, so absence behaves like absence in joins."""
    targets = {c for c in columns if c in df.columns}
    if not targets:
        return df
    return df.select(*[
        F.expr(f"NULLIF({c}, '')").alias(c) if c in targets else col(c)
        for c in df.columns
    ])


def all_present(keys: list[str]) -> str:
    """SQL predicate matching rows where every key column is populated."""
    return " AND ".join(f"{k} IS NOT NULL" for k in keys)


def missing_any(keys: list[str]) -> str:
    """SQL predicate matching rows where at least one key column is null."""
    return " OR ".join(f"{k} IS NULL" for k in keys)


def failing(rules: dict) -> str:
    """SQL predicate matching rows that fail at least one rule."""
    return "NOT (" + " AND ".join(rules.values()) + ")"


def read_source(bronze_table: str, blanks: list[str] | None = None) -> DataFrame:
    """Read one bronze table, cleaned but not yet filtered, keyed or typed."""
    df = drop_metadata(spark.read.table(bronze_table))
    return blank_to_null(df, blanks) if blanks else df


# ---------------------------------------------------------------------------
# Silver table factory
# ---------------------------------------------------------------------------

def silver_table(
        name: str,
        bronze_table: str,
        keys: list[str],
        types: dict[str, str],
        snapshot_per: list[str],
        comment: str = "",
        blanks: list[str] | None = None,
        expect_all_or_drop: dict | None = None,
        expect_all: dict | None = None,
) -> None:
    """
    Define one silver table and its quantine, both rebuilt on each run.

      <name>              materialized view: the winning row for each key
      <name>_quarantine   materialized view: every other row of the latest
                          snapshot, with a quarantine_reason

    snapshot_per names what one snapshot covers: ["season"] when each file is one season,
    [] when each file is the whole table

    A factory rather than one function per table, so each call gets its own closure and binds the right arguments
    """

    def ranked() -> DataFrame:
        """
        The latest snapshot

        Every row numbered within its key, untyped
        """
        df = read_source(bronze_table, blanks)

        # Newest ingest_date per snapshot per value. An aggregate plus join
        # rather than a window: the aggregate shuffles two columns instead of
        # every wide row bronze has accumulated, and then join keeps only the
        # latest snapshot's rows. With snasphot per = [] the aggregate is one
        # row: the newest ingest date overall
        latest = df.groupBy(*snapshot_per).agg(F.max(SEQUENCE_BY).alias(SEQUENCE_BY))
        df = df.join(F.broadcast(latest), on=[*snapshot_per, SEQUENCE_BY], how="inner")

        # One winner per key. Copies within one snapshot share every columnexpect_all
        # there is to order byu, and nothing says which copy is right, so the
        # order only has to be deterministic: a hash of the whole row. The table
        # and its quarantine each compute this, and must agree on the winner
        window = Window.partitionBy(*keys).orderBy(F.xxhash64(*df.columns))
        return df.withColumn("key_rank", F.row_number().over(window))

    def winners() -> DataFrame:
        df = ranked().filter(f"key_rank = 1 AND {all_present(keys)}").drop("key_rank")
        return apply_types(df, types)

    # Expectations wrap the function first, then the dataset decorator: the
    # same order as stacking @dp.expect_all under @dp.materialized_view.
    if expect_all:
        winners = dp.expect_all(expect_all)(winners)
    if expect_all_or_drop:
        winners = dp.expect_all_or_drop(expect_all_or_drop)(winners)
    dp.materialized_view(name=name, comment=comment)(winners)

    @dp.materialized_view(
        name=f"{name}_quarantine",
        comment=(
            f"Rows of the latest {bronze_table} snapshot that did not make it "
            f"into {name}: missing a key ({', '.join(keys)}), a losing copy of "
            f"a duplicated key, or failing a quality rule. Expected empty."
        ),
    )
    def _quarantine() -> DataFrame:
        # Deliberately untyped. A quarantined row is evidence; casting it would
        # turn an unparseable value into a silent null. key_rank is kept so a
        # duplicate's copies can be compared side by side.
        reason = (
            F.when(F.expr(missing_any(keys)), "missing_key")
            .when(col("key_rank") > 1, "duplicate_key")
        )
        if expect_all_or_drop:
            reason = reason.when(F.expr(failing(expect_all_or_drop)), "failed_rule")
        return (
            ranked()
            .withColumn("quarantine_reason", reason)
            .filter(col("quarantine_reason").isNotNull())
        )


# ---------------------------------------------------------------------------
# Plays
# ---------------------------------------------------------------------------
# Defined once and referenced by both the table and its quarantine, so the two
# can never disagree about what "bad" means.

PLAY_RULES = {
    "season_in_range": "season BETWEEN 1999 AND 2100",
    "week_in_range": "week BETWEEN 1 AND 23",
}


# Warnings, not drops. Profiling found values that should not be possible:
# four timeouts_remaining columns bottom out at -1, and away_wp_post reaches
# 1.998 for something that is supposed to be a probability. Whether those are
# nflverse bugs or an encoding for "unknown" is not established, so they are
# counted rather than quarantined.
PLAY_WARNINGS = {
    "posteam_timeouts_sane":
        "posteam_timeouts_remaining IS NULL OR posteam_timeouts_remaining BETWEEN 0 AND 3",
    "defteam_timeouts_sane":
        "defteam_timeouts_remaining IS NULL OR defteam_timeouts_remaining BETWEEN 0 AND 3",
    "home_wp_post_in_range":
        "home_wp_post IS NULL OR home_wp_post BETWEEN 0 AND 1",
    "away_wp_post_in_range":
        "away_wp_post IS NULL OR away_wp_post BETWEEN 0 AND 1",
}


silver_table(
    name="silver.plays",
    bronze_table="bronze.pbp",
    keys=GRAINS["pbp"],
    types=PBP_TYPES,
    snapshot_per=["season"],        # each pbp file is one full season
    blanks=BLANK_AS_NULL["pbp"],
    expect_all_or_drop=PLAY_RULES,
    expect_all=PLAY_WARNINGS,
    comment="One row per play. Grain: game_id + play_id. Each season's newest snapshot.",
)

# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------
# The players table carries gsis_id, pfr_id, esb_id, espn_id and more side by
# side. pbp's *_player_id columns are all gsis, so gsis_id is the key. pfr_id
# is what will bridge PFR stats once those are added.

silver_table(
    name="silver.players",
    bronze_table="bronze.players",
    keys=GRAINS["players"],
    types={},
    snapshot_per=[],                # each players file is the whole table
    comment="One row per player. Grain: gsis_id. Newest snapshot.",
)


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------
# Kept at load_teams()'s own grain: one row per abbreviation, 36 rows for 32
# franchises. Three franchises have more than one abbreviation, all from
# relocations:
#
#     2510  Rams      LA, LAR, STL
#     2520  Raiders   LV, OAK
#     4400  Chargers  LAC, SD
#
# pbp's posteam, defteam, home_team and away_team join straight onto
# team_abbr. team_id is the franchise, so rolling the three relocated teams
# up to one row each is a GROUP BY team_id, left to gold.

silver_table(
    name="silver.teams",
    bronze_table="bronze.teams",
    keys=GRAINS["teams"],
    types={},
    snapshot_per=[],                # each teams file is the whole table
    comment="One row per team abbreviation. Grain: team_abbr. team_id is the franchise.",
)


# ---------------------------------------------------------------------------
# Games
# ---------------------------------------------------------------------------
# From load_schedules(), nflverse's game-level source, rather than collapsed
# out of plays. One row per game, including games not yet played: scores and
# results are null until kickoff and fill in on a later snapshot.
#
# Only season and week are cast. The other columns have not been profiled the
# way pbp's were, so they keep the types nflverse ships until they are.

GAME_RULES = {
    "season_in_range": "season BETWEEN 1999 AND 2100",
    "week_in_range": "week BETWEEN 1 AND 23",
}

silver_table(
    name="silver.games",
    bronze_table="bronze.schedules",
    keys=GRAINS["schedules"],
    types=SEASON_WEEK,
    snapshot_per=["season"],        # each schedules file is one full season
    expect_all_or_drop=GAME_RULES,
    comment="One row per game, played or scheduled. Grain: game_id. Each season's newest snapshot.",
)
