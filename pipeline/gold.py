"""
Gold layer: Lakeflow declarative pipeline.

A dimensional model for reporting and visuals, built from silver. Scope is
the seasons in GOLD_SEASONS (2026 for now). Every table is a materialized view.

  Dimensions
    gold.dim_team            one row per franchise (team_id)
    gold.dim_player          one row per player (player_id = gsis_id)
    gold.dim_game            one row per game in scope

  Atomic fact
    gold.fact_play           one row per play: silver.plays in scope, plus the
                             gold-level flags every aggregate shares

  Aggregate facts
    gold.agg_player_game     player x game: passing, rushing, receiving
    gold.agg_player_season   player x season, with rates
    gold.agg_qb_season       \
    gold.agg_rb_season        > position views of agg_player_season
    gold.agg_wr_te_season    /
    gold.agg_team_game       team x game: offense and defense
    gold.agg_team_season     team x season, with rates

DESIGN

Season tables are built from the game tables by summing additive measures
(counts and totals) and then computing every rate from those season totals.
A season rate is never an average of game rates, which would weight a
3-attempt game the same as a 40-attempt one. The game tables carry rates too,
for weekly charts, but the season tables never read them.

Definitions live in one place: fact_play's flags. is_scrimmage_play decides
what counts as a play for team EPA and success rate; is_explosive is the
explosive-play definition. Change one there and every aggregate follows.

Keys are the natural ones: team_id (franchise), player_id (gsis), game_id.
They are stable, readable and already conformed in silver, so surrogate keys
would add a lookup without adding anything.

Aggregates also carry a few display columns (player name, position, headshot,
team abbreviation, logo) so a dashboard can plot them without joins. The
dimensions remain the source of truth for those attributes.

Player stats follow nflverse's own counting rules, so totals reconcile with
nflverse's published player stats:
  passing    the passer on pass plays and spikes. Sacks count against the
             passer; scrambles do not, because nflverse credits a scramble to
             the QB as a rush.
  rushing    the rusher on runs and kneels, scrambles included.
  receiving  the intended receiver on pass plays that were not sacks.
Two-point attempts are excluded everywhere.

Checked against nflverse's published 2026 player stats (weeks 1-5): every
counting stat (attempts, completions, yards, TDs, interceptions, sacks,
carries, targets, receptions, air yards, first downs) matches exactly, with
one known gap: yards after a lateral are credited by nflverse to the player
who caught the lateral, and are not here (4 players, up to 80 yards). EPA
matches exactly for most players and within about 2 for the rest, from
individual plays nflverse's stats build values slightly differently.

EPA is from the offense's point of view. Defensive EPA is EPA allowed, so lower
is better.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.window import Window

# Seasons loaded into gold. Add a season here to backfill it; every table
# below reads this list.
GOLD_SEASONS = [2026]

GAME_KEYS = ["season", "season_type", "week", "game_id"]


# ---------------------------------------------------------------------------
# Shared definitions
# ---------------------------------------------------------------------------

# A scrimmage play: a pass or run that counted (not wiped out by a penalty,
# which nflverse marks play_type = 'no_play') and was not a two-point try.
# pass = 1 (any dropback: passes, sacks, scrambles) and rush = 1 (designed
# runs) split these plays exactly.
IS_SCRIMMAGE = "play_type IN ('pass', 'run') AND coalesce(two_point_attempt, 0) = 0"

# Explosive play: 20+ yards on a dropback, 10+ on a designed run. Scrambles
# are dropbacks (pass = 1), so they are held to the 20-yard bar.
IS_EXPLOSIVE = f"""
    CASE
      WHEN NOT ({IS_SCRIMMAGE}) THEN 0
      WHEN pass = 1 AND yards_gained >= 20 THEN 1
      WHEN rush = 1 AND yards_gained >= 10 THEN 1
      ELSE 0
    END
"""

# Which plays each player stat counts, matching nflverse (see docstring).
PASSING_PLAYS = (
    "play_type IN ('pass', 'qb_spike') AND coalesce(two_point_attempt, 0) = 0"
    " AND passer_player_id IS NOT NULL"
)
RUSHING_PLAYS = (
    "play_type IN ('run', 'qb_kneel') AND coalesce(two_point_attempt, 0) = 0"
    " AND rusher_player_id IS NOT NULL"
)
TARGET_PLAYS = (
    "play_type = 'pass' AND coalesce(two_point_attempt, 0) = 0 AND sack = 0"
    " AND receiver_player_id IS NOT NULL"
)


def ratio(numerator: str, denominator: str) -> Column:
    """numerator / denominator, null rather than an error when it would divide by 0."""
    return F.expr(f"try_divide({numerator}, {denominator})")


def fact() -> DataFrame:
    return spark.read.table("gold.fact_play")


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="gold.dim_team",
    comment="One row per franchise. Grain: team_id. Abbreviation and names are the current ones.",
)
def dim_team() -> DataFrame:
    # silver.teams has a row per abbreviation, retired ones included (STL, SD,
    # OAK). Keep the one the in-scope play-by-play uses, which is the current
    # one. A franchise absent from those plays falls back to its first
    # abbreviation alphabetically, so every franchise still gets exactly one row.
    current = (
        spark.read.table("silver.plays")
        .filter(col("season").isin(GOLD_SEASONS))
        .select(col("posteam").alias("team_abbr"))
        .distinct()
        .withColumn("is_current", F.lit(1))
    )
    window = Window.partitionBy("team_id").orderBy(
        F.coalesce(col("is_current"), F.lit(0)).desc(), "team_abbr"
    )
    return (
        spark.read.table("silver.teams")
        .join(current, "team_abbr", "left")
        .withColumn("_rank", F.row_number().over(window))
        .filter("_rank = 1")
        .select(
            "team_id", "team_abbr", "team_name", "team_nick", "team_conf",
            "team_division", "team_color", "team_color2", "team_logo_espn",
            "team_logo_squared", "team_wordmark",
        )
    )


@dp.materialized_view(
    name="gold.dim_player",
    comment="One row per player. Grain: player_id (gsis_id). Every player, not just in-scope seasons.",
)
def dim_player() -> DataFrame:
    return spark.read.table("silver.players").select(
        col("gsis_id").alias("player_id"),
        col("display_name").alias("player_name"),
        "first_name", "last_name", "position", "position_group",
        col("latest_team").alias("latest_team_abbr"),
        "jersey_number",
        col("headshot").alias("headshot_url"),
        "height", "weight", "birth_date", "college_name",
        "rookie_season", "years_of_experience",
        "draft_year", "draft_round", "draft_pick", "draft_team", "status",
    )


@dp.materialized_view(
    name="gold.dim_game",
    comment="One row per game in GOLD_SEASONS, played or scheduled. Grain: game_id.",
)
def dim_game() -> DataFrame:
    return (
        spark.read.table("silver.games")
        .filter(col("season").isin(GOLD_SEASONS))
        .select(
            "game_id", "season", "game_type", "week", "gameday", "weekday",
            "kickoff_ts",
            "home_team_id", col("home_team").alias("home_team_abbr"), "home_score",
            "away_team_id", col("away_team").alias("away_team_abbr"), "away_score",
            "result", "total", "overtime", "spread_line", "total_line",
            "div_game", "location", "roof", "surface", "temp", "wind", "stadium",
            "home_coach", "away_coach", "home_qb_name", "away_qb_name", "referee",
            F.expr("home_score IS NOT NULL AND away_score IS NOT NULL").alias("is_completed"),
        )
    )


# ---------------------------------------------------------------------------
# Atomic fact
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="gold.fact_play",
    comment="One row per play in GOLD_SEASONS. Grain: game_id + play_id. silver.plays plus shared gold flags.",
)
def fact_play() -> DataFrame:
    return (
        spark.read.table("silver.plays")
        .filter(col("season").isin(GOLD_SEASONS))
        .withColumns({
            "is_scrimmage_play": F.expr(f"CASE WHEN {IS_SCRIMMAGE} THEN 1 ELSE 0 END").cast("tinyint"),
            "is_explosive": F.expr(IS_EXPLOSIVE).cast("tinyint"),
        })
    )


# ---------------------------------------------------------------------------
# Player aggregates
# ---------------------------------------------------------------------------
# Built role by role, because one play credits up to three players: the
# passer, the rusher and the receiver. Each role is aggregated to player x
# game, then the three are stacked and summed, so a QB who also runs, or a RB
# who also catches, ends up with one row per game.

PLAYER_GAME_KEYS = [*GAME_KEYS, "player_id", "team_id", "opponent_team_id"]


def by_player_game(plays: str, player_col: str) -> DataFrame:
    """Plays matching a role, grouped to the player credited with that role."""
    return (
        fact()
        .filter(plays)
        .groupBy(
            *GAME_KEYS,
            col(player_col).alias("player_id"),
            col("posteam_team_id").alias("team_id"),
            col("defteam_team_id").alias("opponent_team_id"),
        )
    )


def passing_by_game() -> DataFrame:
    return by_player_game(PASSING_PLAYS, "passer_player_id").agg(
        F.count(F.lit(1)).alias("pass_plays"),          # attempts + sacks + spikes
        F.expr("sum(complete_pass + incomplete_pass + interception)").alias("attempts"),
        F.sum("complete_pass").alias("completions"),
        F.sum("passing_yards").alias("passing_yards"),
        F.sum("pass_touchdown").alias("passing_tds"),
        F.sum("interception").alias("interceptions"),
        F.sum("sack").alias("sacks"),
        # Negative, the way nflverse reports it, so totals reconcile.
        F.expr("sum(CASE WHEN sack = 1 THEN yards_gained ELSE 0 END)").alias("sack_yards_lost"),
        F.expr("sum(CASE WHEN sack = 0 THEN air_yards END)").alias("passing_air_yards"),
        F.sum("first_down_pass").alias("passing_first_downs"),
        F.sum("qb_epa").alias("passing_epa"),
        F.sum("success").alias("passing_successes"),
        F.sum("cpoe").alias("cpoe_total"),               # cpoe is averaged over
        F.count("cpoe").alias("cpoe_plays"),             # the plays that have it
    )


def rushing_by_game() -> DataFrame:
    return by_player_game(RUSHING_PLAYS, "rusher_player_id").agg(
        F.count(F.lit(1)).alias("carries"),
        F.sum("rushing_yards").alias("rushing_yards"),
        F.sum("rush_touchdown").alias("rushing_tds"),
        F.sum("first_down_rush").alias("rushing_first_downs"),
        F.sum("epa").alias("rushing_epa"),
        F.sum("success").alias("rushing_successes"),
        F.expr("sum(CASE WHEN rushing_yards >= 10 THEN 1 ELSE 0 END)").alias("rushes_10_plus"),
    )


def receiving_by_game() -> DataFrame:
    return by_player_game(TARGET_PLAYS, "receiver_player_id").agg(
        F.count(F.lit(1)).alias("targets"),
        F.sum("complete_pass").alias("receptions"),
        F.sum("receiving_yards").alias("receiving_yards"),
        F.sum("pass_touchdown").alias("receiving_tds"),
        F.sum("air_yards").alias("receiving_air_yards"),
        F.sum("yards_after_catch").alias("receiving_yards_after_catch"),
        F.sum("first_down_pass").alias("receiving_first_downs"),
        F.sum("epa").alias("receiving_epa"),
        F.sum("success").alias("receiving_successes"),
        F.expr("sum(CASE WHEN receiving_yards >= 20 THEN 1 ELSE 0 END)").alias("receptions_20_plus"),
    )


def team_targets_by_game() -> DataFrame:
    """Each team's total targets and air yards per game: the denominators for shares."""
    return (
        fact()
        .filter(TARGET_PLAYS)
        .groupBy("game_id", col("posteam_team_id").alias("team_id"))
        .agg(
            F.count(F.lit(1)).alias("team_targets"),
            F.sum("air_yards").alias("team_air_yards"),
        )
    )


# Rates computed from whatever totals the row holds: one game's in
# agg_player_game, the whole season's in agg_player_season.
PLAYER_RATES = {
    "completion_pct": ratio("completions", "attempts"),
    "yards_per_attempt": ratio("passing_yards", "attempts"),
    "passing_epa_per_play": ratio("passing_epa", "pass_plays"),
    "passing_success_rate": ratio("passing_successes", "pass_plays"),
    "cpoe": ratio("cpoe_total", "cpoe_plays"),
    "yards_per_carry": ratio("rushing_yards", "carries"),
    "rushing_epa_per_carry": ratio("rushing_epa", "carries"),
    "rushing_success_rate": ratio("rushing_successes", "carries"),
    "catch_rate": ratio("receptions", "targets"),
    "yards_per_target": ratio("receiving_yards", "targets"),
    "yards_per_reception": ratio("receiving_yards", "receptions"),
    "receiving_epa_per_target": ratio("receiving_epa", "targets"),
    "average_depth_of_target": ratio("receiving_air_yards", "targets"),
    "target_share": ratio("targets", "team_targets"),
    "air_yards_share": ratio("receiving_air_yards", "team_air_yards"),
    "scrimmage_yards": F.expr("rushing_yards + receiving_yards"),
    "total_tds": F.expr("passing_tds + rushing_tds + receiving_tds"),
    "total_epa": F.expr("passing_epa + rushing_epa + receiving_epa"),
}


def player_display(df: DataFrame) -> DataFrame:
    """Attach name, position, headshot and team abbreviation for charts."""
    players = spark.read.table("gold.dim_player").select(
        "player_id", "player_name", "position", "position_group", "headshot_url"
    )
    teams = spark.read.table("gold.dim_team").select("team_id", "team_abbr")
    return df.join(players, "player_id", "left").join(teams, "team_id", "left")


@dp.materialized_view(
    name="gold.agg_player_game",
    comment="Passing, rushing and receiving per player per game. Grain: game_id + player_id.",
)
def agg_player_game() -> DataFrame:
    stacked = reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        [passing_by_game(), rushing_by_game(), receiving_by_game()],
    )
    measures = [c for c in stacked.columns if c not in PLAYER_GAME_KEYS]

    # A player missing a role in a game gets 0 for it, not null, so totals add.
    per_game = stacked.groupBy(*PLAYER_GAME_KEYS).agg(
        *[F.coalesce(F.sum(c), F.lit(0)).alias(c) for c in measures]
    )

    # Shares need the team's totals for every player in the game, including
    # a QB or RB with no targets, so they join after the roles are combined.
    return player_display(
        per_game.join(team_targets_by_game(), ["game_id", "team_id"], "left")
        .withColumns(PLAYER_RATES)
    )


@dp.materialized_view(
    name="gold.agg_player_season",
    comment="Passing, rushing and receiving per player per season, with rates. Grain: season + season_type + player_id.",
)
def agg_player_season() -> DataFrame:
    games = spark.read.table("gold.agg_player_game")
    display = ["player_name", "position", "position_group", "headshot_url", "team_abbr"]
    additive = [
        c for c in games.columns
        if c not in (*PLAYER_GAME_KEYS, *PLAYER_RATES, *display)
    ]
    season = games.groupBy("season", "season_type", "player_id").agg(
        F.countDistinct("game_id").alias("games"),
        # A player traded mid-season is shown with his most recent team.
        F.max_by("team_id", "week").alias("team_id"),
        *[F.sum(c).alias(c) for c in additive],
    )
    return player_display(season.withColumns(PLAYER_RATES))


# Position views: the same rows as agg_player_season, filtered by
# position_group, with the columns that matter for that position.

PLAYER_IDENTITY = [
    "season", "season_type", "player_id", "player_name", "position",
    "team_id", "team_abbr", "headshot_url", "games",
]

POSITION_VIEWS = {
    "gold.agg_qb_season": (["QB"], [
        "pass_plays", "attempts", "completions", "completion_pct", "passing_yards",
        "yards_per_attempt", "passing_tds", "interceptions", "sacks", "sack_yards_lost",
        "passing_air_yards", "passing_first_downs", "passing_epa", "passing_epa_per_play",
        "passing_success_rate", "cpoe", "carries", "rushing_yards", "rushing_tds",
        "rushing_epa", "total_tds", "total_epa",
    ]),
    "gold.agg_rb_season": (["RB"], [
        "carries", "rushing_yards", "yards_per_carry", "rushing_tds", "rushing_first_downs",
        "rushing_epa", "rushing_epa_per_carry", "rushing_success_rate", "rushes_10_plus",
        "targets", "receptions", "receiving_yards", "receiving_tds", "receiving_epa",
        "target_share", "scrimmage_yards", "total_tds", "total_epa",
    ]),
    "gold.agg_wr_te_season": (["WR", "TE"], [
        "targets", "receptions", "catch_rate", "receiving_yards", "yards_per_reception",
        "yards_per_target", "receiving_tds", "receiving_air_yards", "average_depth_of_target",
        "receiving_yards_after_catch", "receiving_first_downs", "receiving_epa",
        "receiving_epa_per_target", "target_share", "air_yards_share", "receptions_20_plus",
        "carries", "rushing_yards", "scrimmage_yards", "total_tds", "total_epa",
    ]),
}


def position_view(name: str, groups: list[str], columns: list[str]) -> None:
    """Define one position view. A factory so each view binds its own arguments."""

    @dp.materialized_view(
        name=name,
        comment=f"agg_player_season for position group {', '.join(groups)}.",
    )
    def _view() -> DataFrame:
        return (
            spark.read.table("gold.agg_player_season")
            .filter(col("position_group").isin(groups))
            .select(*PLAYER_IDENTITY, *columns)
        )


for _name, (_groups, _columns) in POSITION_VIEWS.items():
    position_view(_name, _groups, _columns)


# ---------------------------------------------------------------------------
# Team aggregates
# ---------------------------------------------------------------------------
# Every scrimmage play is counted twice: once for the offense (posteam) and
# once for the defense (defteam). The same measures, with an off_ or def_
# prefix, so offense and defense compare like for like.

def side_by_game(team_col: str, prefix: str) -> DataFrame:
    """Scrimmage-play measures per game for the team in team_col."""
    p = prefix
    return (
        fact()
        .filter("is_scrimmage_play = 1")
        .groupBy(*GAME_KEYS, col(team_col).alias("team_id"))
        .agg(
            F.count(F.lit(1)).alias(f"{p}_plays"),
            F.sum("pass").alias(f"{p}_dropbacks"),
            F.sum("rush").alias(f"{p}_rushes"),
            F.sum("epa").alias(f"{p}_epa"),
            F.sum("success").alias(f"{p}_successes"),
            F.expr("sum(CASE WHEN pass = 1 THEN epa ELSE 0 END)").alias(f"{p}_pass_epa"),
            F.expr("sum(CASE WHEN rush = 1 THEN epa ELSE 0 END)").alias(f"{p}_rush_epa"),
            F.expr("sum(CASE WHEN pass = 1 THEN success ELSE 0 END)").alias(f"{p}_pass_successes"),
            F.expr("sum(CASE WHEN rush = 1 THEN success ELSE 0 END)").alias(f"{p}_rush_successes"),
            F.sum("yards_gained").alias(f"{p}_yards"),
            F.expr("sum(CASE WHEN pass = 1 THEN yards_gained ELSE 0 END)").alias(f"{p}_pass_yards"),
            F.expr("sum(CASE WHEN rush = 1 THEN yards_gained ELSE 0 END)").alias(f"{p}_rush_yards"),
            F.sum("is_explosive").alias(f"{p}_explosives"),
            F.expr("sum(coalesce(interception, 0) + coalesce(fumble_lost, 0))").alias(f"{p}_turnovers"),
            F.sum("sack").alias(f"{p}_sacks"),
            F.sum("first_down").alias(f"{p}_first_downs"),
            F.expr("sum(third_down_converted + third_down_failed)").alias(f"{p}_third_downs"),
            F.sum("third_down_converted").alias(f"{p}_third_down_conversions"),
        )
    )


TEAM_RATES = {
    "off_epa_per_play": ratio("off_epa", "off_plays"),
    "off_success_rate": ratio("off_successes", "off_plays"),
    "off_pass_epa_per_play": ratio("off_pass_epa", "off_dropbacks"),
    "off_rush_epa_per_play": ratio("off_rush_epa", "off_rushes"),
    "off_yards_per_play": ratio("off_yards", "off_plays"),
    "off_pass_rate": ratio("off_dropbacks", "off_plays"),
    "off_explosive_rate": ratio("off_explosives", "off_plays"),
    "off_third_down_rate": ratio("off_third_down_conversions", "off_third_downs"),
    "def_epa_per_play": ratio("def_epa", "def_plays"),
    "def_success_rate": ratio("def_successes", "def_plays"),
    "def_pass_epa_per_play": ratio("def_pass_epa", "def_dropbacks"),
    "def_rush_epa_per_play": ratio("def_rush_epa", "def_rushes"),
    "def_yards_per_play": ratio("def_yards", "def_plays"),
    "def_explosive_rate": ratio("def_explosives", "def_plays"),
    "def_third_down_rate": ratio("def_third_down_conversions", "def_third_downs"),
    "point_differential": F.expr("points_for - points_against"),
    "turnover_margin": F.expr("def_takeaways - off_turnovers"),
}


def team_display(df: DataFrame) -> DataFrame:
    """Attach team names, division and logo for charts."""
    teams = spark.read.table("gold.dim_team").select(
        "team_id", "team_abbr", "team_name", "team_conf", "team_division",
        "team_color", "team_logo_espn",
    )
    return df.join(teams, "team_id", "left")


@dp.materialized_view(
    name="gold.agg_team_game",
    comment="Offense and defense per team per game. Grain: game_id + team_id. def_ measures are allowed.",
)
def agg_team_game() -> DataFrame:
    offense = side_by_game("posteam_team_id", "off")
    defense = (
        side_by_game("defteam_team_id", "def")
        # The defense's opponent turning it over is a takeaway; the sacks are its own.
        .withColumnRenamed("def_turnovers", "def_takeaways")
    )
    games = spark.read.table("gold.dim_game").select(
        "game_id", "gameday", "home_team_id", "away_team_id",
        "home_score", "away_score", "is_completed",
    )
    is_home = col("team_id") == col("home_team_id")
    df = (
        offense.join(defense, [*GAME_KEYS, "team_id"], "full")
        .join(games, "game_id", "left")
        .withColumns({
            "is_home": is_home,
            "opponent_team_id": F.when(is_home, col("away_team_id")).otherwise(col("home_team_id")),
            "points_for": F.when(is_home, col("home_score")).otherwise(col("away_score")),
            "points_against": F.when(is_home, col("away_score")).otherwise(col("home_score")),
        })
        .withColumns({
            # Null for a game without a final score, so sums skip it.
            "wins": F.when(col("is_completed"), (col("points_for") > col("points_against")).cast("int")),
            "losses": F.when(col("is_completed"), (col("points_for") < col("points_against")).cast("int")),
            "ties": F.when(col("is_completed"), (col("points_for") == col("points_against")).cast("int")),
        })
        .drop("home_team_id", "away_team_id", "home_score", "away_score")
        .withColumns(TEAM_RATES)
    )
    opponents = spark.read.table("gold.dim_team").select(
        col("team_id").alias("opponent_team_id"), col("team_abbr").alias("opponent_team_abbr")
    )
    return team_display(df.join(opponents, "opponent_team_id", "left"))


@dp.materialized_view(
    name="gold.agg_team_season",
    comment="Offense and defense per team per season, with rates. Grain: season + season_type + team_id.",
)
def agg_team_season() -> DataFrame:
    games = spark.read.table("gold.agg_team_game")
    additive = [
        c for c in games.columns
        if (c.startswith(("off_", "def_")) or c in ("points_for", "points_against", "wins", "losses", "ties"))
        and c not in TEAM_RATES
    ]
    season = games.groupBy("season", "season_type", "team_id").agg(
        F.countDistinct("game_id").alias("games"),
        *[F.sum(c).alias(c) for c in additive],
    )
    per_game = {
        "off_yards_per_game": ratio("off_yards", "games"),
        "def_yards_per_game": ratio("def_yards", "games"),
        "points_for_per_game": ratio("points_for", "games"),
        "points_against_per_game": ratio("points_against", "games"),
    }
    return team_display(season.withColumns(TEAM_RATES).withColumns(per_game))
