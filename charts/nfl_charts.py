"""
The 2026 efficiency scatters, built from the gold tables, with team logos as
the points: five for players (two QB, RB, WR, TE) and four for teams.

Player cutoffs scale with games played: a player qualifies with at least N
plays per game his team has played, so the bar rises as the season goes on.

    python charts/nfl_charts.py                       # all of them
    python charts/nfl_charts.py --chart team_epa      # just one
    python charts/nfl_charts.py --qb-per-game 10      # stricter QB cutoff
    python charts/nfl_charts.py --email-to you@gmail.com   # render, then email them

On your laptop, PNGs land in charts/output/ and the connection settings are
the same as scatter.py (DATABRICKS_CONFIG_PROFILE, DATABRICKS_WAREHOUSE_ID).
As a Databricks job task, queries run on the job's own compute and the PNGs
go to a temp folder; pass --email-to to get them. Email settings: mailer.py.

Every chart is oriented so up and to the right is better. Defensive measures
are EPA and success rate *allowed*, where lower is better, so those axes are
flipped and say so in their label.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

# Make the sibling modules importable however the script is launched
# (some Databricks task runners don't set __file__).
try:
    HERE = Path(__file__).resolve().parent
except NameError:
    HERE = Path(sys.argv[0]).resolve().parent
sys.path.insert(0, str(HERE))

from mailer import send_charts  # noqa: E402
from scatter import IN_DATABRICKS, ScatterSpec, run_query, scatter  # noqa: E402

SEASON = 2026
# Serverless compute here has no internet access, so in Databricks the logos
# come from a volume (loaded by upload_logos.py); on a laptop, straight from ESPN.
LOGO_DIR = "/Volumes/nfl/gold/team_logos"
# The job's files are the deployed bundle, so write somewhere scratch instead.
OUT_DIR = Path(tempfile.gettempdir()) / "nfl_charts" if IN_DATABRICKS else HERE / "output"

# A player qualifies with at least {per_game} plays per game his team has played.
QUALIFIES = """
JOIN nfl.gold.agg_team_season AS ts
  ON ts.team_id = p.team_id AND ts.season = p.season AND ts.season_type = p.season_type
LEFT JOIN nfl.gold.dim_team AS t ON t.team_id = p.team_id
WHERE p.season = {season} AND p.season_type = 'REG' AND p.{volume} >= {per_game} * ts.games
"""

QB_SQL = """
SELECT p.player_name, p.team_abbr, t.team_logo_espn, p.pass_plays, p.passing_epa_per_play, p.cpoe
FROM nfl.gold.agg_qb_season AS p
""" + QUALIFIES

# Each completion's EPA splits exactly into the throw (comp_air_epa, up to the
# catch point) and what came after it (comp_yac_epa).
QB_AIR_YAC_SQL = """
WITH completions AS (
    SELECT passer_player_id AS player_id,
           count(*) AS completions,
           sum(comp_air_epa) AS air_epa,
           sum(comp_yac_epa) AS yac_epa
    FROM nfl.gold.fact_play
    WHERE season = {season} AND season_type = 'REG' AND play_type = 'pass'
      AND complete_pass = 1 AND coalesce(two_point_attempt, 0) = 0 AND passer_player_id IS NOT NULL
    GROUP BY passer_player_id
)
SELECT p.player_name, p.team_abbr, t.team_logo_espn, p.pass_plays,
       c.air_epa / c.completions AS air_epa_per_completion,
       c.yac_epa / c.completions AS yac_epa_per_completion
FROM nfl.gold.agg_qb_season AS p
JOIN completions AS c ON c.player_id = p.player_id
""" + QUALIFIES

RB_SQL = """
SELECT p.player_name, p.team_abbr, t.team_logo_espn, p.carries, p.rushing_epa_per_carry, p.rushing_success_rate
FROM nfl.gold.agg_rb_season AS p
""" + QUALIFIES

RECEIVER_SQL = """
SELECT p.player_name, p.position, p.team_abbr, t.team_logo_espn, p.targets, p.target_share,
       p.receiving_epa_per_target
FROM nfl.gold.agg_wr_te_season AS p
""" + QUALIFIES + "  AND p.position = '{position}'\n"

TEAM_SQL = """
SELECT team_abbr, team_logo_espn, games, off_epa_per_play, def_epa_per_play, off_success_rate, def_success_rate
FROM nfl.gold.agg_team_season
WHERE season = {season} AND season_type = 'REG'
"""

LATEST_WEEK_SQL = """
SELECT max(week) AS week
FROM nfl.gold.dim_game
WHERE season = {season} AND game_type = 'REG' AND is_completed
"""

# Plain-language wording: these charts are meant to be shared with people who
# don't follow advanced stats, so axis labels and captions define every term.
FLIPPED_UP = "(flipped: up is better)"
FLIPPED_RIGHT = "(flipped: right is better)"
EPA_MEANS = ("EPA (expected points added): how many points a play adds, on average, "
             "compared with what was expected before the snap")
SUCCESS_MEANS = "Success rate: share of plays that improved the offense's chances of scoring"
RUSH_SUCCESS_MEANS = "Success rate: share of carries that improved the offense's chances of scoring"
TARGET_SHARE_MEANS = "Target share: share of his team's targets that went his way, in games he played"
AIR_YAC_MEANS = ("Each completion's EPA is split into the throw (up to the catch) and what happened "
                 "after the catch, then averaged per completion")
CPOE_MEANS = "CPOE: how much more often a QB completes passes than expected, given how hard the throws were"
SOURCE = "Data: nflverse"

# Pass catchers get a chart per position, each with its own cutoff:
# position -> (chart name, title, short plural, key in per_game).
RECEIVERS = {
    "WR": ("wr_role_efficiency", "Wide receivers", "WRs", "wr"),
    "TE": ("te_role_efficiency", "Tight ends", "TEs", "te"),
}

# Player charts carry a name on every point, so they get a bigger canvas.
PLAYER_FIGSIZE = (10, 10)

# Quadrant labels, as drawn: (top left, top right, bottom left, bottom right).
EFFICIENT_CONSISTENT = ("Consistent, but inefficient", "Efficient and consistent",
                        "Inefficient and inconsistent", "Efficient, but inconsistent")


def charts(per_game: dict[str, float], through: str) -> dict[str, tuple[str, ScatterSpec]]:
    """Each chart's query and layout. Keyed by the name used with --chart.

    per_game: the qualifying plays per team game for "qb", "rb", "wr" and "te".
    """
    team_sql = TEAM_SQL.format(season=SEASON)
    season = f"{SEASON} regular season {through}".strip()
    qb, rb = (f"{per_game[k]:g}" for k in ("qb", "rb"))
    # Player charts: a logo and a name on every point. Where logos stack, the
    # player with more volume is drawn on top.
    player = dict(image="logo", image_size=20, label_size=9, figsize=PLAYER_FIGSIZE)
    # Team charts: the logo is the label. Abbreviations appear only for a team
    # whose logo couldn't be fetched.
    team = dict(label="team_abbr", image="logo", label_images=False)
    return {
        "qb_epa_cpoe": (
            QB_SQL.format(season=SEASON, volume="pass_plays", per_game=per_game["qb"]),
            ScatterSpec(
                x="cpoe", y="passing_epa_per_play", label="player_name", on_top="pass_plays",
                title="QB efficiency vs. accuracy",
                subtitle=f"{season}. QBs with {qb}+ pass plays per team game. Top right is best.",
                x_label="Completion % above expected (CPOE)",
                y_label="EPA per pass play",
                x_format="signed",
                quadrants=("Efficient, but inaccurate", "Efficient and accurate",
                           "Inefficient and inaccurate", "Accurate, but inefficient"),
                caption=f"{EPA_MEANS}.\n{CPOE_MEANS}.\n{SOURCE}",
                **player,
            ),
        ),
        "qb_air_vs_yac": (
            QB_AIR_YAC_SQL.format(season=SEASON, volume="pass_plays", per_game=per_game["qb"]),
            ScatterSpec(
                x="air_epa_per_completion", y="yac_epa_per_completion", label="player_name",
                on_top="pass_plays",
                title="QB value: the throw vs. after the catch",
                subtitle=f"{season}. QBs with {qb}+ pass plays per team game. Top right is best.",
                x_label="EPA from the throw, per completion",
                y_label="EPA after the catch, per completion",
                quadrants=("Leans on yards after catch", "Strong throws and strong YAC",
                           "Little from either", "Leans on the throw"),
                caption=f"{EPA_MEANS}.\n{AIR_YAC_MEANS}.\n{SOURCE}",
                **player,
            ),
        ),
        "rb_efficiency": (
            RB_SQL.format(season=SEASON, volume="carries", per_game=per_game["rb"]),
            ScatterSpec(
                x="rushing_epa_per_carry", y="rushing_success_rate", label="player_name",
                on_top="carries",
                title="Running backs: efficiency vs. consistency",
                subtitle=f"{season}. RBs with {rb}+ carries per team game. Top right is best.",
                x_label="Rushing EPA per carry", y_label="Rushing success rate", y_format="percent",
                quadrants=EFFICIENT_CONSISTENT,
                caption=f"{EPA_MEANS}.\n{RUSH_SUCCESS_MEANS}.\n{SOURCE}",
                **player,
            ),
        ),
        **{
            name: (
                RECEIVER_SQL.format(season=SEASON, volume="targets", per_game=per_game[key],
                                    position=position),
                ScatterSpec(
                    x="target_share", y="receiving_epa_per_target", label="player_name",
                    on_top="targets",
                    title=f"{group}: role vs. efficiency",
                    subtitle=f"{season}. {short} with {per_game[key]:g}+ targets per team game. Top right is best.",
                    x_label="Target share", y_label="EPA per target", x_format="percent",
                    quadrants=("Efficient in a small role", "Big role, efficient",
                               "Small role, inefficient", "Big role, inefficient"),
                    caption=f"{TARGET_SHARE_MEANS}.\n{EPA_MEANS}.\n{SOURCE}",
                    **player,
                ),
            )
            for position, (name, group, short, key) in RECEIVERS.items()
        },
        "team_epa": (
            team_sql,
            ScatterSpec(
                x="off_epa_per_play", y="def_epa_per_play",
                title="Team efficiency: offense vs. defense",
                subtitle=f"{season}. Top right is best.",
                x_label="Offense: EPA per play", y_label=f"Defense: EPA allowed per play {FLIPPED_UP}",
                invert_y=True,
                quadrants=("Efficient defense only", "Efficient on both sides",
                           "Inefficient on both sides", "Efficient offense only"),
                caption=f"{EPA_MEANS}.\n{SOURCE}",
                **team,
            ),
        ),
        "team_success_rate": (
            team_sql,
            ScatterSpec(
                x="off_success_rate", y="def_success_rate",
                title="Team consistency: offense vs. defense",
                subtitle=f"{season}. Top right is best.",
                x_label="Offense: success rate", y_label=f"Defense: success rate allowed {FLIPPED_UP}",
                invert_y=True, x_format="percent", y_format="percent",
                quadrants=("Consistent defense only", "Consistent on both sides",
                           "Inconsistent on both sides", "Consistent offense only"),
                caption=f"{SUCCESS_MEANS}.\n{SOURCE}",
                **team,
            ),
        ),
        "offense_epa_vs_success": (
            team_sql,
            ScatterSpec(
                x="off_epa_per_play", y="off_success_rate",
                title="Offense: efficiency vs. consistency",
                subtitle=f"{season}. Top right is best.",
                x_label="EPA per play", y_label="Success rate", y_format="percent",
                quadrants=EFFICIENT_CONSISTENT,
                caption=f"{EPA_MEANS}.\n{SUCCESS_MEANS}.\n{SOURCE}",
                **team,
            ),
        ),
        "defense_epa_vs_success": (
            team_sql,
            ScatterSpec(
                x="def_epa_per_play", y="def_success_rate",
                title="Defense: efficiency vs. consistency",
                subtitle=f"{season}. Top right is best.",
                x_label=f"EPA allowed per play {FLIPPED_RIGHT}",
                y_label=f"Success rate allowed {FLIPPED_UP}",
                invert_x=True, invert_y=True, y_format="percent",
                quadrants=EFFICIENT_CONSISTENT,
                caption=f"{EPA_MEANS}.\n{SUCCESS_MEANS}.\n{SOURCE}",
                **team,
            ),
        ),
    }


def with_logos(df):
    """Add the logo each point is drawn with: the volume copy in Databricks, ESPN's URL elsewhere."""
    df = df.copy()
    df["logo"] = LOGO_DIR + "/" + df["team_abbr"] + ".png" if IN_DATABRICKS else df["team_logo_espn"]
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the 2026 efficiency scatters.")
    parser.add_argument("--chart", help="render only this chart (default: all)")
    parser.add_argument("--qb-per-game", type=float, default=6,
                        help="QB charts: pass plays per team game to qualify (default 6)")
    parser.add_argument("--rb-per-game", type=float, default=8,
                        help="RB chart: carries per team game to qualify (default 8)")
    parser.add_argument("--wr-per-game", type=float, default=5,
                        help="WR chart: targets per team game to qualify (default 5)")
    parser.add_argument("--te-per-game", type=float, default=3,
                        help="TE chart: targets per team game to qualify (default 3)")
    parser.add_argument("--email-to", help="Gmail address to email the charts to (and send from)")
    parser.add_argument("--profile", help="Databricks CLI profile (default: $DATABRICKS_CONFIG_PROFILE)")
    parser.add_argument("--warehouse-id", help="SQL warehouse ID (default: $DATABRICKS_WAREHOUSE_ID)")
    args = parser.parse_args()

    def query(sql_text: str):
        return run_query(sql_text, args.profile, args.warehouse_id)

    week = query(LATEST_WEEK_SQL.format(season=SEASON))["week"].iloc[0]
    through = f"through Week {int(week)}" if week == week else ""  # NaN-safe

    per_game = {"qb": args.qb_per_game, "rb": args.rb_per_game, "wr": args.wr_per_game, "te": args.te_per_game}
    selected = charts(per_game, through)
    if args.chart:
        if args.chart not in selected:
            raise SystemExit(f"Unknown chart '{args.chart}'. Choose from: {', '.join(selected)}")
        selected = {args.chart: selected[args.chart]}

    results = {}  # charts that share a query (the four team charts) run it once
    paths = []
    for name, (sql_text, spec) in selected.items():
        if sql_text not in results:
            results[sql_text] = with_logos(query(sql_text))
        paths.append(scatter(results[sql_text], spec, OUT_DIR / f"{name}.png"))

    if args.email_to:
        title = f"NFL {SEASON} charts {through}".strip()
        send_charts(paths, args.email_to, subject=title, intro=f"{title}. Top right is best on every chart.")


if __name__ == "__main__":
    main()
