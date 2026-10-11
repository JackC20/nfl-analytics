"""
The 2026 efficiency scatters, built from the gold tables, with team logos as
the points.

    python charts/nfl_charts.py                       # all five
    python charts/nfl_charts.py --chart team_epa      # just one
    python charts/nfl_charts.py --min-plays 100       # stricter QB cutoff
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
# The job's files are the deployed bundle, so write somewhere scratch instead.
OUT_DIR = Path(tempfile.gettempdir()) / "nfl_charts" if IN_DATABRICKS else HERE / "output"

QB_SQL = """
SELECT q.player_name, q.team_abbr, t.team_logo_espn, q.pass_plays, q.passing_epa_per_play, q.cpoe
FROM nfl.gold.agg_qb_season AS q
LEFT JOIN nfl.gold.dim_team AS t ON t.team_id = q.team_id
WHERE q.season = {season} AND q.season_type = 'REG' AND q.pass_plays >= {min_plays}
"""

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
CPOE_MEANS = "CPOE: how much more often a QB completes passes than expected, given how hard the throws were"
SOURCE = "Data: nflverse"

# Quadrant labels, as drawn: (top left, top right, bottom left, bottom right).
EFFICIENT_CONSISTENT = ("Consistent, but inefficient", "Efficient and consistent",
                        "Inefficient and inconsistent", "Efficient, but inconsistent")


def charts(min_plays: int, through: str) -> dict[str, tuple[str, ScatterSpec]]:
    """Each chart's query and layout. Keyed by the name used with --chart."""
    team_sql = TEAM_SQL.format(season=SEASON)
    season = f"{SEASON} regular season {through}".strip()
    # Team charts: the logo is the label. Abbreviations appear only for a team
    # whose logo couldn't be fetched.
    team = dict(label="team_abbr", image="team_logo_espn", label_images=False)
    return {
        "qb_epa_cpoe": (
            QB_SQL.format(season=SEASON, min_plays=min_plays),
            ScatterSpec(
                x="cpoe", y="passing_epa_per_play", label="player_name",
                # Smaller logos and names: with a low cutoff this chart has 40+ QBs.
                image="team_logo_espn", image_size=18, label_size=8.5,
                on_top="pass_plays",  # where logos stack, the QB with more plays stays visible
                title="QB efficiency vs. accuracy",
                subtitle=f"{season}. QBs with {min_plays}+ pass plays. Top right is best.",
                x_label="Completion % above expected (CPOE)",
                y_label="EPA per pass play",
                x_format="signed",
                quadrants=("Efficient, but inaccurate", "Efficient and accurate",
                           "Inefficient and inaccurate", "Accurate, but inefficient"),
                caption=f"{EPA_MEANS}.\n{CPOE_MEANS}.\n{SOURCE}",
            ),
        ),
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the 2026 efficiency scatters.")
    parser.add_argument("--chart", help="render only this chart (default: all)")
    parser.add_argument("--min-plays", type=int, default=30, help="QB chart cutoff, pass plays (default 30)")
    parser.add_argument("--email-to", help="Gmail address to email the charts to (and send from)")
    parser.add_argument("--profile", help="Databricks CLI profile (default: $DATABRICKS_CONFIG_PROFILE)")
    parser.add_argument("--warehouse-id", help="SQL warehouse ID (default: $DATABRICKS_WAREHOUSE_ID)")
    args = parser.parse_args()

    def query(sql_text: str):
        return run_query(sql_text, args.profile, args.warehouse_id)

    week = query(LATEST_WEEK_SQL.format(season=SEASON))["week"].iloc[0]
    through = f"through Week {int(week)}" if week == week else ""  # NaN-safe

    selected = charts(args.min_plays, through)
    if args.chart:
        if args.chart not in selected:
            raise SystemExit(f"Unknown chart '{args.chart}'. Choose from: {', '.join(selected)}")
        selected = {args.chart: selected[args.chart]}

    results = {}  # the four team charts share one query; run it once
    paths = []
    for name, (sql_text, spec) in selected.items():
        if sql_text not in results:
            results[sql_text] = query(sql_text)
        paths.append(scatter(results[sql_text], spec, OUT_DIR / f"{name}.png"))

    if args.email_to:
        title = f"NFL {SEASON} charts {through}".strip()
        send_charts(paths, args.email_to, subject=title, intro=f"{title}. Top right is best on every chart.")


if __name__ == "__main__":
    main()
