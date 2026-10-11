"""
The 2026 efficiency scatters, built from the gold tables, with team logos as
the points.

    python charts/nfl_charts.py                       # all five
    python charts/nfl_charts.py --chart team_epa      # just one
    python charts/nfl_charts.py --min-plays 150       # stricter QB cutoff
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

OFF_EPA = "Offensive EPA per play"
OFF_SR = "Offensive success rate"
DEF_EPA_UP = "Defensive EPA per play allowed (axis flipped: up is better)"
DEF_EPA_RIGHT = "Defensive EPA per play allowed (axis flipped: right is better)"
DEF_SR_UP = "Defensive success rate allowed (axis flipped: up is better)"


def charts(min_plays: int, through: str) -> dict[str, tuple[str, ScatterSpec]]:
    """Each chart's query and layout. Keyed by the name used with --chart."""
    team_sql = TEAM_SQL.format(season=SEASON)
    team_caption = "Data: nflverse play-by-play via nfl.gold.agg_team_season. Scrimmage plays only; dashed lines are the league average."
    # Team charts: the logo is the label. Abbreviations appear only for a team
    # whose logo couldn't be fetched.
    team = dict(label="team_abbr", image="team_logo_espn", label_images=False, caption=team_caption)
    return {
        "qb_epa_cpoe": (
            QB_SQL.format(season=SEASON, min_plays=min_plays),
            ScatterSpec(
                x="cpoe", y="passing_epa_per_play", label="player_name",
                image="team_logo_espn", image_size=22,  # smaller: the name is the main label here
                title="QB efficiency: EPA per pass play vs CPOE",
                subtitle=f"{SEASON} regular season {through} - QBs with {min_plays}+ pass plays (attempts + sacks). Top right is best.",
                x_label="CPOE: completion % over expected (percentage points)",
                y_label="EPA per pass play",
                x_format="signed",
                caption="Data: nflverse play-by-play via nfl.gold.agg_qb_season. Scrambles count as rushes, as nflverse credits them. Dashed lines: average of plotted QBs.",
            ),
        ),
        "team_epa": (
            team_sql,
            ScatterSpec(
                x="off_epa_per_play", y="def_epa_per_play",
                title="Team efficiency: offense vs defense (EPA per play)",
                subtitle=f"{SEASON} regular season {through}. Top right is best.",
                x_label=OFF_EPA, y_label=DEF_EPA_UP, invert_y=True,
                **team,
            ),
        ),
        "team_success_rate": (
            team_sql,
            ScatterSpec(
                x="off_success_rate", y="def_success_rate",
                title="Team consistency: offensive vs defensive success rate",
                subtitle=f"{SEASON} regular season {through}. A play is a success when its EPA is positive. Top right is best.",
                x_label=OFF_SR, y_label=DEF_SR_UP, invert_y=True,
                x_format="percent", y_format="percent",
                **team,
            ),
        ),
        "offense_epa_vs_success": (
            team_sql,
            ScatterSpec(
                x="off_epa_per_play", y="off_success_rate",
                title="Offense: EPA per play vs success rate",
                subtitle=f"{SEASON} regular season {through}. Right of the line: more explosive; above it: more consistent.",
                x_label=OFF_EPA, y_label=OFF_SR, y_format="percent",
                **team,
            ),
        ),
        "defense_epa_vs_success": (
            team_sql,
            ScatterSpec(
                x="def_epa_per_play", y="def_success_rate",
                title="Defense: EPA per play vs success rate allowed",
                subtitle=f"{SEASON} regular season {through}. Both axes flipped so top right is best.",
                x_label=DEF_EPA_RIGHT, y_label=DEF_SR_UP,
                invert_x=True, invert_y=True, y_format="percent",
                **team,
            ),
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the 2026 efficiency scatters.")
    parser.add_argument("--chart", help="render only this chart (default: all)")
    parser.add_argument("--min-plays", type=int, default=100, help="QB chart cutoff, pass plays (default 100)")
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
