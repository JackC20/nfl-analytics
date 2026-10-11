"""
Copy every team's logo into the Databricks volume the charts job reads from.

The job runs on serverless compute, which in this workspace has no internet
access, so it can't download logos from ESPN itself. Run this once from your
laptop, and again only if a team changes its logo.

    pip install databricks-sdk nflreadpy
    python charts/upload_logos.py --profile nfl

The volume (nfl.gold.team_logos) is created by `databricks bundle deploy`
from resources/team_logos_volume.yml, so deploy first.
"""

from __future__ import annotations

import argparse
import io
import urllib.request

import nflreadpy as nfl
from databricks.sdk import WorkspaceClient

VOLUME = "/Volumes/nfl/gold/team_logos"


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload team logos to the charts volume.")
    parser.add_argument("--profile", help="Databricks CLI profile (default: your default profile)")
    args = parser.parse_args()

    workspace = WorkspaceClient(profile=args.profile)
    # Same source as gold.dim_team.team_logo_espn. Includes a few retired
    # abbreviations (OAK, SD, STL); copying those too is harmless.
    teams = nfl.load_teams().select("team_abbr", "team_logo_espn").to_dicts()

    uploaded = 0
    for team in teams:
        url = team["team_logo_espn"]
        if not url:
            continue
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (nfl-analytics charts)"})
        with urllib.request.urlopen(request, timeout=30) as response:
            image = response.read()
        # Named by team_abbr, which is how the chart queries look logos up.
        workspace.files.upload(f"{VOLUME}/{team['team_abbr']}.png", io.BytesIO(image), overwrite=True)
        uploaded += 1
        print(f"{team['team_abbr']:<4} {len(image) // 1024:>4} KB  {url}")
    print(f"Uploaded {uploaded} logos to {VOLUME}")


if __name__ == "__main__":
    main()
