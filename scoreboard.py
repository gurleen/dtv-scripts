import httpx
import polars as pl
from rich import print
import argparse
from pathlib import Path


MENS_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard?groups=10"
WOMENS_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?groups=10"


def parse_scoreboard(api_url: str) -> pl.DataFrame:
    """
    Parse ESPN scoreboard API JSON and extract team IDs, scores, and game period.

    Returns a DataFrame with columns:
    - game_id: unique game identifier
    - home_team_id: home team ID
    - away_team_id: away team ID
    - home_score: home team score
    - away_score: away team score
    - period: current period (quarter)
    - status: game status (e.g., "Final", "In Progress")
    """
    print(f"Fetching scoreboard data from {api_url}...")
    response = httpx.get(api_url, follow_redirects=True)
    data = response.json()

    games = []

    for event in data.get("events", []):
        game_id = event.get("id")

        for competition in event.get("competitions", []):
            competitors = competition.get("competitors", [])
            status = competition.get("status", {})

            # Initialize variables
            home_team_id = None
            away_team_id = None
            home_score = None
            away_score = None

            # Extract competitor data
            for competitor in competitors:
                team_id = competitor.get("team", {}).get("id")
                score = competitor.get("score")
                home_away = competitor.get("homeAway")

                if home_away == "home":
                    home_team_id = team_id
                    home_score = score
                elif home_away == "away":
                    away_team_id = team_id
                    away_score = score

            # Extract period and status
            period = status.get("period")
            status_type = status.get("type", {})
            status_description = status_type.get("shortDetail", status_type.get("description", ""))

            games.append({
                "game_id": game_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
                "home_score": home_score,
                "away_score": away_score,
                "period": period,
                "status": status_description,
            })

    return pl.DataFrame(games)


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Generate basketball scoreboard JSON files")
    parser.add_argument("output_path", nargs="?", default=".", help="Directory path to write output files (default: current directory)")
    args = parser.parse_args()

    # Ensure the output path exists
    output_dir = Path(args.output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    SPORTS = ["wbb", "mbb"]
    URLS = [WOMENS_API_URL, MENS_API_URL]

    for sport, url in zip(SPORTS, URLS):
        print(f"\n=== Processing {sport.upper()} ===")
        scoreboard = parse_scoreboard(url)

        print(f"Found {len(scoreboard)} games")
        print("\nSample data:")
        print(scoreboard.head(10))

        # Write to JSON
        output_file = output_dir / f"{sport}_scoreboard.json"
        scoreboard.write_json(output_file)
        print(f"Wrote parsed data to {output_file}")
