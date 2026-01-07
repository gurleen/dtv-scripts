import httpx
import polars as pl
from rich import print


API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?groups=10"


def parse_scoreboard() -> pl.DataFrame:
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
    print(f"Fetching scoreboard data from {API_URL}...")
    response = httpx.get(API_URL, follow_redirects=True)
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
    scoreboard = parse_scoreboard()

    print(f"\nFound {len(scoreboard)} games")
    print("\nSample data:")
    print(scoreboard.head(10))

    # Write to JSON
    output_file = "scoreboard.json"
    scoreboard.write_json(output_file)
    print(f"\nWrote parsed data to {output_file}")
