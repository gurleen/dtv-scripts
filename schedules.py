import httpx
import polars as pl
from io import BytesIO
from rich import print


def col_sub(col_a: str, col_b: str, new_col_name: str) -> pl.Expr:
    return pl.col(col_a).sub(pl.col(col_b)).alias(new_col_name)


def col_div(col_a: str, col_b: str, new_col_name: str) -> pl.Expr:
    divide = pl.col(col_a).truediv(pl.col(col_b))
    return pl.when(pl.col(col_b).eq(0)).then(0).otherwise(divide).alias(new_col_name)


WON = pl.col("won")
IS_HOME = pl.col("is_home")
IS_CONF_GAME = pl.col("is_conf_game")


CAA_CONF_ID = 10
WOMENS_SCHEDULE_URL = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_schedules/wbb_schedule_2026.parquet"
MENS_SCHEDULE_URL = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_mens_college_basketball_schedules/mbb_schedule_2026.parquet"
SPORTS = ["wbb", "mbb"]
URLS = [WOMENS_SCHEDULE_URL, MENS_SCHEDULE_URL]


for sport, url in zip(SPORTS, URLS):
    print(f"Fetching {sport} game logs..")
    schedules_parquet_req = httpx.get(url, follow_redirects=True)
    schedules_parquet = BytesIO(schedules_parquet_req.content)
    schedules = pl.read_parquet(schedules_parquet)

    games = schedules.filter(
        pl.col("status_type_completed").eq(True),
    ).select(
        "game_id",
        "home_id",
        "away_id",
        "home_winner",
        "away_winner",
        "home_conference_id",
        "away_conference_id",
    )

    home_games = (
        games.select(
            "game_id",
            "home_id",
            "away_id",
            "home_winner",
            "home_conference_id",
            "away_conference_id",
        )
        .rename(
            {
                "home_id": "team_id",
                "away_id": "opponent_id",
                "home_winner": "won",
                "home_conference_id": "conference_id",
                "away_conference_id": "opponent_conference_id",
            }
        )
        .with_columns(pl.lit(True).alias("is_home"))
    )

    away_games = (
        games.select(
            "game_id",
            "away_id",
            "home_id",
            "away_winner",
            "away_conference_id",
            "home_conference_id",
        )
        .rename(
            {
                "away_id": "team_id",
                "home_id": "opponent_id",
                "away_winner": "won",
                "away_conference_id": "conference_id",
                "home_conference_id": "opponent_conference_id",
            }
        )
        .with_columns(pl.lit(False).alias("is_home"))
    )

    team_games = pl.concat([home_games, away_games]).select(
        "game_id",
        "team_id",
        "opponent_id",
        "is_home",
        "won",
        "conference_id",
        "opponent_conference_id",
    )

    records = (
        team_games.with_columns(
            pl.col("conference_id")
            .eq(pl.col("opponent_conference_id"))
            .alias("is_conf_game")
        )
        .with_columns(
            (WON).cast(pl.Int8).alias("wins"),
            (WON & IS_HOME).cast(pl.Int8).alias("home_wins"),
            (WON & ~IS_HOME).cast(pl.Int8).alias("road_wins"),
            (WON & IS_CONF_GAME).cast(pl.Int8).alias("conf_wins"),
            (IS_HOME).cast(pl.Int8).alias("home_games"),
            (~IS_HOME).cast(pl.Int8).alias("road_games"),
            (IS_CONF_GAME).cast(pl.Int8).alias("conf_games"),
        )
        .group_by("team_id", "conference_id")
        .agg(
            pl.sum(
                "wins",
                "home_wins",
                "road_wins",
                "conf_wins",
                "home_games",
                "road_games",
                "conf_games",
            ),
            pl.len().alias("total_games"),
        )
        .with_columns(
            col_sub("total_games", "wins", "losses"),
            col_sub("home_games", "home_wins", "home_losses"),
            col_sub("road_games", "road_wins", "road_losses"),
            col_sub("conf_games", "conf_wins", "conf_losses"),
        )
        .with_columns(
            pl.format("{}-{}", "wins", "losses").alias("overall_display"),
            pl.format("{}-{}", "home_wins", "home_losses").alias("home_display"),
            pl.format("{}-{}", "road_wins", "road_losses").alias("road_display"),
            pl.format("{}-{}", "conf_wins", "conf_losses").alias("conf_display"),
            col_div("wins", "total_games", "win_pct").round(3),
            col_div("conf_wins", "conf_games", "conf_win_pct").round(3),
        )
        .filter(pl.col("conference_id").eq(CAA_CONF_ID))
        .sort(["conf_win_pct", "win_pct"], descending=True)
    )

    filename = f"{sport}_records.json"
    records.write_json(filename)
    print(f"Wrote to {filename}")
