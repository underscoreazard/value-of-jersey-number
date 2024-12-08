import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import itertools
import time
import requests
import pandas as pd

# Global set for processed player IDs
processed_player_ids = set()


def fetch_all_player_data(player):
    """
    Fetch all data for a player (info, jersey numbers, market values) in parallel.
    """
    player_id = player["player_id"]
    if player_id in processed_player_ids:
        print(f"{player_id} data already exists!")
        return None, None, None

    def fetch_player_info_task():
        return fetch_player_info(player_id)

    def fetch_jersey_number_task():
        return get_player_jersey_number(player_id)

    def fetch_market_values_task():
        return get_player_market_values(player_id)

    # Run all tasks in parallel
    with ThreadPoolExecutor() as executor:
        player_info, jersey_numbers, market_values = executor.map(
            lambda func: func(),
            [
                fetch_player_info_task,
                fetch_jersey_number_task,
                fetch_market_values_task,
            ],
        )

    # Add to processed only after all tasks are complete
    processed_player_ids.add(player_id)

    return player_info, jersey_numbers, market_values


# Helper function to create directories
def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def get_clubs(competition_id, season_id):
    """
    Function to get clubs for a competition and season
    """

    url = f"http://localhost:8000/competitions/{competition_id}/clubs"
    response = requests.get(url, params={"season_id": season_id})
    if response.status_code == 200:
        clubs = response.json().get("clubs", [])
        return [{"club_id": club["id"], "club_name": club["name"]} for club in clubs]
    else:
        raise SystemExit(
            f"Error fetching clubs: {response.status_code} - {response.text}"
        )


def get_players(club_id, season_id):
    """
    Function to get every player in a club for a specific season
    """

    url = f"http://localhost:8000/clubs/{club_id}/players"
    response = requests.get(url, params={"season_id": season_id})
    if response.status_code == 200:
        players = response.json().get("players", [])
        return [
            {"player_id": player["id"], "player_name": player["name"]}
            for player in players
        ]
    else:
        raise SystemExit(
            f"Error fetching players: {response.status_code} - {response.text}"
        )


def fetch_player_info(player_id):
    url = f"http://localhost:8000/players/{player_id}/profile"
    response = requests.get(url)
    if response.status_code == 200:
        player_json = response.json()
        return {
            "player_id": player_id,
            "player_name": player_json.get("name"),
            "image_url": player_json.get("imageURL"),
            "date_of_birth": player_json.get("dateOfBirth"),
            "height": player_json.get("height"),
            "primary_citizenship": player_json.get("citizenship")[0],
            "secondary_citizenship": (
                player_json.get("citizenship", [None])[1]
                if len(player_json.get("citizenship", [])) > 1
                else None
            ),
            "main_position": player_json.get("position", {}).get("main"),
            "other_positions": ", ".join(
                player_json.get("position", {}).get("other", [])
            ),
            "preferred_foot": player_json.get("foot"),
            "outfitter": player_json.get("outfitter"),
        }
    else:
        return {"player_id": player_id, "error": f"Error {response.status_code}"}


def get_player_market_values(player_id):
    url = f"http://localhost:8000/players/{player_id}/market_value"
    response = requests.get(url)
    if response.status_code == 200:
        market_value_data = response.json().get("marketValueHistory", [])
        processed_data = []
        for entry in market_value_data:
            date = pd.to_datetime(entry["date"])
            season = (
                f"{(date.year - 1) % 100:02}/{date.year % 100:02}"
                if date.month < 7
                else f"{date.year % 100:02}/{(date.year + 1) % 100:02}"
            )
            processed_data.append(
                {
                    "player_id": player_id,
                    "date": entry.get("date"),
                    "club_id": entry.get("clubID"),
                    "value": entry.get("value"),
                    "season": season,
                }
            )
        return processed_data
    else:
        return []


def get_player_jersey_number(player_id):
    url = f"http://localhost:8000/players/{player_id}/jersey_numbers"
    response = requests.get(url)
    if response.status_code == 200:
        jersey_data = response.json().get("jerseyNumbers", [])
        return [
            {
                "player_id": player_id,
                "season": entry.get("season"),
                "club_id": entry.get("club"),
                "jersey_number": entry.get("jerseyNumber"),
            }
            for entry in jersey_data
        ]
    else:
        return []


def build_player_dataset(competitions_seasons):
    player_info_list = []
    jersey_numbers_list = []
    market_values_list = []

    for competition_id, season_id in competitions_seasons:
        clubs = get_clubs(competition_id, season_id)

        for club in clubs:
            club_id = club["club_id"]
            club_name = club["club_name"]
            print(f"Working on {club_name} players")

            players = get_players(club_id, season_id)

            # Parallel fetch all data for players
            with ThreadPoolExecutor() as executor:
                results = list(
                    executor.map(lambda p: fetch_all_player_data(p), players)
                )

            # Unpack results and append to respective lists
            for player_info, jersey_numbers, market_values in results:
                if player_info:
                    player_info_list.append(player_info)
                if jersey_numbers:
                    jersey_numbers_list.extend(jersey_numbers)
                if market_values:
                    market_values_list.extend(market_values)

    # Export data
    export_data(player_info_list, jersey_numbers_list, market_values_list)


def export_data(player_info_list, jersey_numbers_list, market_values_list):
    today = datetime.now().strftime("%Y_%m_%d")
    data_dir = os.path.join("data", today)
    create_directory(data_dir)

    # Export player_info
    player_info_df = pd.DataFrame(player_info_list)
    player_info_df.to_csv(os.path.join(data_dir, "player_info.csv"), index=False)

    # Export jersey_numbers
    jersey_numbers_df = pd.DataFrame(jersey_numbers_list)
    jersey_numbers_df.to_csv(
        os.path.join(data_dir, "player_jersey_numbers.csv"), index=False
    )

    # Export market_values
    market_values_df = pd.DataFrame(market_values_list)
    market_values_df.to_csv(
        os.path.join(data_dir, "player_market_values.csv"), index=False
    )

    # # Export competition_clubs
    # competition_clubs_list = list(competition_clubs_cache.values())
    # competition_clubs_df = pd.DataFrame(competition_clubs_list)
    # competition_clubs_df.to_csv(
    #     os.path.join(data_dir, "competition_clubs.csv"), index=False
    # )


if __name__ == "__main__":
    competitions = ["GB1"]
    seasons = ["2024", "2023"]

    competitions_seasons = list(itertools.product(competitions, seasons))

    start_time = time.time()
    build_player_dataset(competitions_seasons)
    end_time = time.time()

    print()
    print(f"[ Build time: {end_time - start_time:.2f} seconds ]")
