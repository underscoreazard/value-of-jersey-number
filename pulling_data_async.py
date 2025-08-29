import time
import os
from datetime import datetime
import itertools
import asyncio
from tqdm import tqdm
import aiohttp
import pandas as pd


async def fetch_all_player_data(player):
    """
    Fetch all data for a player (info, jersey numbers, market values) in parallel.
    """
    player_id = player["player_id"]

    async def fetch_player_info_task(session):
        return await get_player_info(player_id, session)

    async def fetch_jersey_number_task(session):
        return await get_player_jersey_number(player_id, session)

    async def fetch_market_values_task(session):
        return await get_player_market_values(player_id, session)

    async def fetch_stats_task(session):
        return await get_player_stats(player_id, session)

    # Run all tasks in parallel
    async with aiohttp.ClientSession() as session:
        player_info, jersey_numbers, market_values, player_stats = await asyncio.gather(
            fetch_player_info_task(session),
            fetch_jersey_number_task(session),
            fetch_market_values_task(session),
            fetch_stats_task(session),
        )

    return player_info, jersey_numbers, market_values, player_stats


async def get_clubs(competition_id, season_id):
    """
    Function to get clubs for a competition and season
    """
    url = f"http://localhost:8000/competitions/{competition_id}/clubs"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params={"season_id": season_id}) as response:
            if response.status == 200:
                clubs = await response.json()
                return [
                    {"club_id": club["id"], "club_name": club["name"]}
                    for club in clubs.get("clubs", [])
                ]
            else:
                raise SystemExit(
                    f"Error fetching clubs: {response.status} - {await response.text()}"
                )


async def get_players(club_id, season_id, player_ids):
    """
    Fetch player IDs for a specific club and season, adding them directly to the provided set.
    """
    url = f"http://localhost:8000/clubs/{club_id}/players"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params={"season_id": season_id}) as response:
            if response.status == 200:
                players = await response.json()
                # Directly add player IDs to the set
                for player in players.get("players", []):
                    player_ids.add(player["id"])
            else:
                raise SystemExit(
                    f"Error fetching players: {response.status} - {await response.text()}"
                )


async def get_player_info(player_id, session):
    """
    Retrieves information on a specific player
    """
    url = f"http://localhost:8000/players/{player_id}/profile"
    async with session.get(url) as response:
        if response.status == 200:
            player_json = await response.json()
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
            return {"player_id": player_id, "error": f"Error {response.status}"}


async def get_player_market_values(player_id, session):
    """
    Obtains every recorded market value for a given player
    """
    url = f"http://localhost:8000/players/{player_id}/market_value"
    async with session.get(url) as response:
        if response.status == 200:
            market_value_data = await response.json()
            processed_data = []
            for entry in market_value_data.get("marketValueHistory", []):
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
                        "club_name": entry.get("clubName"),
                        "value": entry.get("value"),
                        "season": season,
                    }
                )
            return processed_data
        else:
            return []


async def get_player_jersey_number(player_id, session):
    """
    Obtains every recorded jersey number a player has worn
    """
    url = f"http://localhost:8000/players/{player_id}/jersey_numbers"
    async with session.get(url) as response:
        if response.status == 200:
            jersey_data = await response.json()
            return [
                {
                    "player_id": player_id,
                    "season": entry.get("season"),
                    "club_id": entry.get("club"),
                    "jersey_number": entry.get("jerseyNumber"),
                }
                for entry in jersey_data.get("jerseyNumbers", [])
            ]
        else:
            return []


async def get_player_stats(player_id, session):
    """
    Obtains basic stats for a player's career
    """
    url = f"http://localhost:8000/players/{player_id}/stats"
    async with session.get(url) as response:
        if response.status == 200:
            stats_data = await response.json()
            return [
                {
                    "player_id": player_id,
                    "competition_id": entry.get("competitionID"),
                    "competition_name": entry.get("competitionName"),
                    "season": entry.get("seasonID"),
                    "club_id": entry.get("clubID"),
                    "appearances": entry.get("appearances"),
                    "minutes_played": entry.get("minutesPlayed"),
                    "goals": entry.get("goals"),
                    "assists": entry.get("assists"),
                    "yellow_cards": entry.get("yellowCards"),
                    "red_cards": entry.get("redCards"),
                }
                for entry in stats_data.get("stats", [])
            ]
        else:
            return []


async def build_player_dataset(competitions_seasons):
    """
    Builds the datasets for player_info, jersey_numbers, market_values, and competition_clubs
    """
    player_ids = set()
    competition_clubs = {}

    # Create semaphore inside the function
    semaphore = asyncio.Semaphore(100)

    async def process_club(competition_id, season_id, club, player_ids):
        """
        Process a single club: fetch players and their IDs
        """
        club_id = club["club_id"]
        club_name = club["club_name"]
        await get_players(club_id, season_id, player_ids)

        # Add club to competition_clubs dictionary
        club_key = (competition_id, club_id)
        if club_key not in competition_clubs:
            competition_clubs[club_key] = {
                "competition_id": competition_id,
                "club_id": club_id,
                "club_name": club_name,
                "season_ids": [season_id],
            }
        else:
            competition_clubs[club_key]["season_ids"].append(season_id)

    async def process_competition_season(competition_id, season_id, progress_bar):
        """
        Process all clubs for a given competition and season
        """
        clubs = await get_clubs(competition_id, season_id)

        # Process all clubs asynchronously
        await asyncio.gather(
            *[
                process_club(competition_id, season_id, club, player_ids)
                for club in clubs
            ]
        )
        progress_bar.update(1)

    # Step 1: Process all competition-season pairs to collect unique player IDs
    with tqdm(
        total=len(competitions_seasons),
        desc="[Processing all competition-season pairs]",
    ) as progress_bar:
        await asyncio.gather(
            *[
                process_competition_season(comp, season, progress_bar)
                for comp, season in competitions_seasons
            ]
        )

    print(f"[ Total unique players collected: {len(player_ids)} ]")

    # Step 2: Fetch data for all unique players asynchronously
    player_info_list = []
    jersey_numbers_list = []
    market_values_list = []
    player_stats_list = []

    async def process_player(player_id, progress_bar, semaphore):
        """
        Fetch all data for a single player
        """
        async with semaphore:
            player_info, jersey_numbers, market_values, player_stats = (
                await fetch_all_player_data({"player_id": player_id})
            )
            if player_info:
                player_info_list.append(player_info)
            if jersey_numbers:
                jersey_numbers_list.extend(jersey_numbers)
            if market_values:
                market_values_list.extend(market_values)
            if player_stats:
                player_stats_list.extend(player_stats)

            progress_bar.update(1)

    # Intermediary step to process in batches
    BATCH_SIZE = 1600
    batches = [
        list(player_ids)[i : i + BATCH_SIZE]
        for i in range(0, len(player_ids), BATCH_SIZE)
    ]

    for batch in batches:
        with tqdm(total=len(batch), desc="[Processing Players...]") as progress_bar:
            await asyncio.gather(
                *[
                    process_player(player_id, progress_bar, semaphore)
                    for player_id in batch
                ]
            )

    # Step 3: Convert competition_clubs dictionary to list
    competition_clubs_list = [
        {
            "competition_id": club_data["competition_id"],
            "season_id(s)": ",".join(
                map(str, sorted(club_data["season_ids"], reverse=True))
            ),
            "club_id": club_data["club_id"],
            "club_name": club_data["club_name"],
        }
        for club_data in competition_clubs.values()
    ]

    # Step 4: Add youth teams asynchronously
    """
    FOR FUTURE: THIS WON'T WORK FOR ALL TEAMS CORRECTLY AS IT WORKS OFF OF TEAM POPULARITY
    """

    async def add_youth_team_data(club):
        """
        Fetch youth team data for a single club
        """
        async with aiohttp.ClientSession() as session:
            try:
                url = f"http://localhost:8000/clubs/search/{club['club_name']}"
                async with session.get(url) as response:
                    if response.status == 200:
                        json_data = await response.json()
                        results = json_data.get("results", [])

                        club["youth_team_id"] = None
                        club["youth_team_name"] = None

                        senior_team_country = (
                            results[0].get("country") if results else None
                        )
                        for result in results[
                            1:
                        ]:  # Skip the first result (assumed senior team)
                            if result.get("country") == senior_team_country:
                                club["youth_team_id"] = result.get("id")
                                club["youth_team_name"] = result.get("name")
                                break  # Use the first match and stop searching
            except Exception as e:
                print(f"Error fetching youth team for {club['club_name']}: {e}")
                club["youth_team_id"] = None
                club["youth_team_name"] = None

        progress_bar.update(1)

    with tqdm(
        total=len(competition_clubs_list), desc="[Adding youth team information...]"
    ) as progress_bar:
        await asyncio.gather(
            *[add_youth_team_data(club) for club in competition_clubs_list]
        )

    # Export data
    export_data(
        player_info_list,
        jersey_numbers_list,
        market_values_list,
        player_stats_list,
        competition_clubs_list,
    )


def export_data(
    player_info_list,
    jersey_numbers_list,
    market_values_list,
    player_stats_list,
    competition_clubs_list,
):
    """
    Exports the datasets created into a .csv file
    """
    today = datetime.now().strftime("%Y_%m_%d_%H_%M")
    data_dir = os.path.join("data", today)
    os.makedirs(data_dir, exist_ok=True)

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

    player_stats_df = pd.DataFrame(player_stats_list)
    player_stats_df.to_csv(os.path.join(data_dir, "player_stats.csv"), index=False)

    # Export competition_clubs
    competition_clubs_df = pd.DataFrame(competition_clubs_list)
    competition_clubs_df.to_csv(
        os.path.join(data_dir, "competition_clubs.csv"), index=False
    )


if __name__ == "__main__":
    # To make sure no accident click ends up making a intensive call, I have commented out most of the competitions & seasons
    competitions = [
        "GB1",  # Premier League
        "ES1",  # LaLiga
        "L1",  # Bundesliga
        "IT1",  # Serie A
        "FR1",  # Ligue 1
    ]
    seasons = [
        "2023",
        # "2022",
        # "2021",
        # "2020",
        # "2019",
        # "2018",
        # "2017",
        # "2016",
        # "2015",
        # "2014",
    ]

    competitions_seasons = list(itertools.product(competitions, seasons))

    start_time = time.time()
    asyncio.run(build_player_dataset(competitions_seasons))
    print(
        f"[ Total build time for dataset was: {time.time() - start_time:.2f} seconds ]"
    )
