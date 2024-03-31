import json
import time
from datetime import datetime
import psycopg
from psycopg import errors

from create import *
from parse import *
from connection import *

# Data to allow the script to find data specific to the project statement:
relevantLeagues = ["La Liga", "Premier League"]
relevantSeasons = ["2020/2021", "2019/2020", "2018/2019", "2003/2004"]


# json from competitions.json
competitionData = []

# json from the respective data folders. Only from relevent seasons, each of the 4 entries are json data of a season.
# This will be very important later.
competitionIds = [] #initialized later
eventInfo = []
lineupInfo = []
matchInfo = [] #initialized later, for later use, the array has 4 indexes for each of the seasons.
matchIds = getMatchIds()

# relevant info from competitions.json
competitions_competition_id = []
competitions_season_id = []
competitions_competition_name = []
competitions_country = []
competitions_competition_gender = []
competitions_competition_youth = [] 
competitions_competition_international = [] 
competitions_season_name = [] 


def main():
    # A timer to show the pain we went through.
    start_time = time.time()

    try:
        # holds a connection object to connect to the database
        conn = psycopg.connect(    
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
        cur = conn.cursor()  

        dropTables()
        createTables() 

        # COMPETITION TABLE:
        with open("./data/competitions.json", "r", encoding="utf-8") as read_file:
            competitionData = json.load(read_file)

            #initilize some arrays from before while we have competitions.json open:
            competitionIds = getCompetitionIds(competitionData, relevantLeagues, relevantSeasons)
            matchInfo = getMatchInfo(competitionIds)

            for competition in competitionData:
                competitions_competition_id.append(competition["competition_id"])
                competitions_season_id.append(competition["season_id"])
                competitions_competition_name.append(competition["competition_name"])
                competitions_country.append(competition["country_name"])
                competitions_competition_gender.append(competition["competition_gender"])
                competitions_competition_youth.append(competition["competition_youth"])
                competitions_competition_international.append(competition["competition_international"])
                competitions_season_name.append(competition["season_name"])

                # Insert data into the Competitions table
                cur.execute("""
                    INSERT INTO Competitions 
                    (competition_id, season_id, season_name, competition_name, competition_gender,
                    competition_youth, competition_international, country)
                    VALUES 
                    (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        competition["competition_id"],
                        competition["season_id"],
                        competition["season_name"],
                        competition["competition_name"],
                        competition["competition_gender"],
                        competition["competition_youth"],
                        competition["competition_international"],
                        competition["country_name"]
                    )
                )
        conn.commit()


        # TEAMS, PLAYERS TABLE:
        dupeTeamId = []
        dir = "./data/lineups/"
        for match_id in matchIds:
            file_path = dir + str(match_id) + ".json"

            with open(file_path, "r", encoding="utf-8") as read_file:
                data = json.load(read_file)

                for team_data in data:
                    team_id = team_data['team_id']
                    team_name = team_data['team_name']
                    team_country = team_data['lineup'][0]['country']['name']  # Assuming country data is the same for all players


                    if (team_id not in dupeTeamId):
                        cur.execute('INSERT INTO Teams (id, name, country) VALUES (%s, %s, %s)', (team_id, team_name, team_country))
                        dupeTeamId.append(team_data['team_id'])

                    # Players here
                    for player_info in team_data["lineup"]:
                        player_id = player_info["player_id"]
                        player_name = player_info["player_name"]
                        player_nickname = player_info.get("player_nickname")
                        jersey_number = player_info["jersey_number"]
                        country = player_info["country"]["name"]

                        # Insert unique player records into the Players table
                        cur.execute(
                            "INSERT INTO Players (id, name, nickname, jersey_number, country, team_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s) "
                            "ON CONFLICT DO NOTHING",
                            (player_id, player_name, player_nickname, jersey_number, country, team_id)
                        )

        conn.commit()


        # STADIUM, MATCHES TABLE:         
        dupeMatchId = []
        dupeStadiumId = []

        for season in matchInfo:
            for match in season:
                if "stadium" in match:
                    stadium_id = match["stadium"]["id"]
                    stadium_name = match["stadium"]["name"]
                    stadium_country = match["stadium"]["country"]["name"]  
                    if stadium_id not in dupeStadiumId:
                        cur.execute(
                            "INSERT INTO Stadiums (id, name, country) "
                            "VALUES (%s, %s, %s)",
                            (stadium_id, stadium_name, stadium_country)
                        )
                        dupeStadiumId.append(stadium_id)
                else:
                    stadium_id = None


                match_id = match["match_id"]
                match_date = match["match_date"]
                kick_off = match["kick_off"]
                home_score = match["home_score"]
                away_score = match["away_score"]
                competition_stage_name = match["competition_stage"]["name"]
                competition_id = match["competition"]["competition_id"]
                season_id = match["season"]["season_id"]
                home_id = match["home_team"]["home_team_id"]
                away_id = match["away_team"]["away_team_id"]

                if (match_id not in dupeMatchId):
                    cur.execute(
                        "INSERT INTO Matches (id, date, kick_off, home_score, away_score, competition_stage_name, stadium_id, competition_id, season_id, home_id, away_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (match_id, match_date, kick_off, home_score, away_score, competition_stage_name, stadium_id, competition_id, season_id, home_id, away_id)
                    )
                    dupeMatchId.append(match["match_id"])

        conn.commit()


        #EVENTS TABLE:
        dupeEventId = []
        dir = "./data/events/"
        for match_id in matchIds:
            file_path = dir + str(match_id) + ".json"

            with open(file_path, "r", encoding="utf-8") as read_file:
                data = json.load(read_file)          
        
                for event in data:
                    event_id = event["id"]
                    index = event["index"]
                    period = event["period"]
                    timestamp_str = event["timestamp"]
                    timestamp = datetime.strptime(timestamp_str, "%H:%M:%S.%f").time()
                    minute = event["minute"]
                    second = event["second"]
                    possession = event["possession"]
                    possession_team_id = event["possession_team"]["id"]
                    event_type = event["type"]["name"]
                    play_pattern = event["play_pattern"]["name"]

                    if "position" in event:
                        position = event["position"]["name"]
                    else:
                        position = None

                    if "location" in event:
                        location_x = event["location"][0]
                        location_y = event["location"][1]
                    else:
                        location_x = None
                        location_y = None

                    if "player" in event:
                        player_id = event["player"]["id"]
                    else:
                        player_id = None

                    if "duration" in event:
                        duration = event["duration"]
                    else:
                        duration = None
        
                    # Insert event record into the Events table
                    if (event_id not in dupeEventId):
                        cur.execute(
                            "INSERT INTO Events (event_id, match_id, index, period, timestamp, minute, second, possession, "
                            "possession_team_id, type, duration, play_pattern, position, location_x, location_y, player_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            (event_id, match_id, index, period, timestamp, minute, second, possession, possession_team_id,
                            event_type, duration, play_pattern, position, location_x, location_y, player_id)
                        )
                        dupeEventId.append(event_id)
                        
                        # add corresponding data to the right event table
                        if (event["type"]["name"] == "Shot"):
                            end_location_x, end_location_y = event["shot"]["end_location"][:2]
                            outcome = event["shot"]["outcome"]["name"]
                            technique = event["shot"]["technique"]["name"]
                            body_part = event["shot"]["body_part"]["name"]
                            shot_type = event["shot"]["type"]["name"]
                            xG = event["shot"]["statsbomb_xg"]

                            if "shot" in event and event["shot"].get("first_time") is not None:
                                first_time = event["shot"]["first_time"]
                            else:
                                first_time = False

                            if "shot" in event and event["shot"].get("key_pass_id") is not None:
                                key_pass_id = event["shot"]["key_pass_id"]
                            else:
                                key_pass_id = None

                            cur.execute(
                                "INSERT INTO Shots (event_id, end_location_x, end_location_y, key_pass_id, outcome, first_time, technique, body_part, type, xG) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (event_id, end_location_x, end_location_y, key_pass_id, outcome, first_time, technique, body_part, shot_type, xG)
                            )

                        # Now passes..
                        if event["type"]["name"] == "Pass":
                            pass_data = event.get("pass", {}) #A lot of data for passes, so a temporary object will help with organization.
                            length = pass_data.get("length")
                            angle = pass_data.get("angle")
                            end_location_x, end_location_y = pass_data.get("end_location", [None, None])
                            body_part = pass_data.get("body_part", {}).get("name")
                            outcome = pass_data.get("outcome", {}).get("name")
                            pass_type = pass_data.get("type", {}).get("name")
                            height = pass_data.get("height", {}).get("name")
                            switch_status = pass_data.get("switch", False)
                            cross_status = pass_data.get("cross", False)
                            cutback = pass_data.get("cut_back", False)
                            through_ball = pass_data.get("through_ball", False)
                            assist = pass_data.get("goal_assist", False)
                            key_pass = pass_data.get("key_pass", False)
                            aerial_won = pass_data.get("aerial_won", False)
                            recipient_player_id = pass_data.get("recipient", {}).get("id")

                            cur.execute(
                                "INSERT INTO Passes (event_id, length, angle, end_location_x, end_location_y, body_part, outcome, type, height, switch_status, cross_status, cutback, through_ball, assist, key_pass, aerial_won, recipient_player_id) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (event_id, length, angle, end_location_x, end_location_y, body_part, outcome, pass_type, height, switch_status, cross_status, cutback, through_ball, assist, key_pass, aerial_won, recipient_player_id)
                            )
                        else:
                            # Handle case where event type is not "Pass"
                            pass_data = None


                        # same with dribbles...
                        if event["type"]["name"] == "Dribble":
                            dribble_data = event.get("dribble", {})
        
                            outcome = dribble_data.get("outcome", {}).get("name")
                            player_name = event["player"]["name"]
                            location_x, location_y = event.get("location", [None, None])
                            under_pressure = event.get("under_pressure", False)
                            complete = outcome == "Complete" 
                            
                            cur.execute(
                                "INSERT INTO Dribbles (event_id, name, under_pressure, complete) "
                                "VALUES (%s, %s, %s, %s)",
                                (event_id, player_name, under_pressure, complete)
                            )

                    conn.commit()

        conn.commit()

    except Exception as error:
        print(error)
    finally: #To ensure that the connection object and cursor always close:
        # close the cursor
        cur.close()
        # close the connection, once we are finished with it.
        conn.close() 
    

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")



main()
