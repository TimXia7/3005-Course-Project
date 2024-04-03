import json
import time
from datetime import datetime
import psycopg

from create import *
from parse import *
from connection import *

# Data to allow the script to find data specific to the project statement:
relevantLeagues = ["La Liga", "Premier League"]
relevantSeasons = ["2020/2021", "2019/2020", "2018/2019", "2003/2004"]

# json from competitions.json
# We'll need competition data for other things, so might as well store it
competitionData = []

# json from the respective data folders. Only from relevent seasons, each of the 4 entries are json data of a season.
# This will be very important later.
competitionIds = [] #initialized later
eventInfo = []
lineupInfo = []
matchInfo = [] #initialized later, for later use, the array has 4 indexes for each of the seasons.
matchIds = getMatchIds()

# relevant info from competitions.json, which we will need later
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

            # Store the competition data for later
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


        # To insert cards after matches
        match_id_arr = []
        card_player_id_arr = []
        card_type_arr = []
        reason_arr = []
        card_time_arr = []
        period_arr = []

        # To insert lineups after matches
        lineup_match_id = []
        lineup_team_id = []

        # To insert Player Positions after matches
        position_player_id_arr = []
        position_start_reason_arr = []
        position_end_reason_arr = []
        position_arr = []
        from_date_arr = []
        to_date_arr = []

        

        # TEAMS, PLAYERS, POSITIONS, LINEUPS, CARDS TABLE:
        dupeTeamId = []
        for match_id in matchIds:
            file_path = "./data/lineups/" + str(match_id) + ".json"

            with open(file_path, "r", encoding="utf-8") as read_file:
                data = json.load(read_file)

                # Teams:
                for team_data in data:
                    team_id = team_data['team_id']
                    team_name = team_data['team_name']
                    team_country = team_data['lineup'][0]['country']['name'] 

                    # For Lineup table later:
                    lineup_team_id.append(team_id)
                    lineup_match_id.append(match_id)

                    if (team_id not in dupeTeamId):
                        cur.execute('INSERT INTO Teams (id, name, country) VALUES (%s, %s, %s)', (team_id, team_name, team_country))
                        dupeTeamId.append(team_data['team_id'])

                    # Players:
                    for playerInfo in team_data["lineup"]:
                        id =                playerInfo["player_id"]
                        name =              playerInfo["player_name"]
                        nickname =          playerInfo.get("player_nickname")
                        jersey_number =     playerInfo["jersey_number"]
                        country =           playerInfo["country"]["name"]

                        positions = playerInfo.get("positions", [])
                        for position in positions:
                            position_player_id_arr.append(id)
                            position_start_reason_arr.append(position.get("start_reason"))
                            position_end_reason_arr.append(position.get("end_reason"))
                            position_arr.append(position.get("position"))
                            from_date_arr.append(position.get("from"))
                            to_date_arr.append(position.get("to"))

                        # Insert unique player records into the Players table
                        cur.execute(
                            "INSERT INTO Players (id, name, nickname, jersey_number, country, team_id) "
                            "VALUES (%s, %s, %s, %s, %s, %s) "
                            "ON CONFLICT DO NOTHING",
                            (id, name, nickname, jersey_number, country, team_id)
                        )

                        cards = playerInfo.get("cards", [])
                        for card in cards:
                            match_id_arr.append(match_id)
                            card_player_id_arr.append(id)
                            card_type_arr.append(card.get("card_type"))
                            reason_arr.append(card.get("reason"))
                            card_time_arr.append(card.get("time"))
                            period_arr.append(card.get("period"))
                            #create them after the matches table below



        conn.commit()


        # STADIUM, MATCHES TABLE:         
        dupeMatchId = []
        dupeStadiumId = []

        for season in matchInfo:
            for match in season:
                if "stadium" in match: #in case stadium is not listed
                    
                    stadium_id =        match["stadium"]["id"]
                    stadium_name =      match["stadium"]["name"]
                    stadium_country =   match["stadium"]["country"]["name"]  

                    if stadium_id not in dupeStadiumId:
                        cur.execute(
                            "INSERT INTO Stadiums (id, name, country) "
                            "VALUES (%s, %s, %s)",
                            (stadium_id, stadium_name, stadium_country)
                        )
                        dupeStadiumId.append(stadium_id)
                else:
                    stadium_id = None

                #Managers:
                
                home_team_id = None
                away_team_id = None
                if "home_team" in match:
                    home_team_id = match["home_team"]["home_team_id"]
                if "away_team" in match:
                      away_team_id = match["away_team"]["away_team_id"]

                for team_data in [match.get("home_team", {}), match.get("away_team", {})]: #one of these will be null!
                    team_id = home_team_id if team_data.get("home_team_id") == home_team_id else away_team_id # So we get the right one!

                    managers = team_data.get("managers", [])    #since there can be multiple managers
                    for manager in managers:
                        cur.execute(
                            '''INSERT INTO Managers (id, name, nickname, date_of_birth, team_id, country)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING''',
                            (manager["id"], manager["name"], manager.get("nickname"), manager["dob"], team_id, manager["country"]["name"])
                        )

                #Matches:
                match_id =                  match["match_id"]
                match_date =                match["match_date"]
                kick_off =                  match["kick_off"]
                home_score =                match["home_score"]
                away_score =                match["away_score"]
                competition_stage_name =    match["competition_stage"]["name"]
                competition_id =            match["competition"]["competition_id"]
                season_id =                 match["season"]["season_id"]
                home_id =                   match["home_team"]["home_team_id"]
                away_id =                   match["away_team"]["away_team_id"]

                if (match_id not in dupeMatchId):
                    cur.execute(
                        '''INSERT INTO Matches (id, date, kick_off, home_score, away_score, competition_stage_name, 
                        stadium_id, competition_id, season_id, home_id, away_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                        (match_id, match_date, kick_off, home_score, away_score, competition_stage_name, stadium_id, competition_id, season_id, home_id, away_id)
                    )
                    dupeMatchId.append(match["match_id"])

        conn.commit()


        # Now insert card, lineup, position, data from before, now we have match_ids to map to
        card_data = zip(match_id_arr, card_player_id_arr, card_type_arr, reason_arr, card_time_arr, period_arr)
        for tuple in card_data:
            cur.execute(
                '''INSERT INTO Cards (match_id, player_id, card_type, reason, time, period)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING''',
                tuple
            )

        lineup_data = zip(lineup_team_id, lineup_match_id)
        for tuple in lineup_data:
            cur.execute(
                '''INSERT INTO Lineups (team_id, match_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                ''',
                tuple
            )

        conn.commit()

        lineup_ids = []
        cur.execute("SELECT id FROM Lineups")
        db_lineup_ids = cur.fetchall()
        for row in db_lineup_ids:
            lineup_ids.append(row[0]) #since in our db, thats lineup_id

        position_data = zip(lineup_ids, position_player_id_arr, position_start_reason_arr, position_end_reason_arr, position_arr, from_date_arr, to_date_arr)
        for tuple in position_data:
            cur.execute(
                '''INSERT INTO PlayerPositions (lineup_id, player_id, start_reason, end_reason, position, from_date, to_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                ''',
                tuple
            )        



        # EVENTS TABLE(S):
        # Note, the user of creating objects for events and using .get with either None or False (depending on database value)
        # This is because a lot of events are missing certain types of data, and we kept getting errors for it.
        # get allows us to make a default value easily
        dupeEventId = []
        dir = "./data/events/"
        for match_id in matchIds:
            file_path = dir + str(match_id) + ".json"

            with open(file_path, "r", encoding="utf-8") as read_file:
                data = json.load(read_file)          
        
                for event in data:
                    if (event["id"] not in dupeEventId):
                        event_id = event.get("id")
                        dupeEventId.append(event.get("id"))

                        index =                 event.get("index")
                        period =                event.get("period")
                        timestamp_str =         event.get("timestamp")
                        timestamp =             datetime.strptime(timestamp_str, "%H:%M:%S.%f").time() if timestamp_str else None  # Ensure it's a time object before inserting
                        minute =                event.get("minute")
                        second =                event.get("second")
                        possession =            event.get("possession")
                        possession_team_id =    event.get("possession_team", None).get("id")
                        event_type =            event.get("type", None).get("name")
                        play_pattern =          event.get("play_pattern", None).get("name")

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
            
                        cur.execute(
                            '''INSERT INTO Events (event_id, match_id, index, period, timestamp, minute, second, possession, 
                            possession_team_id, type, duration, play_pattern, position, location_x, location_y, player_id) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', 
                            (event_id, match_id, index, period, timestamp, minute, second, possession, possession_team_id,
                            event_type, duration, play_pattern, position, location_x, location_y, player_id)
                        )
                        dupeEventId.append(event_id)
                        

                        # Shots:
                        if (event["type"]["name"] == "Shot"):

                            end_location_x =                    event["shot"]["end_location"][0]
                            end_location_y =                    event["shot"]["end_location"][1]
                            outcome =                           event["shot"]["outcome"]["name"]
                            technique =                         event["shot"]["technique"]["name"]
                            body_part =                         event["shot"]["body_part"]["name"]
                            shot_type =                         event["shot"]["type"]["name"]
                            xG =                                event["shot"]["statsbomb_xg"]

                            if "shot" in event and event["shot"].get("first_time") is not None:
                                first_time = event["shot"]["first_time"]
                            else:
                                first_time = False
                            if "shot" in event and event["shot"].get("key_pass_id") is not None:
                                key_pass_id = event["shot"]["key_pass_id"]
                            else:
                                key_pass_id = None

                            cur.execute(
                                '''INSERT INTO Shots (event_id, end_location_x, end_location_y, key_pass_id, outcome, 
                                first_time, technique, body_part, type, xG) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                                (event_id, end_location_x, end_location_y, key_pass_id, outcome, first_time, 
                                technique, body_part, shot_type, xG)
                            )

                        # Passes:
                        # for strings, empty dictionaries are used instead of None, this -> {}
                        if event["type"]["name"] == "Pass":
                            passData = event.get("pass", {}) 
                            length = passData.get("length")
                            angle = passData.get("angle")
                            end_location_x, end_location_y = passData.get("end_location", [None, None])
                            body_part = passData.get("body_part", {}).get("name")
                            outcome = passData.get("outcome", {}).get("name")
                            pass_type = passData.get("type", {}).get("name")
                            height = passData.get("height", None).get("name")
                            switch_status = passData.get("switch", False)
                            cross_status = passData.get("cross", False)
                            cutback = passData.get("cut_back", False)
                            through_ball = passData.get("through_ball", False)
                            assist = passData.get("goal_assist", False)
                            key_pass = passData.get("key_pass", False)
                            aerial_won = passData.get("aerial_won", False)
                            recipient_player_id = passData.get("recipient", {}).get("id")

                            cur.execute(
                                '''
                                INSERT INTO Passes (event_id, length, angle, end_location_x, end_location_y, 
                                body_part, outcome, type, height, switch_status, cross_status, cutback, 
                                through_ball, assist, key_pass, aerial_won, recipient_player_id) 
                                '''
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                (event_id, length, angle, end_location_x, end_location_y, body_part, outcome, pass_type, height, 
                                 switch_status, cross_status, cutback, through_ball, assist, key_pass, aerial_won, recipient_player_id)
                            )


                        # Dribbles
                        if event["type"]["name"] == "Dribble":
                            dribbleData = event.get("dribble", None)
                            name = event["player"]["name"]
                            underPressure = event.get("under_pressure", False)

                            outcome = dribbleData.get("outcome", None).get("name")

                            complete = outcome == "Complete" 
                            
                            cur.execute(
                                '''INSERT INTO Dribbles (event_id, name, under_pressure, complete)
                                VALUES (%s, %s, %s, %s)''',
                                (event_id, name, underPressure, complete)
                            )


                        #Foul Commutted and Cards (as they are associated):
                        if event["type"]["name"] == "Foul Committed":
                            foulData = event.get("foul_committed", {})
                            type_id = event["type"]["id"]
                            counterpress = foulData.get("counterpress", False)
                            card = foulData.get("card", None) 
                            offensive = foulData.get("offensive", False) 
                            card_id = None
                            if "card" in foulData:
                                card_id = foulData["card"]["id"]

                            cur.execute(
                                '''
                                 INSERT INTO FoulCommitted (event_id, foul_id, counterpress, offensive, card_id)
                                 VALUES (%s, %s, %s, %s, %s)
                                ''',
                                (event_id, type_id, counterpress, offensive, card_id)
                            )


                        # Clearance
                        if event["type"]["name"] == "Clearance":
                            out = event.get("out", False)
                            under_pressure = event.get("under_pressure", False)
                            body_part = event["clearance"]["body_part"]["name"] if "clearance" in event else None
                            cur.execute(
                                '''INSERT INTO Clearances (event_id, out, under_pressure, body_part)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, out, under_pressure, body_part)
                            )

                        #BallReceipt
                        if event["type"]["name"] == "Ball Receipt" or event["type"]["name"] == "Ball Receipt*":
                            out = event.get("out", False)
                            under_pressure = event.get("under_pressure", False)
                            cur.execute(
                                '''INSERT INTO BallReceipts (event_id, out, under_pressure)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, out, under_pressure)
                            )

                        #FoulWon
                        if event["type"]["name"] == "Foul Won":
                            under_pressure = event.get("under_pressure", False)
                            defensiveWin = event.get("foul_won", {}).get("defensive", False) #return empty dicitonary instead of None to prevent errors.
                            cur.execute(
                                '''INSERT INTO FoulWon (event_id, under_pressure, foul_won)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, under_pressure, defensiveWin)
                            )


                        #GoalKeeper
                        if event["type"]["name"] == "Goal Keeper":
                            goalkeeperType = event["type"]["name"]
                            outcome = event["goalkeeper"].get("outcome", {}).get("name", None)      #some default values since some are Null (even location at some point)
                            technique = event["goalkeeper"].get("technique", {}).get("name", None)
                            end_location_x, end_location_y = None, None
                            if "location" in event: #look me over, may result in None still?
                                end_location_x, end_location_y = event["location"]
                            goalkeeperPos = event["goalkeeper"].get("position", {}).get("name", None)
                            cur.execute(
                                '''INSERT INTO Goalkeepers (event_id, goalkeeper_type, outcome, technique, end_location_x, end_location_y, goalkeeper_pos)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, goalkeeperType, outcome, technique, end_location_x, end_location_y, goalkeeperPos)
                            )


                        # Carries:
                        if event["type"]["name"] == "Carry":
                            end_location_x, end_location_y = None, None
                            if "location" in event: #look me over, may result in None still?
                                end_location_x, end_location_y = event["location"]                           
                            cur.execute(
                                '''INSERT INTO Carries (event_id, end_location_x, end_location_y)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, end_location_x, end_location_y)
                            )


                        # Dispossessed:
                        if event["type"]["name"] == "Dispossessed":
                            under_pressure = event.get("under_pressure", False)
                            cur.execute(
                                '''INSERT INTO Dispossessed (event_id, under_pressure)
                                VALUES (%s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, under_pressure)
                            )
                        
                        # Blocks:
                        if event["type"]["name"] == "Block": 
                            counterpress = event.get("counterpress", False)
                            cur.execute(
                                '''INSERT INTO Blocks (event_id, counterpress)
                                VALUES (%s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, counterpress)
                            )


                        # 50:50
                        if event["type"]["name"] == "50:50": 
                            under_pressure = event.get("under_pressure", False)
                            outcome = event.get("outcome", None).get("name")
                            cur.execute(
                                '''INSERT INTO Fifties (event_id, under_pressure, outcome)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, under_pressure, outcome)
                            )

                        #Duels:
                        if event["type"]["name"] == "Duel": 
                            under_pressure = event.get("under_pressure", False)
                            duel_type = None
                            outcome = None
                            if "duel" in event:
                                if "type" in event["duel"]:
                                    duel_type = event["duel"]["type"]["name"]
                                if "outcome" in event["duel"]:
                                    outcome = event["duel"]["outcome"]["name"]
                            cur.execute(
                                '''INSERT INTO Duels (event_id, under_pressure, outcome, duel_type)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (event_id) DO NOTHING''',
                                (event_id, under_pressure, outcome, duel_type)
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
