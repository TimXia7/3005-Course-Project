import json
import psycopg

from create import *
from parse import *

# Data to allow the script to find data specific to the project statement:
relevantLeagues = ["La Liga", "Premier League"]
relevantSeasons = ["2020/2021", "2019/2020", "2018/2019", "2003/2004"]


# json from competitions.json
competitionData = []

# json from the respective data folders. Only from relevent seasons, each of the 4 entries are json data of a season.
# This will be very important later.
eventInfo = []
lineupInfo = []
matchInfo = []
matchIds = []

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

    #open the competitions.json folder to find the id of the relevant season ids
    with open("./data/competitions.json", "r", encoding="utf-8") as read_file:
        competitionData = json.load(read_file)
        for competition in competitionData:
            competitions_competition_id.append(competition["competition_id"])
            competitions_season_id.append(competition["season_id"])
            competitions_competition_name.append(competition["competition_name"])
            competitions_country.append(competition["country_name"])
            competitions_competition_gender.append(competition["competition_gender"])
            competitions_competition_youth.append(competition["competition_youth"])
            competitions_competition_international.append(competition["competition_international"])
            competitions_season_name.append(competition["season_name"])

    dropTables()
    createTables()
    




main()
