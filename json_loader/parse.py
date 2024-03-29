import json
from connection import *

def getCompetitionIds(competitions, relevantLeagues, relevantSeasons):
    relevantCompetitions = []

    for c in competitions:
        if (c["competition_name"] in relevantLeagues) and (c["season_name"] in relevantSeasons):
            relevantCompetitions.append((c["competition_id"], c["season_id"]))

    return relevantCompetitions

def getMatchInfo(relevantIdPairs):
    matchInfo = []

    for i in range(len(relevantIdPairs)):
        cId = relevantIdPairs[i][0]
        sId = relevantIdPairs[i][1]
        file_path = "./data/matches/{}/{}.json".format(cId, sId)
        with open(file_path, "r", encoding="utf-8") as read_file:
            matchInfo.append(json.load(read_file))

    return matchInfo

def getEventInfo(matchIds):
    eventInfo = []

    for match in matchIds:
        file_path = "./data/events/{}.json".format(match)
        with open(file_path, "r", encoding="utf-8") as read_file:
            currEvents = json.load(read_file)

            eventInfo.append(currEvents)

    return eventInfo

def getLineupInfo(matchIds):
    lineupInfo = []

    for match in matchIds:
        file_path = "./data/lineups/{}.json".format(match)
        with open(file_path, "r", encoding="utf-8") as read_file:
            currLineups = json.load(read_file)

            lineupInfo.append(currLineups)

    return lineupInfo







    # BELOW: OLD PARSING TESTS:
    # relevantIdPairs = getCompetitionIds(competitionData, relevantLeagues, relevantSeasons)

    # # relevant match info
    # matchInfo = getMatchInfo(relevantIdPairs)

    # for index in matchInfo:
    #     for match in index:
    #         matchIds.append(match["match_id"])

    # # relevant event info
    # eventInfo = getEventInfo(matchIds) 
    # # with open("event.txt", "w", encoding="utf-8") as file:
    # #     for event in eventInfo:
    # #         file.write(str(event) + '\n')

    # # relevant lineup info
    # lineupInfo = getLineupInfo(matchIds)