import json

# get match id from parsed season json
def getMatchId(season_json):
    return season_json['match_id']

# get all match ids from a parsed season json
def getAllMatchId(season_json):
    matchIds = []
    for i in range(len(season_json)):
        matchIds.append(getMatchId(season_json[i]))
    return matchIds


#ryan's branch test
def main():
    # parse json file into python usable object
    with open("./json/matches/90.json", "r", encoding="utf-8") as read_file:
        data = json.load(read_file)
    
    print(getAllMatchId(data))
   

main()