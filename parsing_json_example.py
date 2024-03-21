import json



#ryan's branch test
def main():
    with open("./3005-Course-Project/json/90.json", "r", encoding="utf-8") as read_file:
        data = json.load(read_file)
    print(print(json.dumps(data[0], indent=4)))
    