import json

#ryan's branch test
def main():
    with open("./json/90.json", "r", encoding="utf-8") as read_file:
        data = json.load(read_file)
    print(data[0])
main()