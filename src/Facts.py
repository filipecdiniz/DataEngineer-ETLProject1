import json

def main():

    Tasks = [
        #Truncate data
        {
            "name": "TruncateFacts",
            "type": "RunQuery",
            "active": True,
            "DestConnection": 'ETLDatabase'
        }
    ]

    Configs = {}
    Configs["Tasks"] = Tasks

    with open('C:\Projects\python\ETLProject-1\connection\connection.txt', 'r') as file:
        conn = json.loads(file.read())
        Configs["connections"] = conn['connections']

    from ETL import ETL
    ETL(Configs)
main()