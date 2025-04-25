import json
import sys
import os

def main():

    Tasks = [
        #Truncate data
        {
            "name": "TruncateFacts",
            "type": "RunQuery",
            "active": True,
            "DestConnection": 'ETLDatabase'
        },
        #Load sales From Database
        {
            "name": "fact_sales",
            "type": "LoadFromDatabase",
            "active": True,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "fact_sales"
        }
    ]

    Configs = {}
    Configs["Tasks"] = Tasks

    with open('C:\Projects\python\ETLProject-1\src\connection\connection.txt', 'r') as file:
        conn = json.loads(file.read())
        Configs["connections"] = conn['connections']

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)

    from ETL import ETL
    
    ETL(Configs)

main()
