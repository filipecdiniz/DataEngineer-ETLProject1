import json
import sys
import os

def main():

    Tasks = [
        #Truncate data
        {
            "name": "TruncateDimensions",
            "type": "RunQuery",
            "active": True,
            "DestConnection": 'ETLDatabase'
        },

        #Load From excel
        {
            "name": "Gama_InputDeparaDreContabil",
            "type": "LoadFromExcel",
            "active": True,
            "DestTable": "dim_deparadrecontabil",
            "SheetName": "InputDeparaDreContabil",
            "DestConnection": "ETLDatabase"
        },
        #Load character From API
        {
            "name": "dim_characters",
            "type": "LoadFromAPI",
            "active": True,
            "SourceConnection": "RickAndMortyAPI",
            "sufixURL": "character",
            "key": "results",
            "columns": ["id", "name", "status", "gender"],
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_characters"
        },
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
