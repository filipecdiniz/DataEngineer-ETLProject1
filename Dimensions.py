import json

def main():

    Tasks = [
        #Truncate data
        {
            "name": "TruncateDimensions",
            "type": "RunQuery",
            "active": False,
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
        #Load From Database
        {
            "name": "dim_employee",
            "type": "LoadFromDatabase",
            "active": True,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_employee"
        },
        #Load customer From Database
        {
            "name": "dim_customer",
            "type": "LoadFromDatabase",
            "active": True,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_customer"
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

    with open('C:\Projects\python\ETLProject-1\connection\connection.txt', 'r') as file:
        conn = json.loads(file.read())
        Configs["connections"] = conn['connections']

    from ETL import ETL
    ETL(Configs)

main()