import json
import sys
import os

def main():

    Tasks = [
        #Truncate data
        {
            "name": "TruncateDimensions",
            "type": "RunQuery",
            "active": False,
            "DestConnection": 'ETLDatabase'
        },
        #Load employees From Database
        {
            "name": "dim_employee",
            "type": "LoadFromDatabase",
            "active": False,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_employee"
        },
        #Load customers From Database
        {
            "name": "dim_customer",
            "type": "LoadFromDatabase",
            "active": False,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_customer"
        },
        #Load products From Database
        {
            "name": "dim_product",
            "type": "LoadFromDatabase",
            "active": False,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_product"
        },
        #Load department From Database
        {
            "name": "dim_department",
            "type": "LoadFromDatabase",
            "active": False,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_department"
        },
        #Load shift From Database
        {
            "name": "dim_shift",
            "type": "LoadFromDatabase",
            "active": False,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_shift"
        },
        #Load territory From Database
        {
            "name": "dim_territory",
            "type": "LoadFromDatabase",
            "active": False,
            "SourceConnection": "AdventureWorks2022",
            "DestConnection": "ETLDatabase",
            "DestTable": "dim_territory"
        },
    ]

    Configs = {}
    Configs["Tasks"] = Tasks
                                                    # PostgreSQL Jar Path                    ||                      # SQL Server Jar Path
    Configs['jars'] = '/home/filipe.diniz.spark1/opt/jars/postgresql-42.2.27.jar, /home/filipe.diniz.spark1/opt/jars/mssql-jdbc-12.10.0.jre11.jar'

    with open('C:\Projects\python\ETLProject-1\src\connection\connection.txt', 'r') as file:
        conn = json.loads(file.read())
        Configs["connections"] = conn['connections']

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)

    from ETL import ETL
    
    ETL(Configs)

main()
