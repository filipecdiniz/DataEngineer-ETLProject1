import pandas as pd
import psycopg2
import pyodbc
import requests
from pyspark.sql import SparkSession

class ETL:
    def __init__(self, Configs):
        self.connections = Configs['connections']
        self.jars = Configs['jars']
        self.BuildSparkSession()
        print(self.SparkSession)
        for task in Configs['Tasks']:
            if (task['type'] == 'RunQuery'):
                self.RunQuery(task)
            elif (task['type'] == 'LoadFromExcel'):
                # self.LoadFromExcel(task)
                self.LoadFromExcel(task)
                # self.RunQuery()
            elif (task['type'] == 'LoadFromDatabase'):
                # self.LoadFromExcel(task)
                self.LoadFromDatabase(task)
                # self.RunQuery()
            elif (task['type'] == 'LoadFromAPI'):
                self.LoadFromAPI(task)
        
    def BuildSparkSession(self):
        spark = SparkSession.builder \
            .appName('Spark-App') \
            .master('spark://192.168.56.101:7077') \
            .config('spark.jars', self.jars) \
            .getOrCreate()
        print('Spark Session created!')
        self.SparkSession = spark

    def RunQuery(self, task):
        if(task['active'] == True):
            with open(f'C:\Projects\python\ETLProject-1\src\queries\{task['name']}\{task['name']}.txt') as query:
                for conn in self.connections:
                    if(conn["name"] == task["DestConnection"]):
                        connection = psycopg2.connect(f'postgresql://{conn['user']}:{conn['password']}@{conn['host']}:{conn['port']}/{conn['database']}')
                        cursor = connection.cursor()

                        cursor.execute(query.read())

                        connection.commit()
                        cursor.close()
                        connection.close()

        elif(task['active'] == False):
            return

    def LoadFromExcel(self, task):
        if(task['active'] == True):
            
            data = pd.read_excel(f'C:\Projects\python\ETLProject-1\src\datasets\{task['name']}.xlsx', sheet_name=task['SheetName'])
            destConnectionConfig = next(conn for conn in self.connections if conn['name'] == task['DestConnection'])
            connection = psycopg2.connect(f'postgresql://{destConnectionConfig['user']}:{destConnectionConfig['password']}@{destConnectionConfig['host']}:{destConnectionConfig['port']}/{destConnectionConfig['database']}')
            cursor = connection.cursor()

            # Study this part!
            columns = ", ".join(data.columns)
            placeholders = ", ".join(["%s"] * len(data.columns))
            
            # Insert any row of table.
            for _, row in data.iterrows():
                values = [row[col] for col in data.columns]
                cursor.execute(
                    f"INSERT INTO {task['DestTable']} ({columns}) VALUES ({placeholders})",
                    values
                )
            
            connection.commit()
            cursor.close()
            connection.close()
            
            return
        elif(task['active'] == False):
            return
        
    def LoadFromDatabase(self, task, spark_session):
        if(task['active'] == True):
            with open(f'C:\Projects\python\ETLProject-1\src\queries\{task['name']}\{task['name']}.txt') as query:
                destConnection = {}
                sourceConnection = {}

                sourceConnectionConfig = next(conn for conn in self.connections if conn['name'] == task['SourceConnection'])
                destConnectionConfig = next(conn for conn in self.connections if conn['name'] == task['DestConnection'])

                sourceConnection = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sourceConnectionConfig['host']};DATABASE={sourceConnectionConfig['database']};UID={sourceConnectionConfig['user']};PWD={sourceConnectionConfig['password']}')
                destConnection = psycopg2.connect(f'postgresql://{destConnectionConfig['user']}:{destConnectionConfig['password']}@{destConnectionConfig['host']}:{destConnectionConfig['port']}/{destConnectionConfig['database']}')

                destCursor = destConnection.cursor()
                sourceCursor = sourceConnection.cursor()

                
                sourceCursor.execute(query.read())
                res = sourceCursor.fetchall()

                # sourceCursor.description is a List of Tuples with all information about the query, the first item in the Tuple is the name of the column. 
                # For any Tuple, in sourceCursor.description you're asking for the first item in tuple (the name of the column).
                columns = ", ".join([col[0] for col in sourceCursor.description])

                placeholders = ", ".join(["%s"] * len(sourceCursor.description))
            
                # Prepare insert statement
                insert_query = f"INSERT INTO {task['DestTable']} ({columns}) VALUES ({placeholders})"


                for row in res:
                    destCursor.execute(insert_query, row)

                destConnection.commit()
                
                destCursor.close()
                destConnection.close()

                sourceCursor.close()
                sourceConnection.close()

                return
        else:
            return
        
    def LoadFromAPI(self, task):
        if(task['active'] == True):
            # Get the connection configs
            try:
                api_configs = next(conn for conn in self.connections if conn['name'] == task['SourceConnection'])
                destConnectionConfig = next(conn for conn in self.connections if conn['name'] == task['DestConnection'])

                # Opens the dest database connection.
                destConnection = psycopg2.connect(f'postgresql://{destConnectionConfig['user']}:{destConnectionConfig['password']}@{destConnectionConfig['host']}:{destConnectionConfig['port']}/{destConnectionConfig['database']}')
                destCursor = destConnection.cursor()

                request = requests.get(f'{api_configs['url']}/{task['sufixURL']}').json()
                if(task['key']):
                    data = request[task['key']]
                    if(task['columns']):

                        charactersList = [
                            # Returns just the selected columns.
                            {k: item[k] for k in task['columns'] if k in item}
                            for item in data
                        ]
                        columns_string = ", ".join(task['columns'])

                        for character in charactersList:
                            destCursor.execute(f"""
                                INSERT INTO {task['DestTable']} ({columns_string})
                                VALUES {tuple(character[col] for col in task['columns'])};
                            """)

                        destConnection.commit()
                        destCursor.close()
                        destConnection.close()
                    else:
                        print(data)
                else: 
                    print('aqui!')
            finally:
                return
            