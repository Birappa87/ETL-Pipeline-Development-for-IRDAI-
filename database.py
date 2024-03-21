import pandas as pd
from sqlalchemy import create_engine

class MySQLConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.engine = None

    def connect(self):
        try:
            # Create SQLAlchemy engine
            self.engine = create_engine(f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}")
            print("Connected to MySQL database")
        except Exception as e:
            print("Error connecting to MySQL database:", e)

    def insert_dataframe(self, dataframe, table_name):
        try:
            # Insert DataFrame into MySQL table
            dataframe.fillna(0, inplace=True)
            dataframe.to_sql(table_name, self.engine, if_exists='replace', index=False)
            print("Data inserted successfully into MySQL table:", table_name)
        except Exception as e:
            print("Error inserting data into MySQL table:", e)

    def disconnect(self):
        try:
            # No need to explicitly close the engine with SQLAlchemy
            print("MySQL connection is closed")
        except Exception as e:
            print("Error disconnecting from MySQL database:", e)
