import pandas as pd
import mysql.connector
from datetime import datetime

class MySQLConnector:
    def __init__(self, host, database, user, password):
        """
        Initializes a passionate connection to the MySQL database, eager to bridge data and dreams!
        """
        self.host = host
        self.database = database
        self.user = user 
        self.password = password
        self.connection = None  # Awaiting the spark of connection

    def connect(self):
        """
        Ignites a connection to the MySQL database, fueled by anticipation and excitement!
        """
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print("Connected to MySQL database Successfully...")
        except mysql.connector.Error as e:
            print(f"Error connecting to MySQL database: {e}")  # Handle setbacks with grace

    def insert_or_update_dataframe(self, dataframe, table_name, primary_key_column):
        try:
            cursor = self.connection.cursor()
            select_query = f"SELECT * FROM {table_name}"
            cursor.execute(select_query)
            existing_data = cursor.fetchall()

            if table_name == "amfi":
                columns = ['arn', 'holder_name', 'address', 'city', 'IngestionTimeStamp', 'pin', 'email', 'telephone_r', 'telephone_o', 'arn_valid_till', 'arn_valid_from', 'kyd_compliant', 'EUIN']
                existing_df = pd.DataFrame(existing_data, columns=columns)
                existing_df['arn_valid_from'] = pd.to_datetime(
                            existing_df["arn_valid_from"], 
                            errors='coerce', 
                            format='%Y-%m-%d %H:%M:%S'
                            )
                existing_df['arn_valid_till'] = pd.to_datetime(
                            existing_df["arn_valid_till"], 
                            errors='coerce', 
                            format='%Y-%m-%d %H:%M:%S'
                                )
            else:
                existing_df = pd.DataFrame(existing_data, columns=dataframe.columns)
                
            # Subtract exact matches from the original DataFrame
            dataframe = dataframe.merge(existing_df, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

            cols = tuple([col for col in dataframe.columns])
            print("Loading Data in Database .....")
            for index, row in dataframe.iterrows():
                values = [row[col] for col in cols]

                # check if this record exists in database if exists overwrite
                selectexpr = f"SELECT * FROM {table_name} WHERE {primary_key_column} = '{dataframe[primary_key_column].values[index]}'"
                exists = cursor.execute(selectexpr)
                result = cursor.fetchone()
                if result is not None:
                    # update the values
                    set_clause = ", ".join([f"{col}='{row[col]}'" for col in dataframe.columns])
                    query = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key_column} = '{dataframe[primary_key_column].values[index]}'"
                else:
                    # Construct the insert query
                    query = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES {tuple(values)}"
                cursor.execute(query)
                self.connection.commit()
            print(f"Data loaded Successfully {table_name}")
        except mysql.connector.Error as e:
            print(f"Error inserting or updating data: {e}")

    def disconnect(self):
        """
        Releases the connection with tenderness, bidding farewell until our paths cross again.
        """
        try:
            self.connection.close()
            print("MySQL connection gracefully released. Until we meet again, dear data.")
        except mysql.connector.Error as e:
            print(f"Error disconnecting from MySQL database: {e}")  # Handle parting pangs with composure
