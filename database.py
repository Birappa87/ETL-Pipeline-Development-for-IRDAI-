import mysql.connector
import pandas as pd

class MySQLConnector:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            print("Connected to MySQL database")
        except mysql.connector.Error as e:
            print("Error connecting to MySQL database:", e)

    def insert_dataframe(self, dataframe, table_name):
        try:
            # Create table if it doesn't exist
            self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                                f"{', '.join([f'{col} VARCHAR(255)' for col in dataframe.columns])}"
                                f")")

            # Insert DataFrame records
            for _, row in dataframe.iterrows():
                cols = ', '.join(row.keys())
                vals = ', '.join([f"'{val}'" for val in row.values])
                sql_query = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
                self.cursor.execute(sql_query)

            # Commit changes
            self.connection.commit()
            print("Data inserted successfully into MySQL table:", table_name)
        except mysql.connector.Error as e:
            print("Error inserting data into MySQL table:", e)

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.cursor.close()
            self.connection.close()
            print("MySQL connection is closed")

# Example usage:
if __name__ == "__main__":
    # MySQL database connection parameters
    host = 'your_host'
    database = 'your_database'
    user = 'your_username'
    password = 'your_password'

    # Create an instance of MySQLConnector
    mysql_connector = MySQLConnector(host, database, user, password)

    # Connect to the MySQL database
    mysql_connector.connect()

    # Example DataFrame
    data = {
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    }
    df = pd.DataFrame(data)

    # Define the table name
    table_name = 'your_table'

    # Insert DataFrame into the MySQL table
    mysql_connector.insert_dataframe(df, table_name)

    # Disconnect from the MySQL database
    mysql_connector.disconnect()
