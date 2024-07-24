import pymysql
from datetime import datetime

class IRDALogData:
    def __init__(self, description: str, completed: bool, error_if_exists: str, job_started: datetime, job_end: datetime):
        self.description = description
        self.completed = completed
        self.error_if_exists = error_if_exists
        self.job_started = job_started
        self.job_end = job_end

    def load_logs_data(self):
        host = 'hostname'
        user = "user"
        database = "amfi_logs"
        password = "password"

        db_config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
        }
        try:
            connection = pymysql.connect(**db_config)
            print("Log database connection successful...")
        except pymysql.Error as err:
            self.error_if_exists = str(err)
            return

        try:
            with connection.cursor() as cursor:
                insert_query = """
                    INSERT INTO irdai (JobStarted, JobEnd, Description, error_if_exists, completed)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                cursor.execute(insert_query, (self.job_started, self.job_end, self.description, self.error_if_exists, self.completed))
                connection.commit()
                print("Successfully loaded Logs data")
        except pymysql.Error as err:
            print("Error loading data into MySQL table:", err)
        finally:
            connection.close()