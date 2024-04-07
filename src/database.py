import logging
import pymysql


def load_dataframe_to_mysql(df, table_name, db_config):
    """
    Loads a pandas DataFrame into a MySQL database table, handling existing records.

    Args:
        df (pandas.DataFrame): The DataFrame to load.
        table_name (str): The name of the table in the MySQL database.
        db_config (dict): A dictionary containing MySQL connection details.
            - host (str): The hostname or IP address of the MySQL server.
            - user (str): The username to connect to the database.
            - password (str): The password for the user.
            - database (str): The name of the database to connect to.
    """
    try:
        connection = pymysql.connect(**db_config)
        logging.info("Connection to Database succefull ..")
    except pymysql.Error as err:
        logging.err("Error connecting to MySQL database:", err)
        return

    try:
        cursor = connection.cursor()
        data = df.to_records(index=False).tolist()
        
        insert_query = f"""
            REPLACE INTO {table_name} (absorbedagent, agentid, agentname, bntagentid, district, dpid, ingestiontimestamp, insurancetype, insurer, irdaurn, licenceno, mobile_no, phoneno, pincode, state, validfrom, validto)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

        cursor.executemany(insert_query, data)
        connection.commit()
        logging.info(f"Successfully loaded {len(data)} rows into table '{table_name}'.")

    except pymysql.Error as err:
        logging.error("Error loading data into MySQL table:", err)
    finally:
        cursor.close()
        connection.close()
