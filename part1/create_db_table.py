import mysql.connector
from configs import mysql_config

def create_database_and_table():
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS neo_data")
        cursor.execute("USE neo_data")
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS vp_aggregated_athlete_data (
            sport VARCHAR(255) NOT NULL,
            medal VARCHAR(10),
            gender VARCHAR(10) NOT NULL,
            country_noc VARCHAR(255) NOT NULL,
            average_height DOUBLE NOT NULL,
            average_weight DOUBLE NOT NULL,
            calculation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (sport, medal, gender, country_noc)
        )
        """
        cursor.execute(create_table_query)
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("Finished.")

create_database_and_table()
