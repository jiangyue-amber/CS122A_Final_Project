import sys
import mysql.connector
from mysql.connector import Error
import csv
import os

DB_CONFIG = {
    'host': 'localhost',
    'user': 'sql_username',
    'password': 'sql_pass', 
    'database': 'db_name' 
}

def get_db_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def import_data(folder_name):
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return False
    
    try:
        cursor = connection.cursor()
        
        drop_tables = [
            "DROP TABLE IF EXISTS ModelConfiguration",
            "DROP TABLE IF EXISTS CustomizedModel",
            "DROP TABLE IF EXISTS Utilizes",
            "DROP TABLE IF EXISTS BaseModel",
            "DROP TABLE IF EXISTS InternetService",
            "DROP TABLE IF EXISTS AgentClient",
            "DROP TABLE IF EXISTS User"
        ]
        
        for drop_stmt in drop_tables:
            cursor.execute(drop_stmt)
        
        create_tables = [
            """
            CREATE TABLE User (
                uid INT PRIMARY KEY,
                username VARCHAR(255) NOT NULL UNIQUE,
                email VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE AgentClient (
                uid INT PRIMARY KEY,
                card_number BIGINT NOT NULL,
                card_holder VARCHAR(255) NOT NULL,
                expiration_date DATE NOT NULL,
                cvv INT NOT NULL,
                zip INT NOT NULL,
                interests TEXT,
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE InternetService (
                sid INT PRIMARY KEY,
                endpoint VARCHAR(255) NOT NULL,
                provider VARCHAR(255) NOT NULL,
                domain VARCHAR(255)
            )
            """,
            """
            CREATE TABLE BaseModel (
                bmid INT PRIMARY KEY,
                description TEXT,
                creator_uid INT NOT NULL,
                FOREIGN KEY (creator_uid) REFERENCES User(uid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE Utilizes (
                bmid INT,
                sid INT,
                version INT NOT NULL,
                PRIMARY KEY (bmid, sid),
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE CustomizedModel (
                mid INT PRIMARY KEY,
                bmid INT NOT NULL,
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE ModelConfiguration (
                cid INT PRIMARY KEY,
                uid INT NOT NULL,
                mid INT NOT NULL,
                label VARCHAR(255),
                content TEXT,
                duration INT,
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE,
                FOREIGN KEY (mid) REFERENCES CustomizedModel(mid) ON DELETE CASCADE
            )
            """
        ]
        
        for create_stmt in create_tables:
            cursor.execute(create_stmt)
        
        csv_files = {
            'User.csv': 'User',
            'AgentClient.csv': 'AgentClient',
            'InternetService.csv': 'InternetService',
            'BaseModel.csv': 'BaseModel',
            'Utilizes.csv': 'Utilizes',
            'CustomizedModel.csv': 'CustomizedModel',
            'ModelConfiguration.csv': 'ModelConfiguration'
        }
        
        for csv_file, table_name in csv_files.items():
            file_path = os.path.join(folder_name, csv_file)
            
            if not os.path.exists(file_path):
                continue
            
            with open(file_path, 'r') as f:
                csv_reader = csv.reader(f)
                rows = list(csv_reader)
                
                if not rows:
                    continue
                
                col_count = len(rows[0])
                placeholders = ','.join(['%s'] * col_count)
                
                insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                
                for row in rows:
                    processed_row = [None if val == '' or val == 'NULL' else val for val in row]
                    cursor.execute(insert_query, processed_row)
        
        connection.commit()
        print("Success")
        return True
        
    except Error as e:
        print(f"Fail")
        connection.rollback()
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def insertAgentClient(uid, username, email, card_number, card_holder, 
                      expiration_date, cvv, zip_code, interests):
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return False
    
    try:
        cursor = connection.cursor()
        
        user_query = "INSERT INTO User (uid, username, email) VALUES (%s, %s, %s)"
        cursor.execute(user_query, (uid, username, email))
        
        client_query = """
            INSERT INTO AgentClient 
            (uid, card_number, card_holder, expiration_date, cvv, zip, interests) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(client_query, (uid, card_number, card_holder, 
                                      expiration_date, cvv, zip_code, interests))
        
        connection.commit()
        print("Success")
        return True
        
    except Error as e:
        print("Fail")
        connection.rollback()
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def addCustomizedModel(mid, bmid):
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return False
    
    try:
        cursor = connection.cursor()
        
        check_query = "SELECT bmid FROM BaseModel WHERE bmid = %s"
        cursor.execute(check_query, (bmid,))
        
        if cursor.fetchone() is None:
            print("Fail")
            return False
        
        insert_query = "INSERT INTO CustomizedModel (mid, bmid) VALUES (%s, %s)"
        cursor.execute(insert_query, (mid, bmid))
        
        connection.commit()
        print("Success")
        return True
        
    except Error as e:
        print("Fail")
        connection.rollback()
        return False
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()


def deleteBaseModel(bmid):
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return False
    
    try:
        cursor = connection.cursor()
        delete_query = "DELETE FROM BaseModel WHERE bmid = %s"
        cursor.execute(delete_query, (bmid,))
        
        if cursor.rowcount == 0:
            print("Fail")
            return False
        
        connection.commit()
        print("Success")
        return True
        
    except Error as e:
        print("Fail")
        connection.rollback()
        return False
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()

