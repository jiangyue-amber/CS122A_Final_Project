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

#function 1: import data
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

# function 2: insert agent client
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

#function 3: add a customized model
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

#function 4: delete a base model
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



# function 5: list internet service
def listInternetService(bmid):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            print("Fail")
            return
        
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT s.sid, s.endpoint, s.provider
        FROM InternetService s
        JOIN ModelServices ms ON s.sid = ms.sid
        WHERE ms.bmid = %s
        ORDER BY s.provider ASC
        """
        
        cursor.execute(query, (bmid,))
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]}")
        
    except Exception as e:
        print("Fail")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# function 6: count customized model
def countCustomizedModel(*bmids):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            print("Fail")
            return
        
        cursor = conn.cursor()
        
        bmid_list = list(bmids)
        placeholders = ','.join(['%s'] * len(bmid_list))
        
        query = f"""
        SELECT bm.bmid, bm.description, COUNT(cm.mid) as customizedModelCount
        FROM BaseModel bm
        LEFT JOIN CustomizedModel cm ON bm.bmid = cm.bmid
        WHERE bm.bmid IN ({placeholders})
        GROUP BY bm.bmid, bm.description
        ORDER BY bm.bmid ASC
        """
        
        cursor.execute(query, bmid_list)
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]}")
        
    except Exception as e:
        print("Fail")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# function 7: find top-n longest duration configuration
def topNDurationConfig(uid, N):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            print("Fail")
            return
        
        cursor = conn.cursor()
        
        query = """
        SELECT c.client_uid, c.cid, c.labels, c.content, mc.duration
        FROM Configuration c
        JOIN ModelConfigurations mc ON c.cid = mc.cid
        WHERE c.client_uid = %s
        ORDER BY mc.duration DESC
        LIMIT %s
        """
        
        cursor.execute(query, (uid, N))
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}")
        
    except Exception as e:
        print("Fail")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# function 8: keyword search
def listBaseModelKeyWord(keyword):
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            print("Fail")
            return
        
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT bm.bmid, s.sid, s.provider, l.domain
        FROM BaseModel bm
        JOIN ModelServices ms ON bm.bmid = ms.bmid
        JOIN InternetService s ON ms.sid = s.sid
        JOIN LLMService l ON s.sid = l.sid
        WHERE l.domain LIKE %s
        ORDER BY bm.bmid ASC
        LIMIT 5
        """
        
        cursor.execute(query, (f'%{keyword}%',))
        results = cursor.fetchall()
        
        for row in results:
            print(f"{row[0]},{row[1]},{row[2]},{row[3]}")
        
    except Exception as e:
        print("Fail")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()