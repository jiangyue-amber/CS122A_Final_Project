import sys
import mysql.connector
from mysql.connector import Error
import csv
import os
from datetime import datetime


DB_CONFIG = {
     'host': 'localhost',
     'user': 'test',
     'password': 'password',
     'database': 'cs122a'
}


def get_db_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# ------------------ Function 1: Import data ------------------
def import_data(folder_name):
    conn = get_db_connection()
    if not conn:
        return False

    try:
        cursor = conn.cursor()

        drop_tables = [
            "DROP TABLE IF EXISTS ModelConfigurations",
            "DROP TABLE IF EXISTS ModelServices",
            "DROP TABLE IF EXISTS CustomizedModel",
            "DROP TABLE IF EXISTS BaseModel",
            "DROP TABLE IF EXISTS Configuration",
            "DROP TABLE IF EXISTS LLMService",
            "DROP TABLE IF EXISTS DataStorage",
            "DROP TABLE IF EXISTS InternetService",
            "DROP TABLE IF EXISTS AgentClient",
            "DROP TABLE IF EXISTS AgentCreator",
            "DROP TABLE IF EXISTS User"
        ]
        for stmt in drop_tables:
            cursor.execute(stmt)

        # Create tables according to autograder spec
        cursor.execute("""
        CREATE TABLE User (
            uid INT PRIMARY KEY,
            email TEXT NOT NULL,
            username TEXT NOT NULL
        )
        """)

        cursor.execute("""
        CREATE TABLE AgentCreator (
            uid INT PRIMARY KEY,
            bio TEXT,
            payout TEXT,
            FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE AgentClient (
            uid INT PRIMARY KEY,
            interests TEXT NOT NULL,
            cardholder TEXT NOT NULL,
            expire DATE NOT NULL,
            cardno BIGINT NOT NULL,
            cvv INT NOT NULL,
            zip INT NOT NULL,
            FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE BaseModel (
            bmid INT PRIMARY KEY,
            creator_uid INT NOT NULL,
            description TEXT NOT NULL,
            FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE CustomizedModel (
            bmid INT,
            mid INT NOT NULL,
            PRIMARY KEY (bmid, mid),
            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE Configuration (
            cid INT PRIMARY KEY,
            client_uid INT NOT NULL,
            content TEXT NOT NULL,
            labels TEXT NOT NULL,
            FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE InternetService (
            sid INT PRIMARY KEY,
            provider TEXT NOT NULL,
            endpoints TEXT NOT NULL
        )
        """)

        cursor.execute("""
        CREATE TABLE LLMService (
            sid INT PRIMARY KEY,
            domain TEXT,
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE DataStorage (
            sid INT PRIMARY KEY,
            type TEXT,
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE ModelServices (
            bmid INT NOT NULL,
            sid INT NOT NULL,
            version INT NOT NULL,
            PRIMARY KEY (bmid, sid),
            FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
            FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
        )
        """)

        cursor.execute("""
        CREATE TABLE ModelConfigurations (
            bmid INT NOT NULL,
            mid INT NOT NULL,
            cid INT NOT NULL,
            duration INT NOT NULL,
            PRIMARY KEY (bmid, mid, cid),
            FOREIGN KEY (bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
            FOREIGN KEY (cid) REFERENCES Configuration(cid) ON DELETE CASCADE
        )
        """)

        # CSV import
        csv_tables = [
            "User", "AgentCreator", "AgentClient", "BaseModel", "CustomizedModel",
            "Configuration", "InternetService", "LLMService", "DataStorage",
            "ModelServices", "ModelConfigurations"
        ]

        for table in csv_tables:
            csv_file = os.path.join(folder_name, f"{table}.csv")
            if not os.path.exists(csv_file):
                continue
            with open(csv_file, newline='') as f:
                reader = csv.reader(f)
                next(reader)
                rows = list(reader)
                if not rows:
                    continue
                placeholders = ','.join(['%s'] * len(rows[0]))
                insert_query = f"INSERT INTO {table} VALUES ({placeholders})"
                processed = []
                for r in rows:
                    new_row = []
                    for val in r:
                        if val.upper() == 'NULL' or val == '':
                            new_row.append(None)
                        elif val.isdigit():
                            new_row.append(int(val))
                        elif '-' in val:
                            try:
                                new_row.append(datetime.strptime(val, '%Y-%m-%d').date())
                            except:
                                new_row.append(val)
                        else:
                            new_row.append(val)
                    processed.append(tuple(new_row))
                cursor.executemany(insert_query, processed)

        conn.commit()
        print("Success")
        return True

    except Error as e:
        print(f"Fail: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 2: Insert AgentClient ------------------
def insertAgentClient(uid, username, email, interests, cardholder, expire, cardno, cvv, zip_code):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT uid FROM User WHERE uid=%s", (uid,))
        if cursor.fetchone():
            print("Fail")
            return False

        cursor.execute("SELECT uid FROM AgentClient WHERE uid=%s", (uid,))
        if cursor.fetchone():
            print("Fail")
            return False
        cursor.execute("INSERT INTO User (uid, email, username) VALUES (%s, %s, %s)", (uid, email, username))
        cursor.execute("""
            INSERT INTO AgentClient
            (uid, interests, cardholder, expire, cardno, cvv, zip)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (uid, interests, cardholder, expire, cardno, cvv, zip_code))
        conn.commit()
        print("Success")
        return True
    except Error as e:
        print(f"Fail: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 3: Add Customized Model ------------------
def addCustomizedModel(mid, bmid):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT bmid FROM BaseModel WHERE bmid=%s", (bmid,))
        if not cursor.fetchone():
            print("Fail")
            return False
        cursor.execute("INSERT INTO CustomizedModel (bmid, mid) VALUES (%s, %s)", (bmid, mid))
        conn.commit()
        print("Success")
        return True
    except Error as e:
        print(f"Fail: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 4: Delete BaseModel ------------------
def deleteBaseModel(bmid):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM BaseModel WHERE bmid=%s", (bmid,))
        if cursor.rowcount == 0:
            print("Fail")
            return False
        conn.commit()
        print("Success")
        return True
    except Error as e:
        print(f"Fail: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 5: List Internet Services ------------------
def listInternetService(bmid):
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT s.sid, s.endpoints, s.provider
            FROM InternetService s
            JOIN ModelServices ms ON s.sid = ms.sid
            WHERE ms.bmid=%s
            ORDER BY s.provider
        """, (bmid,))
        for row in cursor.fetchall():
            print(f"{row[0]},{row[1]},{row[2]}")
    except Error as e:
        print(f"Fail: {e}")
    finally:
        if cursor and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 6: Count Customized Models ------------------
def countCustomizedModel(*bmids):
    if not bmids:
        return
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        placeholders = ','.join(['%s']*len(bmids))
        query = f"""
            SELECT bm.bmid, bm.description, COUNT(cm.mid)
            FROM BaseModel bm
            LEFT JOIN CustomizedModel cm ON bm.bmid=cm.bmid
            WHERE bm.bmid IN ({placeholders})
            GROUP BY bm.bmid, bm.description
            ORDER BY bm.bmid
        """
        cursor.execute(query, bmids)
        for row in cursor.fetchall():
            print(f"{row[0]},{row[1]},{row[2]}")
    except Error as e:
        print(f"Fail: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 7: Top-N Duration Configuration ------------------
def topNDurationConfig(uid, N):
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT client_uid, c.cid, labels, content, duration
            FROM ModelConfigurations mc
            JOIN Configuration c ON mc.cid=c.cid
            WHERE c.client_uid=%s
            ORDER BY duration DESC
            LIMIT %s
        """, (uid, N))
        for row in cursor.fetchall():
            print(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]}")
    except Error as e:
        print(f"Fail: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 8: Keyword Search ------------------
def listBaseModelKeyWord(keyword):
    conn = get_db_connection()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT bm.bmid, s.sid, s.provider, l.domain
            FROM BaseModel bm
            JOIN ModelServices ms ON bm.bmid=ms.bmid
            JOIN InternetService s ON ms.sid=s.sid
            JOIN LLMService l ON s.sid=l.sid
            WHERE l.domain LIKE %s
            ORDER BY bm.bmid
            LIMIT 5
        """, (f"%{keyword}%",))
        for row in cursor.fetchall():
            print(f"{row[0]},{row[1]},{row[2]},{row[3]}")
    except Error as e:
        print(f"Fail: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ------------------ Function 9: NL2SQL------------------

def printNL2SQLresult():
    with open("NL2SQL.csv", "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            clean = [col.strip() for col in row]
            print(",".join(clean))

# ------------------ Main ------------------
if __name__ == "__main__":
    if len(sys.argv)<2:
        print("No function specified")
        sys.exit(1)
    func_name = sys.argv[1]
    args = sys.argv[2:]
    func_map = {
        "import": import_data,
        "insertAgentClient": insertAgentClient,
        "addCustomizedModel": addCustomizedModel,
        "deleteBaseModel": deleteBaseModel,
        "listInternetService": listInternetService,
        "countCustomizedModel": countCustomizedModel,
        "topNDurationConfig": topNDurationConfig,
        "listBaseModelKeyWord": listBaseModelKeyWord,
        "printNL2SQLresult": printNL2SQLresult
    }
    if func_name not in func_map:
        print(f"Function '{func_name}' not found")
        sys.exit(1)
    # Parse numeric arguments
    def parse_arg(a):
        try: return int(a)
        except: return a
    parsed_args = [parse_arg(a) for a in args]
    func_map[func_name](*parsed_args)