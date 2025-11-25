import mysql.connector
import sys

def main():
    mydb = mysql.connector.connect(
    host="localhost",
    user="test",
    password="password",
    )

if __name__ == "__main__":
    main()