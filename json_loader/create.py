import json
import psycopg
from connection import *


def createTables():

    try:
        # holds a connection object to connect to the database
        conn = psycopg.connect(    
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
        cur = conn.cursor()  

        cur.execute("""
    
            CREATE TABLE IF NOT EXISTS Students (
                student_id			SERIAL		PRIMARY KEY,
                first_name			TEXT		NOT NULL,
                last_name			TEXT		NOT NULL,
                email				TEXT		UNIQUE NOT NULL,
                enrollment_date		DATE		
            );
                    
        """)

        conn.commit()

    except Exception as error:
        print(error)
    finally: #To ensure that the connection object and cursor always close:
        # close the cursor
        cur.close()
        # close the connection, once we are finished with it.
        conn.close() 





def dropTables():

    try:
        # holds a connection object to connect to the database
        conn = psycopg.connect(    
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
        cur = conn.cursor()  

        cur.execute("""
    
            DROP TABLE IF EXISTS Students; 
                    
        """)

        conn.commit()

    except Exception as error:
        print(error)
    finally: #To ensure that the connection object and cursor always close:
        # close the cursor
        cur.close()
        # close the connection, once we are finished with it.
        conn.close() 