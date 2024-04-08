# 3005-Course-Project

Project Members (2):

    - Ryan Tran
    - Tim Xia


In this repo, you should find the following:

    1. The exported database called "dbexport.sql"

    2. Source code used to load the json_data into the database, in the json_loader folder.

    3. "queries.py" which is the auto-grader script with SQL queries in the right places.


Data loading (json_loader):

    1. If you want to run this for yourself, make sure you have python3 and psycopg3 installed first.

    2. If you insist on doing this, then you must have a database and server with matching information in the connection.py file:

        hostname = 'localhost'
        database = 'FinalProject2' 
        username = 'postgres'
        pwd = 'postgres'
        port_id = 5433

    3. Then run database.py with "python ./database.py"


Happy Grading!

