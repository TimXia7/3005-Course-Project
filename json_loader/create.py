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
    
            CREATE TABLE Stadiums (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                country VARCHAR(255)
            );

            CREATE TABLE Competitions (
                competition_id INT,
                season_id INT,
                season_name VARCHAR(255),
                competition_name VARCHAR(255),
                competition_gender VARCHAR(50),
                competition_youth BOOLEAN,
                competition_international BOOLEAN,
                country VARCHAR(255),
                PRIMARY KEY (competition_id, season_id)
            );

            CREATE TABLE Teams (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                country VARCHAR(255)
            );
                    
            CREATE TABLE Matches (
                id INT PRIMARY KEY,
                date DATE,
                kick_off TIME,
                home_score INT,
                away_score INT,
                competition_stage_name VARCHAR(255),
                stadium_id INT,
                competition_id INT,
                season_id INT,
                home_id INT,
                away_id INT,
                home_group VARCHAR(255),
                away_group VARCHAR(255),
                FOREIGN KEY (stadium_id) REFERENCES Stadiums(id),
                FOREIGN KEY (competition_id, season_id) REFERENCES Competitions(competition_id, season_id),
                FOREIGN KEY (home_id) REFERENCES Teams(id),
                FOREIGN KEY (away_id) REFERENCES Teams(id)
            );
     
            CREATE TABLE Players (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                nickname VARCHAR(255),
                jersey_number INT,
                country VARCHAR(255),
                team_id INT,
                FOREIGN KEY (team_id) REFERENCES Teams(id)
            );

            CREATE TABLE Events (
                event_id VARCHAR(255) PRIMARY KEY,
                match_id INT,
                index INT,
                period INT,
                timestamp TIME,
                minute INT,
                second INT,
                possession INT,
                possession_team_id INT,
                type VARCHAR(255),
                duration INT,
                play_pattern VARCHAR(255),
                position VARCHAR(255),
                location_x INT,
                location_y INT,
                player_id INT,
                FOREIGN KEY (match_id) REFERENCES Matches(id),
                FOREIGN KEY (possession_team_id) REFERENCES Teams(id),
                FOREIGN KEY (player_id) REFERENCES Players(id)
            );


            CREATE TABLE Managers (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                nickname VARCHAR(255),
                date_of_birth DATE,
                team_id INT,
                country VARCHAR(255),
                FOREIGN KEY (team_id) REFERENCES Teams(id)
            );
                    
            CREATE TABLE Lineups (
                id INT PRIMARY KEY,
                team_id INT,
                match_id INT,
                FOREIGN KEY (team_id) REFERENCES Teams(id),
                FOREIGN KEY (match_id ) REFERENCES Matches(id)
            );
                    
            CREATE TABLE PlayerPositions (
                lineup_id INT PRIMARY KEY,
                player_id INT,
                start_reason VARCHAR(255),
                end_reason VARCHAR(255),
                position VARCHAR(50),
                from_date VARCHAR(50),
                to_date VARCHAR(50),
                FOREIGN KEY (lineup_id) REFERENCES Lineups(id),
                FOREIGN KEY (player_id) REFERENCES Players(id)
            );

            CREATE TABLE Cards (
                id INT PRIMARY KEY,
                player_id INT,
                position VARCHAR(255),
                from_period INT,
                to_period INT,
                start_reason VARCHAR(255),
                end_reason VARCHAR(255),
                FOREIGN KEY (player_id) REFERENCES Players(id)
            );
                    
            CREATE TABLE Shots (
                event_id VARCHAR(255) PRIMARY KEY,
                end_location_x FLOAT,
                end_location_y FLOAT,
                key_pass_id VARCHAR(255),
                outcome VARCHAR(50),
                first_time BOOLEAN,
                technique VARCHAR(50),
                body_part VARCHAR(50),
                type VARCHAR(50),
                xG FLOAT,
                FOREIGN KEY (event_id) REFERENCES Events(event_id)
            );

            CREATE TABLE Passes (
                event_id VARCHAR(255) PRIMARY KEY,
                length FLOAT,
                angle FLOAT,
                end_location_x FLOAT,
                end_location_y FLOAT,
                body_part VARCHAR(50),
                outcome VARCHAR(50),
                type VARCHAR(50),
                height VARCHAR(50),
                switch_status BOOLEAN,
                cross_status BOOLEAN,
                cutback BOOLEAN,
                through_ball BOOLEAN,
                assist BOOLEAN,
                key_pass BOOLEAN,
                aerial_won BOOLEAN,
                recipient_player_id INT,
                FOREIGN KEY (event_id) REFERENCES Events(event_id),
                FOREIGN KEY (recipient_player_id) REFERENCES Players(id)
            );

            CREATE TABLE Dribbles (
                event_id VARCHAR(255) PRIMARY KEY,
                name VARCHAR(255),
                under_pressure BOOLEAN,
                complete BOOLEAN,
                FOREIGN KEY (event_id) REFERENCES Events(event_id)
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

        # All drops
        # DROP TABLE IF EXISTS Dribbles;
        # DROP TABLE IF EXISTS Passes;
        # DROP TABLE IF EXISTS Shots;
        # DROP TABLE IF EXISTS Cards;
        # DROP TABLE IF EXISTS PlayerPositions;
        # DROP TABLE IF EXISTS Lineups;
        # DROP TABLE IF EXISTS Managers;
        # DROP TABLE IF EXISTS Events;
        # DROP TABLE IF EXISTS Players;
        # DROP TABLE IF EXISTS Matches;
        # DROP TABLE IF EXISTS Teams;
        # DROP TABLE IF EXISTS Competitions;
        # DROP TABLE IF EXISTS Stadiums;
        cur.execute("""
    
            DROP TABLE IF EXISTS Dribbles;
            DROP TABLE IF EXISTS Passes;
            DROP TABLE IF EXISTS Shots;
            DROP TABLE IF EXISTS Cards;
            DROP TABLE IF EXISTS PlayerPositions;
            DROP TABLE IF EXISTS Lineups;
            DROP TABLE IF EXISTS Managers;
            DROP TABLE IF EXISTS Events;
            DROP TABLE IF EXISTS Players;
            DROP TABLE IF EXISTS Matches;
            DROP TABLE IF EXISTS Teams;
            DROP TABLE IF EXISTS Competitions;
            DROP TABLE IF EXISTS Stadiums;
                    
        """)

        conn.commit()

    except Exception as error:
        print(error)
    finally: #To ensure that the connection object and cursor always close:
        # close the cursor
        cur.close()
        # close the connection, once we are finished with it.
        conn.close() 