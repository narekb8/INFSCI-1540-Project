#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: vladimir
Make sure you have mysql-connector installed:
    pip install mysql-connector-python
Also, make sure you created a mysql user deuser with password depassword and granted your user all privileges    
"""

import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

def prepare_odb():
    load_dotenv()
    ip = os.getenv('IP')

    # Connect to MySQL database
    conn = None
    create_db = " CREATE DATABASE odb"
    use_db = "use odb"
    create_player_table = "CREATE TABLE Player (Pid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "P_name VARCHAR(35), Tid INT, Pos VARCHAR(8))"
    create_team_table = "CREATE TABLE Team (Tid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "T_name VARCHAR(30), T_abbr VARCHAR(3), T_conf VARCHAR(3), T_div VARCHAR(5))"
    #ppw = points per week
    create_table_ppw = "CREATE TABLE PPpW (Pid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "Tid INT, OppTid INT, Wid INT, FScore REAL)"        
    create_table_week = "CREATE TABLE Week (Wid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                    "Num INT, Season INT)"        
    try:  
        conn = mysql.connector.connect(host=ip+'', # !!! make sure you use your VM IP here !!!
                                  port=13306, 
                                  user='deuser',
                                  password='depassword')
        if conn.is_connected():
                print('Connected to MySQL database')
        
        cursor = conn.cursor()
        cursor.execute(create_db)
        cursor.execute(use_db)
        cursor.execute(create_player_table)
        cursor.execute(create_team_table)
        cursor.execute(create_table_ppw)
        cursor.execute(create_table_week)
        
        conn.commit()

        print('ODB is prepared')
        

    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()
            
if __name__ == '__main__':
    prepare_odb()
