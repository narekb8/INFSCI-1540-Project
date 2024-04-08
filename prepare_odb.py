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

def prepare_odb():
    # Connect to MySQL database
    conn = None
    create_db = " CREATE DATABASE odb"
    use_db = "use odb"
    create_table1 = "CREATE TABLE Player (pid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "name VARCHAR(20), curr_team INT, pos VARCHAR(20))"
    create_table2 = "CREATE TABLE Team (tid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "name VARCHAR(20), location VARCHAR(20), division VARCHAR(20))"
    create_table3 = "CREATE TABLE Week (wid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "week_num INT, season_num INT, year INT)"
    create_table4 = "CREATE TABLE Transaction (trid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "pid INT, tid INT, opp_tid INT, wid INT, points REAL)"        

    try:  
        conn = mysql.connector.connect(host='10.0.0.42', # !!! make sure you use your VM IP here !!!
                                  port=13306, 
                                  user='deuser',
                                  password='depassword')
        if conn.is_connected():
                print('Connected to MySQL database')
        
        cursor = conn.cursor()
        cursor.execute(create_db)
        cursor.execute(use_db)
        cursor.execute(create_table1)
        cursor.execute(create_table2)
        cursor.execute(create_table3)
        cursor.execute(create_table4)
        
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
    