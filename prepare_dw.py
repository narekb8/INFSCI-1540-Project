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

def prepare_dw():
    load_dotenv()

    ip = os.getenv('IP')
    # Connect to MySQL database
    conn = None
    create_db = " CREATE DATABASE dw"
    use_db = "use dw"
    create_table1 = "CREATE TABLE Fact (factid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "pid INT, tid INT, opp_tid INT, wid INT, points REAL)"        
    create_table2 = "CREATE TABLE Lifetime (ltid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "pid INT, points REAL)"
    create_table3 = "CREATE TABLE Vs (vsid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "opp_tid INT, points REAL)"

    try:  
        conn = mysql.connector.connect(host=ip, # !!! make sure you use your VM IP here !!!
                                  port=23306, 
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
        
        conn.commit()

        print('DW is prepared')
        
            
    except Error as e:
        print(e)
        
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()
            
if __name__ == '__main__':
    prepare_dw()
    