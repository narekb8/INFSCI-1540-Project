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
    create_table2 = "CREATE TABLE Product (Pid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "P_name VARCHAR(20), P_category VARCHAR(20), P_price INT)"
    create_table3 = "CREATE TABLE Customer (Cid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "C_name VARCHAR(20), C_address VARCHAR(20))"
    create_table1 = "CREATE TABLE Transaction (Tid INT NOT NULL AUTO_INCREMENT PRIMARY KEY, " \
                     "Cid INT, Pid INT, amount INT, Total_sale REAL)"        

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
        cursor.execute(create_table2)
        cursor.execute(create_table3)
        cursor.execute(create_table1)
        
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
    