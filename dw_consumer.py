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
from kafka import KafkaConsumer

def dw_consumer():
    # Connect to MySQL database
    dw_conn = None
    dw_load_query = "INSERT INTO fact(product,sale) " \
                     "VALUES(%s,%s)"
    
    consumer = KafkaConsumer('AggrData',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    
    print('\nWaiting for AGGREGATED TUPLES, Ctr/Z to stop ...')
    
    aggr_tuples = []
    z = 0

    for message in consumer:
        in_string = message.value.decode()
        #print ('\nMesssage Received: {}'.format(in_string))
        in_split = in_string.split(',')
            
        product = in_split[0].strip(' \'')
        sale = in_split[1].strip(' \'')
            
        in_tuple = (product,sale)
        print ('\nTuple Received: {}'.format(in_tuple))
        aggr_tuples.append(in_tuple)

        z = z + 1
        
        if z == 2:
              break
        
    try:  
        dw_conn = mysql.connector.connect(host='10.0.0.42', # !!! make sure you use your VM IP here !!!
                                        port=23306, 
                                        database = 'dw',
                                        user='deuser',
                                        password='depassword')
                
        if dw_conn.is_connected():
            print('\nConnected to destination DW MySQL database')
                
        dw_cursor = dw_conn.cursor()
                
        dw_cursor.executemany(dw_load_query,aggr_tuples)
                    
        dw_conn.commit()
                
        dw_cursor.execute("SELECT count(*) FROM fact")
        res = dw_cursor.fetchall()
            
        print('DW is loaded: {} new tuples are inserted'.format(len(in_tuple)))
        print('               {} total tuples are inserted'.format(res[0]))    
                
                    
    except Error as e:
        print(e)
                
    finally:
        if dw_conn is not None and dw_conn.is_connected():
            dw_cursor.close()
            dw_conn.close()    
            
if __name__ == '__main__':
    dw_consumer()
    