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
from kafka import KafkaProducer, KafkaConsumer
from time import sleep

def odb_producer():
    # Connect to MySQL database
    odb_conn = None
    odb_aggregate_query = "SELECT pid, tid, opp_tid, wid, points "\
                          " FROM Transaction "\
                          " GROUP BY Pid"    
                          
    consumer = KafkaConsumer('odb-update-stream',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))                      
    producer = KafkaProducer(bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
          
    print('\nWaiting for ODB UPDATE EVENT, Ctr/Z to stop ...')
    
    while True:
        message = consumer.poll()
        message = next(iter(message.values()), None)

        if message is not None:
            print ('\nODB UPDATE EVENT RECEIVED FROM odb-update-stream')
            print ('Producing aggregated tuple for AggrData stream ...')
            
            sleep(1)
                            
            try:  
                odb_conn = mysql.connector.connect(host='10.0.0.42', # !!! make sure you use your VM IP here !!!
                                        port=13306, 
                                        database = 'odb',
                                        user='deuser',
                                        password='depassword')
                
                if odb_conn.is_connected():
                        print('\nConnected to source ODB MySQL database')
                        
                odb_cursor = odb_conn.cursor()
                odb_cursor.execute(odb_aggregate_query)
                aggr_tuples = odb_cursor.fetchall()
                
                for tuple in aggr_tuples :
                    in_string = ''.join(str(tuple)).strip('()')
                    producer.send('AggrData',in_string.encode() )
                    print("\nProduced aggregated tuple: {}".format(tuple))
            
                
                    
            except Error as e:
                print(e)
                
            finally:
                if odb_conn is not None and odb_conn.is_connected():
                    odb_cursor.close()
                    odb_conn.close()
            
if __name__ == '__main__':
    odb_producer()
    