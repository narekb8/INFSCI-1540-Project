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
import os
from dotenv import load_dotenv

def odb_producer():
    load_dotenv()
    ip = os.getenv('IP')
    # Connect to MySQL database
    odb_conn = None
    odb_transaction_query = "SELECT Pid, Tid, OppTid, Wid, FScore "\
                          " FROM PPpW "\
                          " GROUP BY Pid, Tid, OppTid, Wid, FScore"
    odb_lifetime_query = "SELECT Pid, sum(FScore) "\
                        " FROM PPpW "\
                        " GROUP BY pid"
    odb_vs_query = "SELECT OppTid, sum(FScore) "\
                        " FROM PPpW "\
                        " GROUP BY opptid"
        
                          
    consumer = KafkaConsumer('fact-update-stream',bootstrap_servers=ip+':29092',api_version=(2,0,2),enable_auto_commit=True)                      
    producer = KafkaProducer(bootstrap_servers=ip+':29092',api_version=(2,0,2))
    z = 0
          
    print('\nWaiting for ODB UPDATE EVENT, Ctr/Z to stop ...')
    
    while True:
        message = consumer.poll()
        message = next(iter(message.values()), None)

        if message is not None:
            message = message[0]
            in_string = message.value.decode() 

            print ('\nODB UPDATE EVENT RECEIVED FROM odb-update-stream')
            print ('Updating FACT Table')
                            
            try:  
                odb_conn = mysql.connector.connect(host=ip+'', # !!! make sure you use your VM IP here !!!
                                        port=13306, 
                                        database = 'odb',
                                        user='deuser',
                                        password='depassword')
                odb_cursor = odb_conn.cursor()
                
                if odb_conn.is_connected():
                        print('\nConnected to source ODB MySQL database')

                if 'odb-update-complete' in in_string:
                    odb_cursor.execute(odb_lifetime_query)
                    aggr_tuples1 = odb_cursor.fetchall()
                    for tuple in aggr_tuples1 :
                        in_string = ''.join(str(tuple)).strip('()')
                        producer.send('Lifetime',in_string.encode() )
                        print("\nProduced aggregated tuple: {}".format(tuple))

                    odb_cursor.execute(odb_vs_query)
                    aggr_tuples2 = odb_cursor.fetchall()
                    for tuple in aggr_tuples2 :
                        in_string = ''.join(str(tuple)).strip('()')
                        producer.send('Vs',in_string.encode() )
                        print("\nProduced aggregated tuple: {}".format(tuple))
                        
                    odb_cursor.execute(odb_transaction_query)
                    aggr_tuples = odb_cursor.fetchall()
                    
                    for tuple in aggr_tuples :
                        in_string = ''.join(str(tuple)).strip('()')
                        producer.send('Fact',in_string.encode() )
                        print("\nProduced aggregated tuple: {}".format(tuple))
                        z = z + 1           
                
            except Error as e:
                print(e)
                
            finally:
                if odb_conn is not None and odb_conn.is_connected():
                    odb_cursor.close()
                    odb_conn.close()
            
if __name__ == '__main__':
    odb_producer()
    