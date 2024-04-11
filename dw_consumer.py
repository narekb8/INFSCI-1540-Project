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
import os
from dotenv import load_dotenv

load_dotenv()

def dw_consumer():
    #load db ip
    ip = os.getenv('IP')
    # Connect to MySQL database
    dw_conn = None
    dw_load_fact = "INSERT INTO Fact(pid,tid,opp_tid,wid,points) " \
                     "VALUES(%s,%s,%s,%s,%s)"
    dw_load_lifetime = "INSERT INTO Lifetime(pid,points) " \
                     "VALUES(%s,%s)"
    dw_load_vs = "INSERT INTO Vs(opp_tid,points) " \
                     "VALUES(%s,%s)"
    
    
    
    consumer = KafkaConsumer('Fact',bootstrap_servers=ip+':29092',api_version=(2,0,2))
    consumer.subscribe('Lifetime')
    consumer.subscribe(['Vs', 'Lifetime', 'Fact'])
    
    print('\nWaiting for AGGREGATED TUPLES, Ctr/Z to stop ...')

    while True:
        raw_messages = consumer.poll(max_records=5000)
        #if bool(raw_messages): print(str(raw_messages) + '\n\n\n')
        for topic_partition, message in raw_messages.items():
            topic_partition = topic_partition[0]

            for entry in message:
                if topic_partition == 'Fact':
                    in_string = entry.value.decode()
                    #print ('\nMesssage Received: {}'.format(in_string))
                    in_split = in_string.split(',')
                        
                    pid = in_split[0].strip(' \'')
                    tid = in_split[1].strip(' \'')
                    if tid == 'None':
                        tid = None
                    opp_tid = in_split[2].strip(' \'')
                    if opp_tid == 'None':
                        opp_tid = None
                    wid = in_split[3].strip(' \'')
                    pts = in_split[4].strip(' \'')
                    if pts == 'None':
                        pts = None

                    in_tuple = (pid, tid, opp_tid, wid, pts)

                elif topic_partition == 'Lifetime':
                    in_string = entry.value.decode()
                    #print ('\nMesssage Received: {}'.format(in_string))
                    in_split = in_string.split(',')
                        
                    pid = in_split[0].strip(' \'')
                    pts = in_split[1].strip(' \'')
                    if pts == 'None':
                        pts = None
                    
                    in_tuple = (pid, pts)
                
                elif topic_partition == 'Vs':
                    in_string = entry.value.decode()
                    #print ('\nMesssage Received: {}'.format(in_string))
                    in_split = in_string.split(',')
                        
                    opp_tid = in_split[0].strip(' \'')
                    if opp_tid == 'None':
                        opp_tid = None
                    pts = in_split[1].strip(' \'')
                    if pts == 'None':
                        pts = None
                    
                    in_tuple = (opp_tid, pts)

                print ('\nTuple Received: {}'.format(in_tuple))
                
                try:  
                    dw_conn = mysql.connector.connect(host=ip, # !!! make sure you use your VM IP here !!!
                                                    port=23306, 
                                                    database = 'dw',
                                                    user='deuser',
                                                    password='depassword')
                            
                    if dw_conn.is_connected():
                        print('\nConnected to destination DW MySQL database')
                            
                    dw_cursor = dw_conn.cursor()
                            
                    if topic_partition == 'Fact': dw_cursor.execute(dw_load_fact, in_tuple)
                    elif topic_partition == 'Lifetime': dw_cursor.execute(dw_load_lifetime, in_tuple)
                    elif topic_partition == 'Vs': dw_cursor.execute(dw_load_vs, in_tuple)
                                
                    dw_conn.commit()
                            
                    if topic_partition == 'Fact': dw_cursor.execute("SELECT count(*) FROM Fact")
                    elif topic_partition == 'Lifetime': dw_cursor.execute("SELECT count(*) FROM Lifetime")
                    elif topic_partition == 'Vs': dw_cursor.execute("SELECT count(*) FROM Vs")

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
    