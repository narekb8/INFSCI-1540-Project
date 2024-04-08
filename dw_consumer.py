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
    dw_load_fact = "INSERT INTO fact(pid,tid,opp_tid,wid,points) " \
                     "VALUES(%s,%s,%s,%s,%s)"
    dw_load_lifetime = "INSERT INTO Lifetime(pid,points) " \
                     "VALUES(%s,%s)"
    dw_load_vs = "INSERT INTO Vs(pid,opp_tid,points) " \
                     "VALUES(%s,%s,%s)"
    
    
    
    consumer = KafkaConsumer('Fact',bootstrap_servers=ip+':29092',api_version=(2,0,2))
    consumerL = KafkaConsumer('Lifetime',bootstrap_servers=ip+':29092',api_version=(2,0,2))
    consumerV = KafkaConsumer('Vs',bootstrap_servers=ip+':29092',api_version=(2,0,2))
    
    print('\nWaiting for AGGREGATED TUPLES, Ctr/Z to stop ...')

    while True:
        message = consumer.poll()
        message = next(iter(message.values()), None)

        if message is not None:
            in_string = message.value.decode()
            #print ('\nMesssage Received: {}'.format(in_string))
            in_split = in_string.split(',')
                
            pid = in_split[0].strip(' \'')
            tid = in_split[1].strip(' \'')
            opp_tid = in_split[2].strip(' \'')
            wid = in_split[3].strip(' \'')
            pts = in_split[4].strip(' \'')
                
            in_tuple = (pid, tid, opp_tid, wid, pts)
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
                        
                dw_cursor.execute(dw_load_fact, in_tuple)
                            
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

############################################################################################
        messageL = consumerL.poll()
        messageL = next(iter(messageL.values()), None)

        if messageL is not None:
            in_string = messageL.value.decode()
            #print ('\nMesssage Received: {}'.format(in_string))
            in_split = in_string.split(',')
                
            pid = in_split[0].strip(' \'')
            pts = in_split[1].strip(' \'')
                
            in_tuple = (pid, pts)
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
                        
                dw_cursor.execute(dw_load_lifetime, in_tuple)
                            
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

############################################################################################
        messageV = consumerV.poll()
        messageV = next(iter(messageV.values()), None)

        if messageV is not None:
            in_string = messageV.value.decode()
            #print ('\nMesssage Received: {}'.format(in_string))
            in_split = in_string.split(',')
                
            pid = in_split[0].strip(' \'')
            opp_tid = in_split[1].strip(' \'')
            pts = in_split[2].strip(' \'')
                
            in_tuple = (pid, opp_tid, pts)
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
                        
                dw_cursor.execute(dw_load_vs, in_tuple)
                            
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
    