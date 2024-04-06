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

from kafka import KafkaConsumer, KafkaProducer
#import json
from time import sleep

def odb_consumer():
    # Connect to MySQL database
    conn = None
    queryP = "INSERT INTO Player(teamid,ht,wt) " \
            "VALUES(%s,%s,%s)"
    queryTe = "INSERT INTO Team(name,city) " \
            "VALUES(%s,%s)"
    queryW = "INSERT INTO Week(num,season,year) " \
            "VALUES(%s,%s,%s)"
    queryT = "INSERT INTO Transaction(pid,tid,opp_tid,wid,points) " \
            "VALUES(%s,%s,%s,%s,%s)"
    
    consumerP = KafkaConsumer('Player',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    consumerTe = KafkaConsumer('Team',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    consumerW = KafkaConsumer('Week',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    consumerT = KafkaConsumer('Transaction',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    producer = KafkaProducer(bootstrap_servers='10.0.0.42:29092')
    
    print('\nWaiting for INPUT TUPLES, Ctr/Z to stop ...')
    
    tuples = []
    transaction_done = False

    while True:

        if transaction_done is True:
            transaction_done = False
            m = 'odb update event'   
            producer.send('odb-update-stream', m.encode())
            print('\nODB UPDATE EVENT SENT TO ODB UPDATE STREAM')
            producer.flush()
        
        messageP = consumerP.poll()
        messageP = next(iter(messageP.values()), None)

        if messageP is not None:
            messageP = messageP[0]
            in_string = messageP.value.decode()
            in_tuple = in_string.strip('"').split(',')
            print ('\nInput Tuple Received: {}'.format(in_tuple))
            
            sleep(1)
            
            teamid = in_tuple[0]
            ht = in_tuple[1]
            wt = in_tuple[2]
            tuples.append((teamid,ht,wt))
        
            try:  
                conn = mysql.connector.connect(host='10.0.0.42', # !!! make sure you use your VM IP here !!!
                                        port=13306, 
                                        database = 'odb',
                                        user='deuser',
                                        password='depassword')
                if conn.is_connected():
                        print('\nConnected to destination ODB MySQL database')
                
                cursor = conn.cursor()
                
                for tuple in tuples:
                    cursor.execute(queryP,tuple)
                    
                conn.commit()
                
                cursor.execute("SELECT count(*) FROM Player")
                print(cursor.fetchall())
                
                sleep(2)
                    
            except Error as e:
                print(e)
                
            finally:
                if conn is not None and conn.is_connected():
                    cursor.close()
                    conn.close()
            
            tuples.clear()
############################################################################################
        messageW = consumerW.poll()
        messageW = next(iter(messageW.values()), None)

        if messageW is not None:
            messageW = messageW[0]
            in_string = messageW.value.decode()
            in_tuple = in_string.strip('"').split(',')
            print ('\nInput Tuple Received: {}'.format(in_tuple))
            
            sleep(1)
            
            num = in_tuple[0]
            season = in_tuple[1]
            year = in_tuple[2]
            tuples.append((num,season,year))
        
            try:  
                conn = mysql.connector.connect(host='10.0.0.42', # !!! make sure you use your VM IP here !!!
                                        port=13306, 
                                        database = 'odb',
                                        user='deuser',
                                        password='depassword')
                if conn.is_connected():
                        print('\nConnected to destination ODB MySQL database')
                
                cursor = conn.cursor()
                
                for tuple in tuples:
                    cursor.execute(queryW,tuple)
                    
                conn.commit()
                
                cursor.execute("SELECT count(*) FROM Week")
                print(cursor.fetchall())
                
                sleep(2)
                    
            except Error as e:
                print(e)
                
            finally:
                if conn is not None and conn.is_connected():
                    cursor.close()
                    conn.close()
            
            tuples.clear()
############################################################################################
        messageTe = consumerTe.poll()
        messageTe = next(iter(messageTe.values()), None)

        if messageTe is not None:
            messageTe = messageTe[0]
            in_string = messageTe.value.decode()
            in_tuple = in_string.strip('"').split(',')
            print ('\nInput Tuple Received: {}'.format(in_tuple))
            
            sleep(1)
            
            name = in_tuple[0]
            city = in_tuple[1]
            tuples.append((name,city))
        
            try:  
                conn = mysql.connector.connect(host='10.0.0.42', # !!! make sure you use your VM IP here !!!
                                        port=13306, 
                                        database = 'odb',
                                        user='deuser',
                                        password='depassword')
                if conn.is_connected():
                        print('\nConnected to destination ODB MySQL database')
                
                cursor = conn.cursor()
                
                for tuple in tuples:
                    cursor.execute(queryTe,tuple)
                    
                conn.commit()
                
                cursor.execute("SELECT count(*) FROM Team")
                print(cursor.fetchall())
                
                sleep(2)
                    
            except Error as e:
                print(e)
                
            finally:
                if conn is not None and conn.is_connected():
                    cursor.close()
                    conn.close()

            tuples.clear()
############################################################################################
        messageT = consumerT.poll()
        messageT = next(iter(messageT.values()), None)
        
        if messageT is not None:
            print(consumerT.subscription())
            messageT = messageT[0]
            in_string = messageT.value.decode()
            in_tuple = in_string.strip('"').split(',')
            if len(in_tuple) < 4:
                 continue
            
            print ('\nInput Tuple Received: {}'.format(in_tuple))

            sleep(1)
            
            pid = in_tuple[0]
            tid = in_tuple[1]
            opp_tid = in_tuple[2]
            wid = in_tuple[3]
            points = in_tuple[4]
            tuples.append((pid,tid,opp_tid,wid,points))
        
            try:  
                conn = mysql.connector.connect(host='10.0.0.42', # !!! make sure you use your VM IP here !!!
                                        port=13306, 
                                        database = 'odb',
                                        user='deuser',
                                        password='depassword')
                if conn.is_connected():
                        print('\nConnected to destination ODB MySQL database')
                
                cursor = conn.cursor()
                
                for tuple in tuples:
                    cursor.execute(queryT,tuple)
                    
                conn.commit()
                
                cursor.execute("SELECT count(*) FROM Transaction")
                print(cursor.fetchall())
                
                sleep(2)
                    
            except Error as e:
                print(e)
                
            finally:
                if conn is not None and conn.is_connected():
                    cursor.close()
                    conn.close()

            tuples.clear()
            
if __name__ == '__main__':
    odb_consumer()
    