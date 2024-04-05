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
    queryP = "INSERT INTO Product(P_name,P_category,P_price) " \
            "VALUES(%s,%s,%s)"
    queryC = "INSERT INTO Customer(C_name,C_address) " \
            "VALUES(%s,%s)"
    queryT = "INSERT INTO Transaction(Cid,Pid,amount,Total_sale) " \
            "VALUES(%s,%s,%s,%s)"
    
    consumerP = KafkaConsumer('Product',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    consumerC = KafkaConsumer('Customer',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    consumerT = KafkaConsumer('Transaction',bootstrap_servers='10.0.0.42:29092',api_version=(2,0,2))
    producer = KafkaProducer(bootstrap_servers='10.0.0.42:29092')
                             
    #tuples = [('jones','loc1','prod1', 10),('smith','loc1','prod1', 20),('jones','loc1','prod1', 10)]  
    
    print('\nWaiting for INPUT TUPLES, Ctr/Z to stop ...')
    
    tuples = []
    z = 0

    while True:
        if z == 4:
             break
        messageP = consumerP.poll()
        messageP = next(iter(messageP.values()), None)
        if messageP is not None:
            messageP = messageP[0]
            in_string = messageP.value.decode()
            in_tuple = in_string.strip('"').split(',')
            print ('\nInput Tuple Received: {}'.format(in_tuple))
            
            sleep(1)
            
            name = in_tuple[0]
            category = in_tuple[1]
            price = in_tuple[2]
            tuples.append((name,category,price))
        
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
                
                cursor.execute("SELECT count(*) FROM Product")
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
        messageC = consumerC.poll()
        messageC = next(iter(messageC.values()), None)
        if messageC is not None:
            messageC = messageC[0]
            in_string = messageC.value.decode()
            in_tuple = in_string.strip('"').split(',')
            print ('\nInput Tuple Received: {}'.format(in_tuple))
            
            sleep(1)
            
            name = in_tuple[0]
            address = in_tuple[1]
            tuples.append((name,address))
        
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
                    cursor.execute(queryC,tuple)
                    
                conn.commit()
                
                cursor.execute("SELECT count(*) FROM Customer")
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
            
            cid = in_tuple[0]
            pid = in_tuple[1]
            amount = in_tuple[2]
            total = in_tuple[3]
            tuples.append((cid,pid,amount,total))
        
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
                
                z = z + 1
                    
            except Error as e:
                print(e)
                
            finally:
                if conn is not None and conn.is_connected():
                    cursor.close()
                    conn.close()

            tuples.clear()
    
    m = 'odb update event'   
    producer.send('odb-update-stream', m.encode())
    print('\nODB UPDATE EVENT SENT TO ODB UPDATE STREAM')
    producer.flush()
    odb_consumer()
            
if __name__ == '__main__':
    odb_consumer()
    