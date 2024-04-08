#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
import sys
import os
from dotenv import load_dotenv
#doing this so we don't have to keep loading ips
load_dotenv()

def producer_f(topic,broker_addr):

    producer = KafkaProducer(bootstrap_servers=broker_addr,api_version=(2,0,2))
    
    filename = topic+".txt"
    file_src = open(filename,"r")
    count = 0

    while True:
        count += 1
        line = file_src.readline().strip() # getting rid of \n
        if line != '':
            producer.send(topic,line.encode())
        
        sleep(4)

        # if line is empty
        # end of file is reached
        if not line:
            break
        print("\nProduced input tuple {}: {}".format(count-1, line))
        #print("Sent {}".format(line))

    file_src.close()
    print("\nDone with producing data to topic {}.".format(topic))

ip = os.getenv('IP')
broker_addr = ip+':29092'

producer_f(sys.argv[1], broker_addr)

