#!/usr/bin/env python3
from kafka import KafkaProducer
from time import sleep
import sys
import os
from dotenv import load_dotenv
import csv
#doing this so we don't have to keep loading ips
load_dotenv()

def producer_f(topic,broker_addr):

    producer = KafkaProducer(bootstrap_servers=broker_addr,api_version=(2,0,2))
    
    filename = topic+".csv"
    # file_src = open(filename,"r")
    count = 0

    #while True:
    
    with open(filename, mode = 'r') as file:
        csvFile = csv.reader(file)
        for line in csvFile:
            producer.send(topic,line.encode())
            count += 1
            print("\nProduced input tuple {}: {}".format(count-1, line))

    
    sleep(4)
    print("\nDone with producing data to topic {}.".format(topic))

ip = os.getenv('IP')
broker_addr = ip+':29092'

producer_f('Teams', broker_addr)
