import pika
import sys
import time
from pymongo import MongoClient
import os

def insert_into_database(val):
    conn_string = f'mongodb://mongodb:27017/mydatabase'
    myclient = MongoClient(conn_string)
    db = myclient.mydatabase
    db.ride_requests.insert_one({'data':val})


time.sleep(20)

connection=pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel=connection.channel()

channel.exchange_declare(exchange='direct_logs',exchange_type='direct')

queue=channel.queue_declare(queue='')

channel.queue_bind(exchange='direct_logs',queue=queue.method.queue,routing_key='database_consumer')

def callback(ch,method,properties,body):
    insert_into_database(body)

channel.basic_consume(queue=queue.method.queue,on_message_callback=callback,auto_ack=True)

channel.start_consuming()