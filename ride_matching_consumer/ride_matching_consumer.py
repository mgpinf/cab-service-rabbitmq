import pika
import sys
import time
import os
import json
import requests

time.sleep(20)

server_id=str(os.getenv('SERVERIP'))
consumer_id=int(os.getenv('CONSUMERID'))
url="http://producer:3200/new_ride_matching_consumer"
names={'consumer_id':consumer_id,'consumer_name':"Jhabhd"}
requests.post(url,data=names)
    

connection=pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel=connection.channel()

channel.exchange_declare(exchange='direct_logs',exchange_type='direct')

queue=channel.queue_declare(queue='',durable=True)

channel.queue_bind(exchange='direct_logs',queue=queue.method.queue,routing_key='ride_matching_consumer')

def callback(ch,method,properties,body):
    #sleep_seconds = 0
    #time.sleep(sleep_seconds)
    #time.sleep(3)
    #print(body)
    #ch.basic_ack(delivery_tag=method.delivery_tag)
    datadirc=json.loads(body.decode("utf-8"))
    task_id=datadirc["taskId"]
    sleep_seconds = datadirc["time"]
    print("Starting to sleep for ",sleep_seconds," seconds")
    print("MessageID ",task_id)
    time.sleep(sleep_seconds)
    print(body)
    #print(consumer_id)
    #print(task_id)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue.method.queue,on_message_callback=callback)

channel.start_consuming()
