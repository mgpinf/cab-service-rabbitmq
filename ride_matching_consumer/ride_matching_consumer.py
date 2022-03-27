import pika
import sys
import time
import os
import requests

time.sleep(20)

producer_ip_addr=os.environ("PRODUCER_ADDR")
consumer_id=os.environ("CONSUMER_ID")
PORT=3200

url="http://"+producer_ip_addr+":3200/new_ride_matching_consumer"
myobj = {"CONSUMER_ID": consumer_id}

requests.post(url, data = myobj)

def route():
    server_id,server_port=str(os.environ()).split(":")
    consumer_id=int(os.environ())
    url=server_id+":"+server_port+"/newride"
    names={consumer_id:"Jamie"}
    requests.post(url,data=names)
    

connection=pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel=connection.channel()

channel.exchange_declare(exchange='direct_logs',exchange_type='direct')

queue=channel.queue_declare(queue='',durable=True)

channel.queue_bind(exchange='direct_logs',queue=queue.method.queue,routing_key='ride_matching_consumer')

def callback(ch,method,properties,body):
    sleep_seconds = 0
    time.sleep(sleep_seconds)
    time.sleep(3)
    print(body)
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue.method.queue,on_message_callback=callback)

channel.start_consuming()
