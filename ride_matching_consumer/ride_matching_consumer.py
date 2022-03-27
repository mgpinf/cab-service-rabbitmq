import pika
import sys
import time
import os
import requests

time.sleep(5)

consumer_id=None
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

channel.queue_bind(exchange='direct_logs',queue=queue.method.queue,routing_key='ride_matching')

def callback(ch,method,properties,body):
    print(body)
    sleep_seconds = 0
    time.sleep(sleep_seconds)
    time.sleep(3)
    print('[x] Done')
    print(consumer_id)
    print(method.delivery_tag)
    ch.basic_ack(delivery_tag=method.delivery_tag)

#channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue.method.queue,on_message_callback=callback)

channel.start_consuming()
