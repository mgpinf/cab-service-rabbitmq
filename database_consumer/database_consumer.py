import pika
import sys
import time

time.sleep(15)

connection=pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
channel=connection.channel()

channel.exchange_declare(exchange='direct_logs',exchange_type='direct')

queue=channel.queue_declare(queue='')

channel.queue_bind(exchange='direct_logs',queue=queue.method.queue,routing_key='database_consumer')

def callback(ch,method,properties,body):
    print(body)

channel.basic_consume(queue=queue.method.queue,on_message_callback=callback,auto_ack=True)

channel.start_consuming()