from flask import Flask, render_template, make_response, jsonify, request, redirect
import pymongo
import socket
import pika
import time
import json
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

count=[1]
consumers_active=[]
app=Flask(__name__)

PORT=3200
HOST="0.0.0.0"

key_value_dict={}

def send_message(key,message):
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    
    #channel.exchange_declare(exchange="direct_logs",exchange_type="direct")
    channel.queue_declare(queue='ride_matching_consumer',durable=True)
    channel.queue_declare(queue='database_consumer')

    
    if(key=='ride_matching_consumer'):
        channel.basic_publish(exchange='',routing_key='ride_matching_consumer',body=message,properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
        #channel.basic_publish(exchange="direct_logs",routing_key=key,body=message,properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
    else:
        #channel.basic_publish(exchange="direct_logs",routing_key=key,body=message)
        channel.basic_publish(exchange='',routing_key='database_consumer',body=message)
    print("Message successfully sent")
    connection.close()

@app.route("/new_ride", methods=["POST"])
def new_ride():
    count[0]+=1
    datadic=request.json
    datadic['time']=5
    datadic['taskId']=count[0]
    message=json.dumps(datadic)
    send_message("ride_matching_consumer",message)
    send_message("database_consumer",message)
    return ""

# post req. comes from consumer
@app.route("/new_ride_matching_consumer", methods=["POST"])
def new_ride_matching_consumer():
    #request_ip_addr = jsonify({"ip": request.remote_addr}), 200
    #request_ip_addr = request.remote_addr
    #host = socket.getfqdn()
    #consumer_ip_addr = socket.gethostbyname(host)
    #consumer_key=(consumer_name, consumer_ip_addr)
    #consumer_value=(consumer_id, request_ip_addr)
    #key_value_dict[consumer_key]=consumer_value
    print("Ride matched ------ >")
    print("Ride Details : ",request.json)
    send_message("database_consumer",json.dumps(request.json))
    #consumers_active.append(request.json)
    return ""

@app.route("/get_consumers", methods=["GET"])
def get_all_active_consumers():
    return ("These are the consumers currently listening for messages \n"+str(consumers_active))

@app.route("/register_consumer",methods=["POST"])
def register_consumer():
    consumers_active.append(str(request.json))
    return ""

@app.route("/rabbit_test", methods=["GET"])
def test_rabbitmq():
    datadic={}
    datadic['time']=5
    datadic['taskId']=count
    message=json.dumps(datadic)
    send_message('ride_matching_consumer',message)
    send_message('database_consumer',message)
    return "Test Successful"

if __name__=="__main__":
    print("Server running in port {}".format(PORT))
    app.run(host=HOST, port=PORT)
