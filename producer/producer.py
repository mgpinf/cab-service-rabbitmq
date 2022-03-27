from flask import Flask, render_template, make_response, jsonify, request, redirect
import pymongo
import socket
import pika
import time
import json
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

app=Flask(__name__)

PORT=3200
HOST="0.0.0.0"

key_value_dict={}

def send_message(key,message):
    connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
    channel = connection.channel()
    
    channel.exchange_declare(exchange="direct_logs",exchange_type="direct")
    
    
    if(key=='ride_matching_consumer'):
        channel.basic_publish(exchange="direct_logs",routing_key=key,body=message,properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
    else:
        channel.basic_publish(exchange="direct_logs",routing_key=key,body=message)
    
    print("Message successfully sent")
    connection.close()

@app.route("/new_ride", methods=["POST"])
def new_ride():
    message=json.dumps(request.json)
    send_message("ride_matching_consumer",message)
    return ""

# post req. comes from consumer
@app.route("/new_ride_matching_consumer", methods=["POST"])
def new_ride_matching_consumer():
    consumer_id = request.json.get("consumer_id")
    consumer_name = request.json.get("consumer_name")
    #request_ip_addr = jsonify({"ip": request.remote_addr}), 200
    request_ip_addr = request.remote_addr
    host = socket.getfqdn()
    consumer_ip_addr = socket.gethostbyname(host)
    consumer_key=(consumer_name, consumer_ip_addr)
    consumer_value=(consumer_id, request_ip_addr)
    key_value_dict[consumer_key]=consumer_value
    return ""

@app.route("/rabbit_test", methods=["GET"])
def test_rabbitmq():
    send_message('ride_matching_consumer','This is a sample Message')
    return "Test Successful"

if __name__=="__main__":
    print("Server running in port {}".format(PORT))
    app.run(host=HOST, port=PORT)
