from flask import Flask, render_template, make_response, jsonify, request, redirect
import pymongo
import socket
import pika
import time
from pymongo import MongoClient
from bson.objectid import ObjectId
from datetime import datetime

app=Flask(__name__)

PORT=3200
HOST="0.0.0.0"

key_value_dict={}

def send_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    channel = connection.channel()
    
    channel.exchange_declare(exchange="direct_logs",exchange_type="direct")
    
    key="ride_matching"
    message_body="Please match a ride to my request"
    
    channel.basic_publish(exchange="direct_logs",routing_key=key,body=message_body,properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE))
    
    key="database"
    message_body="Please add it to your database"
    
    channel.basic_publish(exchange="direct_logs",routing_key=key,body=message_body)
    
    print("Message successfully sent")
    connection.close()

@app.route("/new_ride", methods=["POST"])
def new_ride():
    #consumer_id = request.args.get("consumer_id")
    pickup = request.json.get("pickup")
    destination = request.json.get("destination")
    time_in_seconds = request.json.get("time")
    cost = request.json.get("cost")
    seats = request.json.get("seats")
    current_consumer_details = (pickup, destination, time_in_seconds, cost, seats)
    #print(current_consumer_details)
    send_message()
    #amqp_url = os.environ['AMQP_URL']
    #url_params = pika.URLParameters(amqp_url)
    #if session["consumers_details"] is None:
    #    session["consumers_details"] = []
    #session["consumers_details"].append(current_consumer_details)
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
    #print(key_value_dict)
    #print(consumer_key)
    #print(consumer_value)
    #if session["consumers_details_matching"] is None:
    #    session["consumers_details_matching"] = []
    #session["consumers_details_matching"].append(current_consumer_details_matching)
    return ""

if __name__=="__main__":
    print("Server running in port {}".format(PORT))
    app.run(host=HOST, port=PORT)
