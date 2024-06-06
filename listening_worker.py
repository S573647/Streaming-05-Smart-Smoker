"""
    This program listens for work messages contiously. 
    Start multiple versions to add more workers.  

    Author: Kamalini Pradhan
    Date: May 30, 2024
    modifed : June 6, 2024
    purpose : added dqueue logic and edited the script to trigger alerts for different events based on the data in queue stored .

"""


import pika
import struct
import sys
from datetime import datetime
import sys
# Configure logging
from util_logger import setup_logger
from collections import deque
logger, logname = setup_logger(__file__)

# Define deques for storing recent temperature readings for smoker , Food A , Food B temperatures 
smoker_temps = deque(maxlen=5)
food_a_temps = deque(maxlen=20)
food_b_temps = deque(maxlen=20)


def process_smoker_queue(ch, method, properties, body):
    """process messages from the smoker queue."""
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Received from smoker: {formatted_time}, Temperature: {temp}F")
    smoker_temps.append(temp)

    if len(smoker_temps) == smoker_temps.maxlen and (smoker_temps[0] - temp) >= 15:
        logger.info(f"***Smoker Alert**** : {formatted_time} - Smoker temperature dropped by 15F or more in the last 2.5 minutes! , current temperature {temp}F")

    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_food_a_queue(ch, method, properties, body):
    """process messages from the food A queue."""
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Received from food A: {formatted_time}, Temperature: {temp}F")
    food_a_temps.append(temp)

    if len(food_a_temps) == food_a_temps.maxlen :
        if (food_a_temps[-1] - food_a_temps[0]) <= 1:
            logger.info(f"****Food A Alert**** : {formatted_time} Food A temperature change is 1F or less in the last 10 minutes! , current temperature {temp}F")
        elif (food_a_temps[-1] - food_a_temps[0]) > 1:
            logger.info(f"****Food A Alert**** : {formatted_time} Food A temperature change is more than 1F in the last 10 minutes! , current temperature {temp}F")
        else:
            logger.info(f"****Food A Alert**** : {formatted_time} Food A temperature has no change in the last 10 minutes! , current temperature {temp}F")
    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_food_b_queue(ch, method, properties, body):
    """process messages from the food B queue."""
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"Received from food B: {formatted_time}, Temperature: {temp}F")
    food_b_temps.append(temp)

    if len(food_b_temps) == food_b_temps.maxlen :
        if (food_b_temps[-1] - food_b_temps[0]) <= 1:
            logger.info(f"*****Food B Alert******: {formatted_time} Food B temperature change is 1F or less in the last 10 minutes! current temperature {temp}F")
        elif (food_b_temps[-1] - food_b_temps[0]) > 1:
            logger.info(f"*****Food B Alert******: {formatted_time} Food B temperature change is more than 1F in the last 10 minutes! current temperature {temp}F")
        else:
            logger.info(f"*****Food B Alert******: {formatted_time} Food B has no change in the last 10 minutes! current temperature {temp}F")

    ch.basic_ack(delivery_tag=method.delivery_tag)



# define a callback function to be called when a message is received
"""def callback(ch, method, properties, body):
    # Processes and prints data received from RabbitMQ queues.
    timestamp, temp = struct.unpack('!df', body)
    formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f" [x] {formatted_time} - Received Temperature {temp}F from {method.routing_key}")    
    ch.basic_ack(delivery_tag=method.delivery_tag)
    """

def start_listening(queue_name,callback_func):
    """Sets up RabbitMQ consumer connections to listen to a specific queue."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    channel.basic_consume(queue=queue_name, on_message_callback=callback_func, auto_ack=False)    
    print(f"Listening for messages on {queue_name}. Press CTRL+C to exit.")
    channel.start_consuming()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python listener.py <queue_name>  please choose any of the queues 1. 01-smoker 2. 02-Food-A 3. 03-Food-B")
        sys.exit(1)
    queue_name = sys.argv[1]
    #start_listening(queue_name)

    
    if queue_name == "01-smoker":
        start_listening(queue_name, process_smoker_queue)
    elif queue_name == "02-Food-A":
        start_listening(queue_name, process_food_a_queue)
    elif queue_name == "03-Food-B":
        start_listening(queue_name, process_food_b_queue)
