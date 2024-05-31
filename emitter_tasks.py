"""
This script reads temperature data from 'smoker-temps.csv' every 30 seconds 
and sends it to specific RabbitMQ queues for each temperature channel.

Author: Kamalini Pradhan
Date: May 29, 2024
"""

import pika
import csv
import time
from datetime import datetime
import struct
import sys
from util_logger import setup_logger

# Configure logging
logger, logname = setup_logger(__file__)

def send_data_to_queue(server: str, queue_name: str, content: bytes) -> None:
    """
    Connects to RabbitMQ and sends encoded data to a specified queue.
    
    Args:
        server: RabbitMQ server address.
        queue_name: Name of the RabbitMQ queue.
        content: Encoded data to send.
    """
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(server))
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
        channel.basic_publish(exchange="", routing_key=queue_name, body=content)        
        logger.info(f" [x] Encoded data sent to {queue_name}: {content}")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()
            logger.info(f" [x] Connection closed: {connection}")

def process_data(file_path: str) -> None:
    """
    Reads temperature data from the specified CSV file and sends it to the appropriate RabbitMQ queues.
    
    Args:
        file_path: Path to the CSV file containing temperature data.
    """
    try:
        with open(file_path, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip the header row
            logger.info(f" [x] Processing file: {file_path}")

            for row in reader:
                try:
                    timestamp = datetime.strptime(row[0], '%m/%d/%y %H:%M:%S').timestamp()
                    for index, temp in enumerate(row[1:], 1):
                        if temp:
                            queue_name = f"0{index}-smoker" if index == 1 else f"0{index}-smoker-A" if index == 2 else f"0{index}-smoker-B"
                            smoker_message = struct.pack('!df', timestamp, float(temp))
                            logger.info(f" [x] Read message {temp} from CSV file for queue name {queue_name}")
                            send_data_to_queue("localhost", queue_name, smoker_message)
                    time.sleep(5)
                except ValueError as e:
                    logger.error(f"Error processing row {row}: {e}")
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python script.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    logger.info(f" [x] Task file path: {file_path}")
    process_data(file_path)
