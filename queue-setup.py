import pika
import json
from datetime import datetime

# Establish connection to RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare queues
channel.queue_declare(queue='read_queries')
channel.queue_declare(queue='transaction_operations')
channel.queue_declare(queue='combined_messages')

connection.close()
