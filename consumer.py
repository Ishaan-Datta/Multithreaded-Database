import pika
import json

def send_to_combined_queue(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_publish(exchange='', routing_key='combined_messages', body=message)
    connection.close()

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f"Received message: {message}")

    # Process message based on queue
    if method.routing_key == 'read_queries':
        process_read_query(message)
    elif method.routing_key == 'transaction_operations':
        process_transaction(message)

    # Send to combined queue
    send_to_combined_queue(json.dumps(message))

def process_read_query(message):
    print(f"Processing read query: {message['query']} added on {message['date_added']}")
    # Implement your read query handling logic here

def process_transaction(message):
    print(f"Processing transaction: {message['operation']} with data {message['data']} added on {message['date_added']}")
    # Implement your transaction handling logic here

def start_consuming():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.basic_consume(queue='read_queries', on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue='transaction_operations', on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    start_consuming()
