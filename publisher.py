def send_message(queue_name, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_publish(exchange='', routing_key=queue_name, body=message)
    connection.close()

# Example usage
read_query = QueryMessage("SELECT * FROM users", "READ")
transaction = TransactionMessage("INSERT", {"name": "John Doe", "age": 30})

send_message('read_queries', read_query.to_json())
send_message('transaction_operations', transaction.to_json())
