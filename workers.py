import pika
import json
import threading
import sqlite3

class DatabaseWorker:
    def __init__(self):
        self.readers = 0
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def process_message(self, message):
        if message['query_type'] == 'READ':
            self.start_read()
            try:
                self.execute_read(message['query'])
            finally:
                self.end_read()
        else:
            self.start_write()
            try:
                self.execute_write(message['operation'], message['data'])
            finally:
                self.end_write()

    def start_read(self):
        with self.condition:
            while self.readers == -1:
                self.condition.wait()
            self.readers += 1

    def end_read(self):
        with self.condition:
            self.readers -= 1
            if self.readers == 0:
                self.condition.notify_all()

    def start_write(self):
        with self.condition:
            while self.readers != 0:
                self.condition.wait()
            self.readers = -1

    def end_write(self):
        with self.condition:
            self.readers = 0
            self.condition.notify_all()

    def execute_read(self, query):
        print(f"Executing read query: {query}")
        conn = sqlite3.connect('example.db')
        c = conn.cursor()
        for row in c.execute(query):
            print(row)
        conn.close()

    def execute_write(self, operation, data):
        print(f"Executing transaction: {operation} with data {data}")
        conn = sqlite3.connect('example.db')
        c = conn.cursor()
        if operation == 'INSERT':
            c.execute("INSERT INTO users (name, age) VALUES (?, ?)", (data['name'], data['age']))
            conn.commit()
        conn.close()

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f"Received message: {message}")
    worker.process_message(message)

def start_worker():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.basic_consume(queue='combined_messages', on_message_callback=callback, auto_ack=True)
    print('Worker started, waiting for messages.')
    channel.start_consuming()

if __name__ == '__main__':
    worker = DatabaseWorker()
    threads = []
    num_workers = 5  # Number of worker threads

    for i in range(num_workers):
        thread = threading.Thread(target=start_worker)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
