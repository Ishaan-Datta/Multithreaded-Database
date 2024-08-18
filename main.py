import threading
from worker import start_worker
from db_setup import setup_database

if __name__ == '__main__':
    setup_database()
    
    threads = []
    num_workers = 5  # Number of worker threads

    for i in range(num_workers):
        thread = threading.Thread(target=start_worker)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

# Entry point for the application. It sets up RabbitMQ, starts worker threads, and coordinates the overall process.