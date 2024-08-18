package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

type Task struct {
	ID      int
	Op      string
	Key     string
	Value   string
}

type DBQueue struct {
	tasks chan Task
	db    *bolt.DB
	wg    sync.WaitGroup
	mutex sync.Mutex
}

func NewDBQueue(db *bolt.DB, workers int) *DBQueue {
	q := &DBQueue{
		tasks: make(chan Task, 100), // Buffered channel for tasks
		db:    db,
	}

	for i := 0; i < workers; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}

	return q
}

func (q *DBQueue) worker(id int) {
	defer q.wg.Done()
	for task := range q.tasks {
		switch task.Op {
		case "read":
			q.handleRead(task)
		case "write":
			q.handleWrite(task)
		default:
			log.Printf("Unknown task operation: %s", task.Op)
		}
	}
}

func (q *DBQueue) handleRead(task Task) {
	q.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		value := b.Get([]byte(task.Key))
		fmt.Printf("Worker %d: Read Key=%s, Value=%s\n", task.ID, task.Key, value)
		return nil
	})
}

func (q *DBQueue) handleWrite(task Task) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		err := b.Put([]byte(task.Key), []byte(task.Value))
		if err == nil {
			fmt.Printf("Worker %d: Wrote Key=%s, Value=%s\n", task.ID, task.Key, task.Value)
		}
		return err
	})

	if err != nil {
		log.Printf("Error writing to database: %v", err)
	}
}

func (q *DBQueue) AddTask(task Task) {
	q.tasks <- task
}

func (q *DBQueue) Close() {
	close(q.tasks)
	q.wg.Wait()
}

func main() {
	// Open the BoltDB database
	db, err := bolt.Open("mydb.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create the bucket
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		return err
	})

	// Create a DBQueue with 3 workers
	queue := NewDBQueue(db, 3)

	// Add some tasks
	queue.AddTask(Task{ID: 1, Op: "write", Key: "foo", Value: "bar"})
	queue.AddTask(Task{ID: 2, Op: "read", Key: "foo"})
	queue.AddTask(Task{ID: 3, Op: "write", Key: "baz", Value: "qux"})
	queue.AddTask(Task{ID: 4, Op: "read", Key: "baz"})

	// Close the queue and wait for all workers to finish
	queue.Close()

	fmt.Println("All tasks completed")
}
