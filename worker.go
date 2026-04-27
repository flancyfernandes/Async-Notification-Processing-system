package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

func main() {

	connStr := "host=localhost port=5432 user=postgres password=flancy dbname=myDb sslmode=disable"

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "notifications",
		GroupID: "notification-workers",
	})

	// Writer for retry
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "notifications",
	})
	defer writer.Close()

	fmt.Println("Worker started...")

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Kafka error:", err)
			continue
		}

		var event map[string]interface{}
		json.Unmarshal(msg.Value, &event)

		id, ok := event["notification_id"].(string)
		if !ok {
			log.Println("invalid message")
			continue
		}

		fmt.Println("Received:", id)

		go processNotification(db, writer, id, msg.Value)
	}
}

func processNotification(db *sql.DB, writer *kafka.Writer, id string, originalMsg []byte) {

	var status string
	var retryCount int

	// 🔹 Check current status (Idempotency)
	err := db.QueryRow(`
		SELECT status, retry_count 
		FROM notifications 
		WHERE id=$1
	`, id).Scan(&status, &retryCount)

	if err != nil {
		log.Println("DB fetch error:", err)
		return
	}

	// Skip if already processed
	if status == "sent" || status == "processing" {
		fmt.Println("Skipping duplicate:", id)
		return
	}

	// 🔹 Update to processing
	_, err = db.Exec(`
		UPDATE notifications 
		SET status='processing', updated_at=$2 
		WHERE id=$1
	`, id, time.Now())

	if err != nil {
		log.Println("DB update error:", err)
		return
	}

	fmt.Println("Processing:", id)

	// 🔹 Simulate processing (you can simulate failure here)
	success := simulateWork()

	if success {
		// ✅ Mark as sent
		_, err = db.Exec(`
			UPDATE notifications 
			SET status='sent', updated_at=$2 
			WHERE id=$1
		`, id, time.Now())

		if err != nil {
			log.Println("DB error:", err)
		}

		fmt.Println("Sent:", id)
		return
	}

	// ❌ Failure handling
	if retryCount < 3 {

		retryCount++

		fmt.Println("Retrying:", id, "Attempt:", retryCount)

		// update retry_count + status back to pending
		_, err = db.Exec(`
			UPDATE notifications 
			SET status='pending', retry_count=$2, updated_at=$3 
			WHERE id=$1
		`, id, retryCount, time.Now())

		if err != nil {
			log.Println("DB error:", err)
			return
		}

		// small delay (backoff)
		time.Sleep(2 * time.Second)

		// re-publish message to Kafka
		err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: originalMsg,
			},
		)

		if err != nil {
			log.Println("Kafka retry error:", err)
		}

	} else {

		_, err = db.Exec(`
			UPDATE notifications 
			SET status='failed', updated_at=$2 
			WHERE id=$1
		`, id, time.Now())

		if err != nil {
			log.Println("DB error:", err)
		}

		fmt.Println("Failed:", id)
	}
}

func simulateWork() bool {
	n := rand.Intn(10)
	fmt.Println("Random value:", n)

	if n < 3 {
		fmt.Println("SUCCESS")
		return true
	}

	fmt.Println("FAIL")
	return false
}
