package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

var db *sql.DB

type Notification struct {
	UserID  string                 `json:"user_id"`
	Type    string                 `json:"type"`
	Payload map[string]interface{} `json:"payload"`
}

func main() {

	connStr := "host=localhost port=5432 user=postgres password=flancy dbname=myDb sslmode=disable"
	var err error

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	r := gin.Default()

	r.POST("/notifications", createNotification)
	r.GET("/notifications/:id", getNotification)

	r.GET("/notifications", listNotifications)
	r.Run(":8080")
}

func createNotification(c *gin.Context) {

	var req Notification

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	if req.UserID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}

	if req.Type != "email" && req.Type != "sms" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid type"})
		return
	}

	id := uuid.New().String()

	payloadBytes, _ := json.Marshal(req.Payload)

	_, err := db.Exec(`
	INSERT INTO notifications 
	(id, user_id, type, payload, status, retry_count, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,0,$6,$7)
	`, id, req.UserID, req.Type, payloadBytes, "pending", time.Now(), time.Now())

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Kafka Producer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "notifications",
	})
	defer writer.Close()

	event := map[string]interface{}{
		"notification_id": id,
		"user_id":         req.UserID,
		"type":            req.Type,
		"payload":         req.Payload,
	}

	eventBytes, _ := json.Marshal(event)

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: eventBytes,
		},
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":     id,
		"status": "pending",
	})
}

func getNotification(c *gin.Context) {

	id := c.Param("id")

	var status string
	var retryCount int

	err := db.QueryRow(`
	SELECT status, retry_count 
	FROM notifications 
	WHERE id=$1
	`, id).Scan(&status, &retryCount)

	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":          id,
		"status":      status,
		"retry_count": retryCount,
	})
}
func listNotifications(c *gin.Context) {

	userID := c.Query("user_id")

	rows, err := db.Query(`
		SELECT id, status, retry_count 
		FROM notifications 
		WHERE user_id=$1
	`, userID)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	defer rows.Close()

	var result []gin.H

	for rows.Next() {
		var id string
		var status string
		var retryCount int

		err := rows.Scan(&id, &status, &retryCount)
		if err != nil {
			continue
		}

		result = append(result, gin.H{
			"id":          id,
			"status":      status,
			"retry_count": retryCount,
		})
	}

	c.JSON(http.StatusOK, result)
}
