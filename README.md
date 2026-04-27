 Async Notification Processing System

An asynchronous notification system built using **Go, Kafka, and PostgreSQL**.
This project processes notifications (email/SMS) in the background with retry logic, failure handling, and concurrent workers.

---

 Problem Statement

Build a scalable system where:

1. Client sends a notification request
2. Request is stored in PostgreSQL
3. Event is published to Kafka
4. Worker consumes and processes notification
5. Status is updated in the database

---

Tech Stack

* **Golang (Gin Framework)**
* **Apache Kafka**
* **PostgreSQL**

---

 Features

* ✅ REST API to create notifications
* ✅ Kafka-based asynchronous processing
* ✅ Worker service (consumer)
* ✅ Retry mechanism (max 3 retries)
* ✅ Failure handling
* ✅ Idempotent processing (no duplicate execution)
* ✅ Concurrent processing using goroutines
* ✅ Status tracking system

---

 System Architecture

```
Client → API (main.go) → PostgreSQL → Kafka → Worker (worker.go) → PostgreSQL
```

---

 Notification Lifecycle

```
pending → processing → sent
        → processing → failed
```

---

 Database Schema

### `notifications` table

| Field       | Type               |
| ----------- | ------------------ |
| id          | UUID               |
| user_id     | string             |
| type        | string (email/sms) |
| payload     | JSON               |
| status      | string             |
| retry_count | int                |
| created_at  | timestamp          |
| updated_at  | timestamp          |

---

 APIs

### 1️⃣ Create Notification

**POST** `/notifications`

#### Request:

```json
{
  "user_id": "user_123",
  "type": "email",
  "payload": {
    "to": "test@example.com",
    "message": "Hello!"
  }
}
```

#### Response:

```json
{
  "id": "notification_id",
  "status": "pending"
}
```

---

### 2️⃣ Get Notification Status

**GET** `/notifications/{id}`

#### Response:

```json
{
  "id": "notification_id",
  "status": "sent",
  "retry_count": 1
}
```

---

### 3️⃣ List Notifications

**GET** `/notifications?user_id=xxx`

#### Response:

```json
[
  {
    "id": "notification_id",
    "status": "sent",
    "retry_count": 0
  }
]
```

---

## 🔁 Retry Logic

* Max **3 retries**
* On failure:

  * Increment `retry_count`
  * Set status back to `pending`
  * Re-publish message to Kafka
* If retries exceed limit:

  * Status = `failed`

---

 Idempotency

* Prevents duplicate processing
* If status is already `sent` or `processing`, worker skips execution

---

 Concurrency

* Worker processes multiple notifications simultaneously using **goroutines**

---

 Test Scenarios Covered

 Basic Flow

* Notification created → processed → status = `sent`

 Failure + Retry

* Failure → retries → success
* Failure → retries exhausted → `failed`

 Idempotency

* Same message processed multiple times → no duplicate updates

 Edge Cases

* Invalid input
* Unsupported type
* Retry limits
* Kafka re-processing

---

 How to Run

### 1. Start Kafka & Zookeeper

```bash
zookeeper-server-start.bat config\zookeeper.properties
kafka-server-start.bat config\server.properties
```

---

### 2. Run API

```bash
go run main.go
```

---

### 3. Run Worker

```bash
go run worker.go
```

---

### 4. Test API (Postman / Curl)

Create notification:

```bash
POST http://localhost:8080/notifications
```

---

Example Flow

1. Create notification → status = `pending`
2. Worker picks it → `processing`
3. If success → `sent`
4. If failure → retry → eventually `sent` or `failed`

---

Key Concepts Demonstrated

* Event-driven architecture
* Asynchronous processing
* Message queues (Kafka)
* Retry & failure handling
* Idempotent design
* Concurrent processing

---

##

---

##
