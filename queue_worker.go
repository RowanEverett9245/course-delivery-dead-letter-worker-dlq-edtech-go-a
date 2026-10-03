package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

type CourseDeliveryJob struct {
	CourseID     string    `json:"course_id"`
	LearnerID    string    `json:"learner_id"`
	Deadline     time.Time `json:"deadline"`
	Attempt      int       `json:"attempt"`
	FailureClass string    `json:"failure_class"`
}

type EducatorReport struct {
	CourseID  string `json:"course_id"`
	LearnerID string `json:"learner_id"`
	Outcome   string `json:"outcome"`
	Reason    string `json:"reason"`
}

type Decision string

const (
	DecisionDeliver Decision = "deliver"
	DecisionRetry   Decision = "retry"
	DecisionExpired Decision = "report_expired"
)

func Decide(job CourseDeliveryJob, now time.Time) Decision {
	if !now.Before(job.Deadline) {
		return DecisionExpired
	}
	if job.FailureClass != "" {
		return DecisionRetry
	}
	return DecisionDeliver
}

func ProcessMessage(ctx context.Context, queue *InfraiQueue, message QueueMessage, now time.Time) (EducatorReport, error) {
	var job CourseDeliveryJob
	if err := json.Unmarshal(message.Payload, &job); err != nil {
		return EducatorReport{}, fmt.Errorf("decode course job: %w", err)
	}

	switch Decide(job, now) {
	case DecisionRetry:
		return EducatorReport{}, queue.Nack(ctx, message.MessageID)
	case DecisionExpired:
		report := EducatorReport{
			CourseID: job.CourseID, LearnerID: job.LearnerID,
			Outcome: "not_delivered", Reason: "learner_deadline_passed",
		}
		return report, queue.Ack(ctx, message.MessageID)
	default:
		report := EducatorReport{
			CourseID: job.CourseID, LearnerID: job.LearnerID,
			Outcome: "delivered", Reason: "on_time",
		}
		return report, queue.Ack(ctx, message.MessageID)
	}
}

func run(ctx context.Context, queue *InfraiQueue) error {
	messages, err := queue.Consume(ctx)
	if err != nil {
		return err
	}
	for _, message := range messages {
		report, err := ProcessMessage(ctx, queue, message, time.Now().UTC())
		if err != nil {
			return err
		}
		if report.Outcome != "" {
			encoded, _ := json.Marshal(report)
			fmt.Println(string(encoded))
		}
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: course-dlq work | dlq QUEUE | redrive QUEUE")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	queue := NewInfraiQueue(key)
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "work":
		err = run(ctx, queue)
	case "dlq":
		if len(os.Args) != 3 {
			err = errors.New("usage: course-dlq dlq QUEUE")
			break
		}
		var data json.RawMessage
		data, err = queue.ListDeadLetters(ctx, os.Args[2])
		if err == nil {
			fmt.Println(string(data))
		}
	case "redrive":
		if len(os.Args) != 3 {
			err = errors.New("usage: course-dlq redrive QUEUE")
			break
		}
		err = queue.Redrive(ctx, os.Args[2])
		if err == nil {
			fmt.Println("redrive accepted")
		}
	default:
		err = errors.New("unknown command")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
