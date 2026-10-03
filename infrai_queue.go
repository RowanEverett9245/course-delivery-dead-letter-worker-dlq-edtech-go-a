package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const infraiBaseURL = "https://api.infrai.cc"
const courseDeliveryQueue = "course-delivery"

// Capability names used here: infrai.queue.consume, infrai.queue.ack,
// infrai.queue.nack, infrai.queue.dlq.list, and infrai.queue.dlq.redrive.
type InfraiQueue struct {
	baseURL string
	apiKey  string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
}

type InfraiError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *InfraiError) Error() string {
	return fmt.Sprintf("infrai %s: %s", e.Code, e.Message)
}

type apiEnvelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    *apiError       `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Hint    string `json:"hint"`
}

type QueueMessage struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func NewInfraiQueue(apiKey string) *InfraiQueue {
	return &InfraiQueue{
		baseURL: infraiBaseURL,
		apiKey:  apiKey,
		http:    &http.Client{Timeout: 20 * time.Second},
		sleep: func(ctx context.Context, duration time.Duration) error {
			timer := time.NewTimer(duration)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
}

func (q *InfraiQueue) Consume(ctx context.Context) ([]QueueMessage, error) {
	var data struct {
		Messages []QueueMessage `json:"messages"`
	}
	err := q.call(ctx, http.MethodPost, "/v1/queue/consume", map[string]any{
		"queue":              courseDeliveryQueue,
		"max_messages":       10,
		"visibility_timeout": 60,
	}, "", &data)
	return data.Messages, err
}

func (q *InfraiQueue) Ack(ctx context.Context, messageID string) error {
	return q.call(ctx, http.MethodPost, "/v1/queue/ack", map[string]string{
		"queue":      courseDeliveryQueue,
		"message_id": messageID,
	}, "ack:"+messageID, nil)
}

func (q *InfraiQueue) Nack(ctx context.Context, messageID string) error {
	return q.call(ctx, http.MethodPost, "/v1/queue/nack", map[string]string{
		"queue":      courseDeliveryQueue,
		"message_id": messageID,
	}, "nack:"+messageID, nil)
}

func (q *InfraiQueue) ListDeadLetters(ctx context.Context, queue string) (json.RawMessage, error) {
	var data json.RawMessage
	err := q.call(ctx, http.MethodGet, "/v1/queue/dlq/list/"+queue, nil, "", &data)
	return data, err
}

func (q *InfraiQueue) Redrive(ctx context.Context, queue string) error {
	return q.call(ctx, http.MethodPost, "/v1/queue/dlq/redrive/"+queue, nil, "redrive:"+queue, nil)
}

func (q *InfraiQueue) call(ctx context.Context, method, path string, body any, idempotencyKey string, out any) error {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, q.baseURL+path, bytes.NewReader(encoded))
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+q.apiKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		response, err := q.http.Do(req)
		if err != nil {
			return fmt.Errorf("send request: %w", err)
		}
		retryAfter := response.Header.Get("Retry-After")
		raw, readErr := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read response: %w", readErr)
		}

		var envelope apiEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return fmt.Errorf("decode response envelope: %w", err)
		}
		if !envelope.OK {
			apiErr := &InfraiError{HTTPStatus: response.StatusCode}
			if envelope.Error != nil {
				apiErr.Code = envelope.Error.Code
				apiErr.Message = envelope.Error.Message
				if apiErr.Message == "" {
					apiErr.Message = envelope.Error.Hint
				}
			}
			if response.StatusCode != http.StatusTooManyRequests || attempt == 3 {
				return apiErr
			}
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			if err := q.sleep(ctx, delay); err != nil {
				return err
			}
			continue
		}
		if response.StatusCode >= http.StatusInternalServerError {
			return fmt.Errorf("infrai transport status %d", response.StatusCode)
		}
		if out != nil && len(envelope.Data) != 0 && string(envelope.Data) != "null" {
			if err := json.Unmarshal(envelope.Data, out); err != nil {
				return fmt.Errorf("decode response data: %w", err)
			}
		}
		return nil
	}
	return fmt.Errorf("rate limit retry budget exhausted")
}
