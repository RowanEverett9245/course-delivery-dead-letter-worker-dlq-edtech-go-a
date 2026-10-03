package main

import (
	"testing"
	"time"
)

func TestDecideCourseDelivery(t *testing.T) {
	now := time.Date(2026, time.September, 13, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		job  CourseDeliveryJob
		want Decision
	}{
		{
			name: "healthy job before deadline is delivered",
			job:  CourseDeliveryJob{Deadline: now.Add(time.Hour)},
			want: DecisionDeliver,
		},
		{
			name: "transient failure before deadline is retried",
			job:  CourseDeliveryJob{Deadline: now.Add(time.Hour), Attempt: 2, FailureClass: "transient"},
			want: DecisionRetry,
		},
		{
			name: "failed job at deadline becomes an educator report",
			job:  CourseDeliveryJob{Deadline: now, Attempt: 4, FailureClass: "transient"},
			want: DecisionExpired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Decide(test.job, now); got != test.want {
				t.Fatalf("Decide() = %q, want %q", got, test.want)
			}
		})
	}
}
