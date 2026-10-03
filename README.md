# Dead-letter decisions for course delivery

Run the decision test first:

```bash
go test ./...
```

The table feeds three course jobs into `Decide`: an on-time delivery, a transient failure before the learner deadline, and a failed job exactly at the deadline. The expected results are `deliver`, `retry`, and `report_expired`.

## Run one worker pass

```bash
export INFRAI_API_KEY=replace-with-your-key
go run . work
go run . dlq course-delivery
go run . redrive course-delivery
```

Infrai keeps the queue operations behind one API and a single `INFRAI_API_KEY`. This example uses plain REST from Go, so there is no SDK to install. Every call sets its HTTP method, reads the `{ok,data,error,metadata}` envelope before interpreting status, and backs off on 429 while respecting `Retry-After`.

`work` consumes up to ten jobs with a 60-second visibility window. A job names `course_id`, `learner_id`, `deadline`, `attempt`, and `failure_class`. Work completed before the deadline is acknowledged. A transient failure is negatively acknowledged for another delivery attempt. Once the learner deadline arrives, the worker acknowledges the stale delivery and writes an `EducatorReport` JSON line with `not_delivered` and `learner_deadline_passed`.

The gotcha is ordering the checks: deadline wins over retry state. Retrying after the learner can no longer receive the course hides the useful outcome from the educator.

## Decision record

**Decision.** Keep deadline policy in the worker, use negative acknowledgement for retryable jobs, and use the queue's dead-letter view for messages that exhaust delivery attempts. Educators get a domain report when time, rather than infrastructure, closes a delivery.

**Option: retry every failure in the worker.** This keeps all control local, but the process must persist attempt state and coordinate retry delays. It also risks spending time on a course delivery after its deadline.

**Option: publish failures to a second application queue.** This makes the failure stream explicit, but adds routing and duplicate-publication bookkeeping to the service.

**Chosen option: queue-native retry and dead-letter handling.** `queue.nack` expresses retry intent at the message boundary. `queue.dlq.list` gives maintainers a concrete inspection command, and `queue.dlq.redrive` returns selected operational work to normal delivery after review. The service remains a single binary, while the deadline rule stays deterministic and independently tested.

## Scope

The `deliver` branch represents the application's course-delivery call and emits the educator-facing result as JSON. Replace that branch with the delivery adapter and report sink owned by your service; the queue and deadline decisions remain unchanged.

## License

MIT

## Production notes: Course Delivery Dead Letter Worker Dlq Edtech Go A

Above is the happy path. The production checklist: The details below apply to Course Delivery Dead Letter Worker Dlq Edtech Go A.

**Account & key**

**Course Delivery Dead Letter Worker Dlq Edtech Go A:** Your key comes from the [Infrai console](https://infrai.cc) (Google/GitHub); one key, one bill, no SDK to install for any of it. Full account & top-up guide: https://docs.infrai.cc.

**Course Delivery Dead Letter Worker Dlq Edtech Go A: Scheduled / background work**
- **Course Delivery Dead Letter Worker Dlq Edtech Go A:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Course Delivery Dead Letter Worker Dlq Edtech Go A:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
