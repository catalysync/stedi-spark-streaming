# Evaluate Human Balance with Spark Streaming (nd029 P2)

Reads customer events from `redis-server` and `stedi-events` Kafka topics,
parses the JSON + Base64-decodes the Redis envelopes, joins on email,
and sinks the resulting customer-risk payload to the `customer-risk`
topic that the STEDI risk-graph dashboard subscribes to.

## Files

```
scripts/
  sparkpyeventskafkastreamtoconsole.py   — stedi-events → console (criterion 18002)
  sparkpyrediskafkastreamtoconsole.py    — redis-server → console (criterion 18003)
  sparkpykafkajoin.py                    — join + sink to customer-risk (main deliverable)
screenshots/
  spark-cluster.png                      — Spark Worker registered with master
  customer-risk-graph-1.png              — STEDI graph with data points
  customer-risk-graph-2.png              — STEDI graph after the data shifts
```

## Rubric coverage

| ID    | Criterion                                      | Where                                                                 |
|-------|------------------------------------------------|-----------------------------------------------------------------------|
| 17989 | Start a Spark cluster                          | docker-compose Spark master + worker (see `spark-cluster.png`)        |
| 17991 | Spark application runs on the cluster          | `submit-event-kafkajoin.sh` runs `sparkpykafkajoin.py`                |
| 17992 | Continuous output of customer risk scores      | `customer-risk-graph-1.png` + `customer-risk-graph-2.png`             |
| 17994 | Spark consumes `redis-server` topic            | `sparkpykafkajoin.py` — `redisServerRawStreamingDF`                   |
| 17995 | Spark consumes `stedi-events` topic            | `sparkpykafkajoin.py` — `stediEventsRawStreamingDF`                   |
| 17996 | JSON + Base64 decode                           | `unbase64(col("encodedCustomer")).cast("string")` + `from_json`       |
| 17997 | Two distinct streaming dataframes              | `emailAndBirthYear` + `emailAndRiskScore`                             |
| 17998 | Join → `riskScoreByBirthYear`                  | `emailAndRiskScore.join(emailAndBirthYear, "email")`                  |
| 18000 | Sink to Kafka                                  | `.writeStream.format("kafka") ... .option("topic", "customer-risk")`  |
| 18001 | JSON-formatted payload                         | `to_json(struct(customer, Score, email, birthYear))`                  |
| 18002 | Console output with customer + score           | `sparkpyeventskafkastreamtoconsole.py` writes to console              |
| 18003 | Console output with email + birthYear          | `sparkpyrediskafkastreamtoconsole.py` writes to console               |

## Running in the Udacity workspace

```
/home/workspace/submit-redis-kafka-streaming.sh    # terminal 1 (criterion 18003)
/home/workspace/submit-event-kafkastreaming.sh     # terminal 2 (criterion 18002)
/home/workspace/submit-event-kafkajoin.sh          # terminal 3 (deliverable)
```

Then enable simulation at <http://localhost:3000>, wait for the dashboard
to render risk scores, and capture two screenshots showing different data
points as evidence for criterion 17992.

## Notes on the data flow

1. The STEDI Java app writes new Customer records to a Redis sorted set.
2. The Redis Kafka-Source connector tails Redis and republishes each
   Customer record onto the `redis-server` Kafka topic, wrapped in a
   Base64-encoded `zSetEntries[].element` envelope.
3. The STEDI Java app independently emits `{customer, score, riskDate}`
   JSON onto `stedi-events` whenever a customer accumulates ≥4 step
   assessments.
4. `sparkpykafkajoin.py` consumes both topics, base64-decodes the
   Customer record out of `redis-server`, extracts the birth year from
   the JSON, joins the two streams on `email`, and publishes the
   `{customer, Score, email, birthYear}` payload onto `customer-risk`.
5. The STEDI app subscribes to `customer-risk` and pushes each record
   to the browser over a websocket, which the risk-graph page plots.
