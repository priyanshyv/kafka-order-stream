# flexipill-stream

Learning message streams by rebuilding **your** order flow — the one in the
"Order Placement Wiring" doc — on Kafka instead of `await` chains, RabbitMQ,
SQS, and `void`.

Nothing here is a toy topic. Every file maps to something real:

| This repo | Your system |
|---|---|
| `POST /orders/initiateOrder` | `createOrderForMethod` — the nine-step write sequence |
| topic `orders` | what `postOrderCreationJobs()` fans out to |
| topic `order-status` | the `order_status_log` table, as an actual log |
| `email.service.ts` | `sendOrderDetailsEmail` off `processPostOrderCreationJobs` |
| `inventory.service.ts` | `processItemProcurementForOrderV2` — the `void` one |
| `zoho.service.ts` | `createZohoSalesOrderForOrderV2` + a real DLQ |
| `analytics.service.ts` | the SQS `META_EVENT` / `FIREBASE_EVENT` pushes |
| `fraud.service.ts` | the consumer you can't build today without a backfill script |

---

## Setup

```bash
docker compose up -d      # Redpanda (Kafka-compatible) + a web console
npm install
npm run topics            # creates orders(3p), order-status(3p), orders-dlq(1p)
```

Web console at **http://localhost:8080** — browse topics, partitions, individual
messages, and consumer group lag. Keep it open; it's your microscope.

Each service is its own terminal. That's deliberate — you need to kill them
independently.

```
Terminal 1:  npm run api          Terminal 4:  npm run analytics
Terminal 2:  npm run email        Terminal 5:  npm run zoho
Terminal 3:  npm run inventory    Terminal 6:  npm run offsets    (run on demand)
```

---

## Phase 1 — produce and consume

```bash
npm run api          # terminal 1
npm run email        # terminal 2
npm run seed -- 5    # terminal 3
```

**Watch:** `email-service` prints `p1@0`, `p0@2`, `p2@1` … That's
partition@offset. Every message has a permanent address in the log.

**Notice what didn't happen:** `api.ts` has no idea `email.service.ts` exists.
Compare to `postOrderCreationJobs()`, which names every downstream by hand and
must be edited to add one.

---

## Phase 2 — the "why not a queue" moment

Start `npm run analytics` too. Seed again.

**Both services get every message.** With one RabbitMQ queue, whichever worker
grabs a message takes it away from everyone else — so today you need a separate
queue per consumer, and the producer must know to publish to each.

Now the second half, and this is the part people misunderstand:

```bash
npm run analytics     # terminal 4
npm run analytics     # terminal 5 — a SECOND copy, same file
npm run seed -- 9
```

Two processes, same `groupId` → Kafka splits the 3 partitions between them.
Each message goes to **exactly one**. Same code, opposite behaviour — decided
entirely by `groupId`. That one string is the whole broadcast-vs-load-balance
switch.

**Then kill terminal 3 (`inventory`), seed 10 more, and run `npm run offsets`.**
Its lag grows. Nothing is lost. Restart it — it resumes at the exact message it
died on and drains to zero. That's the durable log in one table.

---

## Phase 3 — partition keys and ordering

Kafka orders messages **within a partition**, not within a topic. Same key →
same partition → guaranteed order. `publisher.ts` keys on `orderId`, which is
why an order's `OrderPlaced → PaymentConfirmed → DISPATCHED` can never arrive
scrambled.

Break it on purpose:

```bash
curl -X POST localhost:3000/lab/unkeyed
```

Five transitions for one order, published with **no key**, so they scatter.
`inventory-service` will shout:

```
OUT OF ORDER for ORD-UNKEYED-1007: got seq 3 (QC_COMPLETED) after seq 4.
  partition=0. This is what an unkeyed producer buys you.
```

This is the single most common production bug in event systems, and you just
caused it in one command.

**Ask yourself:** in your real system, what would the key be for
`order_status_log`? What breaks if you key on `user_id` instead?

---

## Phase 4 — replay

The thing a queue **cannot** do.

```bash
npm run seed -- 20     # build up history first
npm run fraud          # a brand new service, written after the fact
```

`fraud.service.ts` joins with `fromBeginning: true` and processes every order
ever placed — including all the ones from before the file existed. It labels
each one `HISTORICAL` or `live` by comparing the broker timestamp to now.

Today, shipping a new consumer means writing a Postgres backfill script and
praying your query matches what the live path does. Here the backfill *is* the
live path.

Now re-run it with a new group id and watch the entire history replay again:

```bash
GROUP_ID=fraud-detection-v2 npm run fraud
```

That's how you re-run a consumer after fixing a bug in it. Or rewind one in
place (stop it first — Kafka refuses to move offsets under a live member):

```bash
npm run reset -- email-service
npm run email       # re-sends every confirmation email from the start
```

---

## Phase 5 — failure, retries, DLQ

```bash
FAIL_RATE=0.5 npm run zoho
npm run seed -- 10
```

Half the "Zoho" calls throw. Watch the retry ladder, then the survivors:

```
attempt 1/3 failed: Zoho API 503 - retrying
attempt 2/3 failed: Zoho API 503 - retrying
ORD-1006 -> Zoho sales order SO-1006 created on attempt 3
giving up after 3 attempts -> orders-dlq: Zoho API 503 (attempt 3)
```

Read the graveyard, with full provenance:

```bash
npm run dlq
# OrderPlaced ORD-1001 killed by zoho-service: Zoho API 503 (was orders p1@0)

REPLAY=1 npm run dlq     # push them back onto the original topic
```

**Compare to today.** Your doc: `postOrderCreationJobs()`'s body sits in one
`try` whose `catch` only logs `POST_ORDER_CREATION_JOBS_ERROR`. A thrown
warehouse lookup means the customer gets a clean 200, the order exists and is
paid for, and the RabbitMQ message at the bottom of the function was never
sent — rescued only by a cron.

Here that same failure: retried three times, parked with its original
topic/partition/offset, replayable with one command, and it never blocked the
messages behind it.

**The tax:** retries mean `handle()` runs more than once for the same message.
That's at-least-once delivery, and it's why `inventory.service.ts` keeps a
`processedEvents` set keyed on `meta.eventId`. Delete that set, set
`FAIL_RATE=0.9`, and watch your stock double-decrement. Do it once — it's the
lesson that sticks.

---

## The tool you'll use most

```bash
npm run offsets
```

```
TOPIC END OFFSETS (how much history exists)
  orders         p0=5  p1=1  p2=3

CONSUMER GROUPS
  email-service
    orders  p0  committed=5  end=5  (caught up)
    orders  p1  committed=0  end=1  <-- 1 behind
```

Committed vs end vs lag, per group per partition. Almost every question you
have while learning this ("did it get the message?", "why is it slow?", "did
the rewind work?") is answered by this table.

---

## What this exercise is actually arguing

Your doc ends with three soft spots: no transaction across the nine writes,
`void` on side effects that matter, and a catch-all that turns a fan-out
failure into a silent success — and correctly notes **none of them are fixed by
adopting a message stream.**

That's still true, and this repo doesn't pretend otherwise. Steps 1–7 here are
just as unprotected as they are in production. What a stream changes is only
step 8 onward: one keyed append replaces a RabbitMQ publish plus a Zoho HTTP
call plus a `void`, and every consumer gets durability, retries, replay, and
its own independent position for free.

Worth holding both ideas at once: `order_status_log` already gives you an
append-only ordered event log you can **join in SQL** — which Kafka can't. The
honest case for a stream is fan-out and replay, not "we need an event log."
You already have one.

## Suggested order of attack

1. Run Phase 1–2 exactly as written. Don't skip the kill-and-restart.
2. Break Phase 3 yourself before reading the explanation.
3. In Phase 5, delete the idempotency set and watch it corrupt.
4. Then write a **sixth** consumer with no help — a `loyalty-service` that
   activates membership on `DELIVERED` — and replay all history through it.
   If you can do that without re-reading this file, you've got it.
