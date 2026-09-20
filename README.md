# kafka-order-stream

Learning Kafka by rebuilding a realistic e-commerce order flow — checkout →
payment → fulfilment — as a message stream instead of an `await` chain plus a
pile of queues.

Not a toy `hello-world` topic. It's the flow every commerce backend actually
has, with the failure modes that flow actually has, so each Kafka concept shows
up as the answer to a problem you've already felt.

## The problem it's modelled on

A typical checkout does a sequence of local writes, then fans out to everything
downstream — and that fan-out is usually written by hand inside the checkout
path:

```ts
async function fanOutAfterOrderPlaced(order) {
  await assignWarehouse(order);
  await rabbit.publish('ORDER_PLACED', order);   // inventory, eventually
  await externalErp.createSalesOrder(order);     // slow, flaky, third-party
  void  sendConfirmationEmail(order);            // fire and forget
  void  pushAnalyticsEvent(order);               // fire and forget
}
```

Three things are wrong with this shape, and all three are common:

1. **Adding a consumer means editing checkout** — the riskiest file you own.
2. **`void` means no retries.** If the inventory decrement throws, nothing
   retries it and nobody finds out.
3. **One `try/catch` around the lot** means a failure halfway through returns a
   clean `200` to the customer with half the downstream work silently skipped.

This repo rebuilds that fan-out as **one keyed append to a log**, and then walks
through what you get for free once you do.

## What's here

| File | Role |
|---|---|
| `src/producer/api.ts` | checkout — the only producer |
| `src/producer/publisher.ts` | the keyed `publish()`, and a deliberately broken unkeyed one |
| `src/lib/kafka.ts` | shared consumer harness: groups, retries, DLQ |
| `src/consumers/email.service.ts` | simplest consumer |
| `src/consumers/analytics.service.ts` | second independent group — run two copies |
| `src/consumers/inventory.service.ts` | ordering + idempotency, on the side effect that costs money |
| `src/consumers/erp.service.ts` | a flaky third-party API + dead-letter topic |
| `src/consumers/fraud.service.ts` | joins late, replays all history |
| `src/tools/offsets.ts` | committed vs end vs lag, per group per partition |

Three topics: `orders` (3 partitions), `order-status` (3), `orders-dlq` (1).

---

## Setup

```bash
docker compose up -d      # Redpanda (Kafka-compatible) + a web console
npm install
npm run topics
```

Web console at **http://localhost:8080** — browse topics, partitions, individual
messages, and consumer group lag. Keep it open; it makes the abstract parts
visible.

Each service runs in its own terminal. That's deliberate — the whole point is
killing one without touching the others.

```
Terminal 1:  npm run api          Terminal 4:  npm run analytics
Terminal 2:  npm run email        Terminal 5:  npm run erp
Terminal 3:  npm run inventory    Terminal 6:  npm run offsets   (on demand)
```

---

## Phase 1 — produce and consume

```bash
npm run api
npm run email
npm run seed -- 5
```

```
[order-service]  ORD-1001 placed (COD, Rs.240) -> partition 1 offset 1
[email-service]  p1@1 -> confirmation email to user-7 for ORD-1001 (Rs.240)
```

`p1@1` = partition 1, offset 1 — a permanent address in an append-only file.
Offsets never reset and never get reused.

**Now grep `api.ts` for the word "email".** It isn't there. The producer states
that something happened; it has no idea who cares.

---

## Phase 2 — consumer groups

The single most important field in the repo is `groupId`, and it does two jobs:
it names the bookmark, and it declares a team.

**Different group names → everyone gets everything:**

```bash
npm run email        # groupId: 'email-service'
npm run analytics    # groupId: 'analytics-service'
npm run seed -- 3
```

Both print the same offsets. Nothing was copied — the log is read-only, and
each group has its own bookmark row on the broker.

**Same group name → the work splits:**

```bash
npm run analytics    # terminal A
npm run analytics    # terminal B — same file, same groupId
npm run seed -- 9
```

```
COPY 1                        COPY 2
ORD-1007 [p2 offset=5]         ORD-1008 [p1 offset=3]
ORD-1009 [p0 offset=9]         ORD-1010 [p1 offset=4]
ORD-1011 [p0 offset=10]
```

Kafka doesn't distribute *messages*, it distributes **partitions** — one owner
each. That's how it gets load-balancing and ordering at the same time.

Consequence: **partition count is your parallelism cap.** 3 partitions means a
4th consumer in that group sits idle forever.

**Then kill a consumer, seed more, and look:**

```bash
npm run offsets
```

```
email-service
  orders  p0  committed=11  end=15   <-- 4 behind
  orders  p1  committed= 5  end= 5   (caught up)
```

Lag is just `end - committed`. Both numbers live on the broker, which is why
you can measure a consumer that isn't running. Restart it and it resumes at the
exact message it died on.

---

## Phase 3 — partition keys and ordering

> **Kafka guarantees order within a partition. Not within a topic.**

Since the key picks the partition, the working rule is: *same key → same
partition → guaranteed order.*

Keyed (`key: orderId`), one order's whole life:

```
ORD-1010 seq 1  null              -> ORDER_CREATED       [p1]
ORD-1010 seq 2  ORDER_CREATED     -> PICKING_COMPLETED   [p1]
ORD-1010 seq 3  PICKING_COMPLETED -> QC_COMPLETED        [p1]
ORD-1010 seq 4  QC_COMPLETED      -> DISPATCHED          [p1]
ORD-1010 seq 5  DISPATCHED        -> DELIVERED           [p1]
```

Same partition every time. Now break it on purpose:

```bash
curl -X POST localhost:3000/lab/unkeyed
```

```json
{"orderId":"ORD-UNKEYED-1011","partitions":[0,1,2,0,1]}
```

```
seq 1 -> ORDER_CREATED      [p0]
seq 2 -> PICKING_COMPLETED  [p1]
seq 5 -> DELIVERED          [p1]     <-- !!
OUT OF ORDER: got seq 3 (QC_COMPLETED) after seq 5.  partition=2
OUT OF ORDER: got seq 4 (DISPATCHED)  after seq 5.  partition=0
```

Delivered before QC. Loyalty activated on an order whose shipment hasn't been
booked. **No exception, no error log, monitoring stays green** — which is
exactly why these bugs survive to production.

Run it five times; you'll get a different scramble each time, and occasionally
the right order by luck.

### Four ways to lose ordering

1. **No key** — events scatter across partitions.
2. **Partition count changed** — `hash % 3` becomes `hash % 6` and *some* keys
   relocate while most don't. Never repartition a keyed topic in production.
3. **Producer retries without `idempotent: true`** — a resend lands after a
   later message. It's one word, and it's off by default.
4. **Concurrency in your own handler** — `void someAsync()` throws away the
   guarantee just as thoroughly as a missing key. Kafka's promise ends at your
   handler's door.

### Choosing the key

The key is both the boundary of your ordering guarantee and the unit of your
parallelism, and those pull in opposite directions:

| Key | Ordered within | Parallelism | Risk |
|---|---|---|---|
| `orderId` | one order | excellent | none really |
| `userId` | a user's orders | fine | heavy users become hot keys |
| `warehouseId` | one warehouse | poor — few values | one busy site jams a partition |
| none | nothing | best | ordering gone |

Ask what the narrowest entity is whose events must not be reordered, then check
no single key is disproportionately hot.

---

## Phase 4 — replay

The thing a queue physically cannot do.

```bash
npm run seed -- 20     # build history first
npm run fraud          # a service written after the fact
```

`fraud.service.ts` joins with `fromBeginning: true` under a brand-new group and
processes every order ever placed, labelling each `HISTORICAL` or `live`:

```
#1 HISTORICAL (15s old) ORD-1001 user=user-1 lifetime=Rs.240
#6 HISTORICAL (14s old) ORD-1006 user=user-2 lifetime=Rs.2050
```

With a queue, a new consumer means a hand-written database backfill that has to
match the live path's logic. Here the backfill **is** the live path — identical
code, different starting offset.

Bump the group name to replay from scratch, e.g. after fixing a bug:

```bash
GROUP_ID=fraud-detection-v2 npm run fraud
```

Or rewind an existing group in place (stop it first — Kafka refuses to move
offsets under a live member):

```bash
npm run reset -- email-service
npm run email
```

---

## Phase 5 — retries, DLQ, idempotency

```bash
FAIL_RATE=0.5 npm run erp
npm run seed -- 10
```

```
attempt 1/3 failed: ERP API 503 - retrying
attempt 2/3 failed: ERP API 503 - retrying
ORD-1006 -> ERP sales order SO-1006 created on attempt 3
giving up after 3 attempts -> orders-dlq: ERP API 503 (attempt 3)
```

Read the graveyard, with full provenance:

```bash
npm run dlq
# OrderPlaced ORD-1001 killed by erp-service: ERP API 503 (was orders p1@0)

REPLAY=1 npm run dlq     # push them back onto the original topic
```

The failure never reached the customer, never blocked the messages behind it,
and never vanished.

**The tax:** the offset commits *after* your handler, so a crash mid-handler
means you get the message again. That's **at-least-once** delivery — Kafka
chose "maybe twice" over "maybe never", because you can defend against twice
and you cannot recover from never.

Defending against it is idempotency, which is why `inventory.service.ts` keeps
a `processedEvents` set keyed on `meta.eventId`. Delete that set, set
`FAIL_RATE=0.9`, and watch stock double-decrement. Worth doing once.

---

## The tool you'll use most

```bash
npm run offsets
```

```
TOPIC END OFFSETS
  orders         p0=5  p1=1  p2=3

CONSUMER GROUPS
  email-service
    orders  p0  committed=5  end=5  (caught up)
    orders  p1  committed=0  end=1  <-- 1 behind
```

Almost every question you'll have while learning this — *did it get the
message? why is it slow? did the rewind work?* — is answered by this table.

---

## Queue or stream?

Streams are not a strict upgrade. The honest split:

| | Queue (SQS / RabbitMQ) | Stream (Kafka) |
|---|---|---|
| after reading | message destroyed | message stays |
| consumers per message | one | unlimited groups |
| replay history | impossible | free |
| parallelism cap | unlimited workers | = partition count |
| retry one message | yes, in isolation | blocks the partition |
| ordering | none | per key |
| ops cost | low | real |

> **Queue when the message is a *task* — "do this thing, once."**
> **Stream when the message is a *fact* — "this happened."**

"Send SMS to user 5" is a task: one worker, then it's done, and you want fifty
workers. That belongs on a queue and should stay there. "Order 1001 was placed"
is a fact: true forever, and five teams have an opinion about it.

Most mature systems run both — a stream carries the facts, and queues fan tasks
out to workers off those facts.

Two things a stream does **not** fix, worth saying out loud:

- **It won't give you a transaction.** If your checkout writes nine rows with no
  transaction, it still does after you adopt Kafka.
- **You may already have an event log.** An append-only `order_status_log` table
  is ordered, queryable, and **joinable in SQL** — which Kafka is not.

The honest case for a stream is narrower and stronger than the usual pitch:
**fan-out without touching checkout, and replay for new consumers.**

---

## Suggested order of attack

1. Run Phases 1–2 as written. Don't skip the kill-and-restart.
2. Break Phase 3 yourself before reading the explanation.
3. In Phase 5, delete the idempotency set and watch it corrupt.
4. Then write a **sixth** consumer with no help — a `loyalty-service` that
   activates membership on `DELIVERED` — and replay all history through it.
   If you can do that without re-reading this file, you've got it.

## Stack

TypeScript · [kafkajs](https://kafka.js.org/) · Redpanda (Kafka API-compatible,
single binary, no Zookeeper) · Express. Everything speaks plain Kafka, so
swapping in real Kafka changes nothing but a broker address.
