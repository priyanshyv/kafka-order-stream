import { Kafka, logLevel, type Consumer, type EachMessagePayload } from 'kafkajs';
import { BROKERS, TOPICS, PARTITIONS } from '../config.js';
import type { OrderEvent } from '../events.js';
import { logger } from './log.js';

export function client(clientId: string): Kafka {
  return new Kafka({ clientId, brokers: BROKERS, logLevel: logLevel.NOTHING });
}

export async function ensureTopics(): Promise<void> {
  const admin = client('admin').admin();
  await admin.connect();
  const existing = await admin.listTopics();
  const wanted = [
    { topic: TOPICS.orders, numPartitions: PARTITIONS, replicationFactor: 1 },
    { topic: TOPICS.orderStatus, numPartitions: PARTITIONS, replicationFactor: 1 },
    { topic: TOPICS.dlq, numPartitions: 1, replicationFactor: 1 },
  ].filter((t) => !existing.includes(t.topic));
  if (wanted.length) await admin.createTopics({ topics: wanted, waitForLeaders: true });
  await admin.disconnect();
}

export interface HandlerCtx {
  event: OrderEvent;
  /** Raw Kafka envelope - partition, offset, timestamp, key. Look at these. */
  raw: EachMessagePayload;
  /** How many times THIS message has been handed to you (Phase 5). */
  attempt: number;
}

export interface ConsumerOptions {
  /** The consumer group id. THE most important knob in this whole repo. */
  groupId: string;
  topics: string[];
  /**
   * true  -> on first run, read the topic from offset 0 (Phase 4: replay)
   * false -> on first run, only read messages produced from now on
   * Ignored entirely once the group has committed an offset.
   */
  fromBeginning?: boolean;
  /** Phase 5: retry in-process this many times before the DLQ. */
  maxAttempts?: number;
  handle: (ctx: HandlerCtx) => Promise<void>;
}

/**
 * Shared consumer harness. Every service in src/consumers uses this so the
 * only thing that differs between them is the groupId and the handler body -
 * which is exactly the point of Phase 2.
 */
export async function runConsumer(name: string, opts: ConsumerOptions): Promise<Consumer> {
  const log = logger(name);
  const kafka = client(name);
  const consumer = kafka.consumer({ groupId: opts.groupId });
  const producer = kafka.producer();
  const maxAttempts = opts.maxAttempts ?? 1;

  await consumer.connect();
  await producer.connect();
  for (const topic of opts.topics) {
    await consumer.subscribe({ topic, fromBeginning: opts.fromBeginning ?? false });
  }

  log.info(
    `up. group=${opts.groupId} topics=${opts.topics.join(',')} fromBeginning=${opts.fromBeginning ?? false}`,
  );

  await consumer.run({
    eachMessage: async (raw) => {
      const value = raw.message.value?.toString();
      if (!value) return;
      const event = JSON.parse(value) as OrderEvent;

      for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
          await opts.handle({ event, raw, attempt });
          return;
        } catch (err) {
          const reason = err instanceof Error ? err.message : String(err);
          if (attempt < maxAttempts) {
            log.warn(`attempt ${attempt}/${maxAttempts} failed: ${reason} - retrying`);
            await sleep(200 * attempt);
            continue;
          }
          // Out of retries. Park it, commit past it, keep the partition moving.
          log.error(`giving up after ${maxAttempts} attempts -> ${TOPICS.dlq}: ${reason}`);
          // Carry the replay counter forward. If this message only reached us
          // BECAUSE someone replayed it from the DLQ, that history has to
          // survive the second death - otherwise replaying into a consumer
          // that is still broken loops forever.
          const replayCount = raw.message.headers?.['x-replay-count']?.toString() ?? '0';
          await producer.send({
            topic: TOPICS.dlq,
            messages: [
              {
                key: raw.message.key,
                value,
                headers: {
                  'x-original-topic': raw.topic,
                  'x-original-partition': String(raw.partition),
                  'x-original-offset': raw.message.offset,
                  'x-failed-by': name,
                  'x-error': reason,
                  'x-replay-count': replayCount,
                },
              },
            ],
          });
        }
      }
    },
  });

  const shutdown = async () => {
    log.info('shutting down (offsets already committed - restart resumes here)');
    await consumer.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  return consumer;
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
