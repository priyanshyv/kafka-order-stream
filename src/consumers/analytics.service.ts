/**
 * PHASE 2 - a second, completely independent group.
 *
 * Replaces: the SQS META_EVENT / FIREBASE_EVENT pushes.
 * Run this alongside email-service and watch BOTH receive every message. That
 * is the thing a queue cannot do: with one RabbitMQ queue, whoever reads a
 * message takes it away from everyone else.
 *
 * Then run a SECOND copy of this same file in another terminal. Because they
 * share a groupId, Kafka splits the 3 partitions between the two processes -
 * each message goes to exactly one of them. Same code, two behaviours,
 * decided entirely by groupId.
 */
import { runConsumer } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';

const log = logger(`analytics-${process.pid}`);
let seen = 0;

await runConsumer('analytics-service', {
  groupId: 'analytics-service',
  topics: [TOPICS.orders, TOPICS.orderStatus],
  handle: async ({ event, raw }) => {
    seen += 1;
    log.info(
      `#${seen} ${event.type} ${event.orderId} [topic=${raw.topic} p${raw.partition} offset=${raw.message.offset}]`,
    );
  },
});
