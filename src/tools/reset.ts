/**
 * Rewind a consumer group to the start of the log, so it reprocesses history.
 *
 *   npm run reset -- email-service           # rewind to offset 0
 *   npm run reset -- email-service latest    # skip to the end instead
 *
 * The group must be STOPPED - Kafka refuses to move offsets under a live
 * member, which is a feature, not an obstacle.
 */
import { client } from '../lib/kafka.js';
import { TOPICS } from '../config.js';

const groupId = process.argv[2];
const where = process.argv[3] === 'latest' ? 'latest' : 'earliest';
if (!groupId) {
  console.error('usage: npm run reset -- <groupId> [earliest|latest]');
  process.exit(1);
}

const admin = client('reset').admin();
await admin.connect();
for (const topic of [TOPICS.orders, TOPICS.orderStatus, TOPICS.dlq]) {
  try {
    await admin.resetOffsets({ groupId, topic, earliest: where === 'earliest' });
    console.log(`reset ${groupId} on ${topic} -> ${where}`);
  } catch (err) {
    console.error(`skipped ${topic}: ${(err as Error).message}`);
  }
}
await admin.disconnect();
console.log('\nnow restart that consumer and watch it re-read.');
