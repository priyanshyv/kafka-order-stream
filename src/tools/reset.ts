/**
 * Rewind a consumer group so it reprocesses history.
 *
 *   npm run reset -- email-service           # rewind to the start of the log
 *   npm run reset -- email-service latest    # skip to the end instead
 *
 * The group must be STOPPED - Kafka refuses to move offsets under a live
 * member, which is a feature, not an obstacle.
 *
 * Note we resolve "earliest" into a CONCRETE offset per partition rather than
 * using admin.resetOffsets(). That helper stores Kafka's -2 sentinel, and a
 * consumer configured with `fromBeginning: false` then treats the group as
 * having no valid position and jumps to the END instead - so the replay you
 * asked for silently does not happen. Writing real numbers removes the
 * ambiguity: there is nothing left for the consumer to interpret.
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
    // low = first offset still retained, high = next offset to be written.
    const bounds = await admin.fetchTopicOffsets(topic);
    const partitions = bounds.map((b) => ({
      partition: b.partition,
      offset: where === 'earliest' ? b.low : b.high,
    }));
    await admin.setOffsets({ groupId, topic, partitions });
    const shown = partitions.map((p) => `p${p.partition}=${p.offset}`).join(' ');
    console.log(`reset ${groupId} on ${topic.padEnd(14)} -> ${where.padEnd(8)} ${shown}`);
  } catch (err) {
    console.error(`skipped ${topic}: ${(err as Error).message}`);
  }
}

await admin.disconnect();
console.log('\nnow restart that consumer and watch it re-read.');
