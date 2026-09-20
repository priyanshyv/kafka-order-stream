import { ensureTopics, client } from '../lib/kafka.js';
import { logger } from '../lib/log.js';

const log = logger('topics');
await ensureTopics();

const admin = client('topics').admin();
await admin.connect();
const meta = await admin.fetchTopicMetadata();
for (const t of meta.topics) {
  if (t.name.startsWith('_')) continue;
  log.info(`${t.name}: ${t.partitions.length} partition(s)`);
}
await admin.disconnect();
