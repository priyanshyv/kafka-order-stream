/**
 * The single most useful command in this repo.
 *
 *   npm run offsets
 *
 * For every consumer group: which partition it owns, where it has committed,
 * where the end of the log is, and the LAG between them. Kill a consumer, seed
 * more orders, run this - the lag grows and nothing is lost. Restart it and
 * the lag drains to zero. That is the whole "durable log" idea in one table.
 */
import { client } from '../lib/kafka.js';
import { TOPICS } from '../config.js';

const admin = client('offsets').admin();
await admin.connect();

const groups = await admin.listGroups();
const topics = [TOPICS.orders, TOPICS.orderStatus, TOPICS.dlq];

const ends = new Map<string, Map<number, string>>();
for (const topic of topics) {
  const offsets = await admin.fetchTopicOffsets(topic);
  ends.set(topic, new Map(offsets.map((o) => [o.partition, o.high])));
}

console.log('\nTOPIC END OFFSETS (how much history exists)');
for (const topic of topics) {
  const parts = [...ends.get(topic)!.entries()].map(([p, hi]) => `p${p}=${hi}`).join('  ');
  console.log(`  ${topic.padEnd(14)} ${parts}`);
}

console.log('\nCONSUMER GROUPS');
if (!groups.groups.length) console.log('  (none - start a consumer)');

for (const g of groups.groups) {
  console.log(`\n  ${g.groupId}`);
  for (const topic of topics) {
    let committed;
    try {
      committed = await admin.fetchOffsets({ groupId: g.groupId, topics: [topic] });
    } catch {
      continue;
    }
    for (const t of committed) {
      for (const p of t.partitions) {
        // -1 = group never read this partition. -2 = an unresolved "earliest"
        // sentinel, which means somebody stored a placeholder instead of a real
        // position; flag it rather than doing arithmetic on it.
        if (p.offset === '-1') continue;
        const high = ends.get(topic)?.get(p.partition) ?? '0';
        if (p.offset === '-2') {
          console.log(`    ${topic.padEnd(14)} p${p.partition}  committed=  (unset)  end=${String(high).padStart(4)}  <-- sentinel, will fall back to auto-offset-reset`);
          continue;
        }
        const lag = Number(high) - Number(p.offset);
        const bar = lag > 0 ? `  <-- ${lag} behind` : '  (caught up)';
        console.log(`    ${topic.padEnd(14)} p${p.partition}  committed=${String(p.offset).padStart(4)}  end=${String(high).padStart(4)}${bar}`);
      }
    }
  }
}
console.log();
await admin.disconnect();
