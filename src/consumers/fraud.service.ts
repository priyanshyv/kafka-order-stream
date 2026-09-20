/**
 * PHASE 4 - REPLAY. The one a queue physically cannot do.
 *
 * Start this only AFTER you have placed a pile of orders. It joins with
 * fromBeginning: true and a brand-new groupId, and processes every order that
 * was ever placed - including the ones from before this file existed.
 *
 * With a queue, shipping a new consumer means backfilling from the database
 * with a hand-written script and hoping your query matches what the live path
 * does. Here the history IS the input, so the backfill and the live path run
 * identical code.
 *
 * Note the groupId ends in a version. Bump it (`-v2`, `-v3`) and you replay
 * the entire history again from scratch - that is how you re-run a consumer
 * after fixing a bug in it.
 */
import { runConsumer } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';
import { isType } from '../events.js';

const log = logger('fraud-detection');
const spendByUser = new Map<string, number>();
let replayed = 0;

await runConsumer('fraud-detection', {
  groupId: process.env.GROUP_ID ?? 'fraud-detection-v1',
  topics: [TOPICS.orders],
  fromBeginning: true,           // <- the entire lesson of Phase 4
  handle: async ({ event, raw }) => {
    if (!isType(event, 'OrderPlaced')) return;
    replayed += 1;
    const spend = (spendByUser.get(event.userId) ?? 0) + event.total;
    spendByUser.set(event.userId, spend);

    const lagMs = Date.now() - Number(raw.message.timestamp);
    const age = lagMs > 5000 ? `HISTORICAL (${Math.round(lagMs / 1000)}s old)` : 'live';

    log.info(`#${replayed} ${age} ${event.orderId} user=${event.userId} lifetime=Rs.${spend}`);
    if (spend > 5000) log.warn(`user ${event.userId} crossed Rs.5000 - flagging for review`);
  },
});
