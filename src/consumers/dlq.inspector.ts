/**
 * PHASE 5 - read the graveyard.
 *
 * Prints everything parked in orders-dlq with the headers that tell you exactly
 * which topic/partition/offset it came from and which service killed it.
 *
 *   npm run dlq                 inspect only
 *   REPLAY=1 npm run dlq        push each one back onto its original topic
 *
 * REPLAY IS NOT SAFE BY DEFAULT, and the guard below is the reason.
 *
 * If you replay a message into a consumer that is still broken, it fails
 * again, lands back in the DLQ, and this replayer - which is subscribed to the
 * DLQ - picks it up and replays it again. That is an infinite loop, and it is
 * not theoretical: with FAIL_RATE=1 it turned 3 orders into 35 dead-letters
 * and 32 replays in under a minute, accelerating.
 *
 * So every replayed message carries x-replay-count, kafka.ts preserves it when
 * the message dies a second time, and past MAX_REPLAYS we refuse. Fix the
 * consumer first; the DLQ will wait.
 */
import { runConsumer, client } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';

const log = logger('dlq-inspector');
const REPLAY = process.env.REPLAY === '1';
const MAX_REPLAYS = Number(process.env.MAX_REPLAYS ?? 3);

const producer = client('dlq-replayer').producer();
if (REPLAY) await producer.connect();

let refused = 0;

await runConsumer('dlq-inspector', {
  groupId: REPLAY ? 'dlq-replayer' : 'dlq-inspector',
  topics: [TOPICS.dlq],
  fromBeginning: true,
  handle: async ({ event, raw }) => {
    const h = raw.message.headers ?? {};
    const get = (k: string) => h[k]?.toString() ?? '?';
    const replays = Number(h['x-replay-count']?.toString() ?? '0');

    log.error(
      `${event.type} ${event.orderId} killed by ${get('x-failed-by')}: ${get('x-error')} ` +
        `(was ${get('x-original-topic')} p${get('x-original-partition')}@${get('x-original-offset')}` +
        `${replays > 0 ? `, replayed ${replays}x already` : ''})`,
    );

    if (!REPLAY) return;

    if (replays >= MAX_REPLAYS) {
      refused += 1;
      log.warn(
        `refusing to replay ${event.orderId} - already retried ${replays}x. ` +
          `Fix the consumer, then raise MAX_REPLAYS. (${refused} refused so far)`,
      );
      return;
    }

    await producer.send({
      topic: get('x-original-topic'),
      messages: [
        {
          key: raw.message.key,
          value: raw.message.value,
          headers: { 'x-replay-count': String(replays + 1) },
        },
      ],
    });
    log.info(`replayed ${event.orderId} back onto ${get('x-original-topic')} (replay ${replays + 1}/${MAX_REPLAYS})`);
  },
});
