/**
 * PHASE 5 - read the graveyard.
 *
 * Prints everything parked in orders-dlq with the headers that tell you exactly
 * which topic/partition/offset it came from and which service killed it.
 * Set REPLAY=1 to push each one back onto its original topic.
 */
import { runConsumer, client } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';

const log = logger('dlq-inspector');
const REPLAY = process.env.REPLAY === '1';

const producer = client('dlq-replayer').producer();
if (REPLAY) await producer.connect();

await runConsumer('dlq-inspector', {
  groupId: REPLAY ? 'dlq-replayer' : 'dlq-inspector',
  topics: [TOPICS.dlq],
  fromBeginning: true,
  handle: async ({ event, raw }) => {
    const h = raw.message.headers ?? {};
    const get = (k: string) => h[k]?.toString() ?? '?';
    log.error(
      `${event.type} ${event.orderId} killed by ${get('x-failed-by')}: ${get('x-error')} ` +
        `(was ${get('x-original-topic')} p${get('x-original-partition')}@${get('x-original-offset')})`,
    );
    if (REPLAY) {
      await producer.send({
        topic: get('x-original-topic'),
        messages: [{ key: raw.message.key, value: raw.message.value }],
      });
      log.info(`replayed ${event.orderId} back onto ${get('x-original-topic')}`);
    }
  },
});
