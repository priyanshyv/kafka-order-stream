/**
 * PHASE 3 - ordering, partition keys, and idempotency.
 *
 * Inventory is the side effect that actually costs money when it goes wrong,
 * which makes it the honest place to argue for a stream.
 *
 * A common queue-based shape: checkout publishes, a worker picks it up, calls
 * an external ERP, and only then fires the stock decrement - often as a
 * fire-and-forget call, so if it throws nothing retries and the item stays
 * sellable to someone else.
 *
 * Here it's one hop, it commits an offset, and if it crashes it resumes from
 * the exact message it died on.
 */
import { runConsumer } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';
import { isType } from '../events.js';

const log = logger('inventory-service');

const stock = new Map<string, number>();
const getStock = (sku: string) => stock.get(sku) ?? 100;

/**
 * Consumers see at-least-once delivery: after a crash, Kafka replays from the
 * last COMMITTED offset, so the message you were mid-way through arrives again.
 * Without this set, a rebalance silently double-decrements your stock.
 */
const processedEvents = new Set<string>();

/** Per-order sequence tracker - proves keyed messages arrive in order. */
const lastSeq = new Map<string, number>();

await runConsumer('inventory-service', {
  groupId: 'inventory-service',
  topics: [TOPICS.orders, TOPICS.orderStatus],
  handle: async ({ event, raw }) => {
    if (processedEvents.has(event.meta.eventId)) {
      log.warn(`duplicate ${event.meta.eventId} - skipping (idempotency saved you)`);
      return;
    }

    if (isType(event, 'OrderPlaced')) {
      for (const item of event.items) {
        const before = getStock(item.sku);
        stock.set(item.sku, before - item.qty);
        log.info(`${event.orderId} decrement ${item.sku} ${before} -> ${before - item.qty} [p${raw.partition}]`);
      }
    }

    if (isType(event, 'OrderStatusChanged')) {
      const prev = lastSeq.get(event.orderId) ?? 0;
      if (event.sequence < prev) {
        // Only possible if the message was published WITHOUT a key.
        log.error(
          `OUT OF ORDER for ${event.orderId}: got seq ${event.sequence} (${event.to}) after seq ${prev}. ` +
            `partition=${raw.partition}. This is what an unkeyed producer buys you.`,
        );
      } else {
        log.info(`${event.orderId} seq ${event.sequence} ${event.from ?? 'null'} -> ${event.to} [p${raw.partition}]`);
      }
      lastSeq.set(event.orderId, Math.max(prev, event.sequence));

      if (event.to === 'CANCELLED') log.info(`${event.orderId} cancelled - releasing stock`);
    }

    processedEvents.add(event.meta.eventId);
  },
});
