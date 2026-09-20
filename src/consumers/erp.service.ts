/**
 * PHASE 5 - failure, retries, and the dead-letter topic.
 *
 * Stands in for an external system you don't control: an ERP, a tax service,
 * a shipping carrier's API. It is slow, it is flaky, and it is the single most
 * common source of "the order is paid for but nothing happened downstream".
 *
 * The common anti-pattern this replaces: calling the external API inline during
 * checkout, inside a try/catch that only logs. The customer gets a clean 200,
 * the order exists and is paid for, and the downstream work silently never
 * happened - usually rescued later by a reconciliation cron.
 *
 * Here that same failure cannot touch the customer's response, cannot skip the
 * messages behind it, and cannot vanish - it retries, then parks in orders-dlq
 * with the original topic/partition/offset in the headers so you can replay it.
 *
 * FAIL_RATE controls how flaky the external system is. Default 30%.
 *   FAIL_RATE=1 npm run erp     <- everything dead-letters
 */
import { runConsumer } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';
import { isType } from '../events.js';

const log = logger('erp-service');
const FAIL_RATE = Number(process.env.FAIL_RATE ?? 0.3);

/** A real ERP would reject a duplicate sales order anyway - model that here. */
const salesOrders = new Map<string, string>();

await runConsumer('erp-service', {
  groupId: 'erp-service',
  topics: [TOPICS.orders],
  maxAttempts: 3,                       // in-process retry with backoff
  handle: async ({ event, attempt }) => {
    if (!isType(event, 'OrderPlaced')) return;

    if (salesOrders.has(event.orderId)) {
      log.warn(`${event.orderId} already has sales order ${salesOrders.get(event.orderId)} - skip`);
      return;
    }

    // Retries mean you WILL run this body more than once for the same message.
    // Everything after this line has to be safe to repeat. That is idempotency,
    // and it is the tax you pay for at-least-once delivery.
    if (Math.random() < FAIL_RATE) {
      throw new Error(`ERP API 503 (attempt ${attempt})`);
    }

    const soId = `SO-${event.orderId.split('-')[1]}`;
    salesOrders.set(event.orderId, soId);
    log.info(`${event.orderId} -> ERP sales order ${soId} created on attempt ${attempt}`);
  },
});
