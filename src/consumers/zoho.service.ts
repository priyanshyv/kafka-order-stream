/**
 * PHASE 5 - failure, retries, and the dead-letter topic.
 *
 * Replaces: createZohoSalesOrderForOrderV2(), which today sits inside
 * postOrderCreationJobs()'s single try/catch that only logs
 * POST_ORDER_CREATION_JOBS_ERROR. Your doc's exact words: the customer gets a
 * clean 200, the order is paid for, and the RabbitMQ message at the bottom of
 * the function was never sent.
 *
 * Here a Zoho failure cannot touch the customer's response, cannot skip the
 * messages after it, and cannot vanish - it retries, then parks in orders-dlq
 * with the original topic/partition/offset in the headers so you can replay it.
 *
 * FAIL_RATE controls how flaky "Zoho" is. Default 30%.
 *   FAIL_RATE=1 npm run zoho     <- everything dead-letters
 */
import { runConsumer } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';
import { isType } from '../events.js';

const log = logger('zoho-service');
const FAIL_RATE = Number(process.env.FAIL_RATE ?? 0.3);

/** Zoho would reject a duplicate sales order anyway - model that here. */
const salesOrders = new Map<string, string>();

await runConsumer('zoho-service', {
  groupId: 'zoho-service',
  topics: [TOPICS.orders],
  maxAttempts: 3,                       // in-process retry with backoff
  handle: async ({ event, attempt }) => {
    if (!isType(event, 'OrderPlaced')) return;

    if (salesOrders.has(event.orderId)) {
      log.warn(`${event.orderId} already has sales order ${salesOrders.get(event.orderId)} - skip`);
      return;
    }

    // Retries mean you WILL run this body more than once for the same order.
    // Everything after this line has to be safe to repeat. That is idempotency,
    // and it is the tax you pay for at-least-once delivery.
    if (Math.random() < FAIL_RATE) {
      throw new Error(`Zoho API 503 (attempt ${attempt})`);
    }

    const soId = `SO-${event.orderId.split('-')[1]}`;
    salesOrders.set(event.orderId, soId);
    log.info(`${event.orderId} -> Zoho sales order ${soId} created on attempt ${attempt}`);
  },
});
