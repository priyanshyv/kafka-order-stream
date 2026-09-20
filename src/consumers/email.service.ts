/**
 * PHASE 1 + 2 - the simplest consumer.
 *
 * Replaces: sendOrderDetailsEmail() inside processPostOrderCreationJobs().
 * In the real system this only runs because capture-event.middleware pushed a
 * CREATE_EVENT to SQS after the response object was written. Here it just
 * subscribes. Nobody had to know it exists.
 */
import { runConsumer } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import { logger } from '../lib/log.js';
import { isType } from '../events.js';

const log = logger('email-service');

await runConsumer('email-service', {
  groupId: 'email-service',            // <- its own group: gets every message
  topics: [TOPICS.orders],
  handle: async ({ event, raw }) => {
    const where = `p${raw.partition}@${raw.message.offset}`;
    if (isType(event, 'OrderPlaced')) {
      log.info(`${where} -> confirmation email to ${event.userId} for ${event.orderId} (Rs.${event.total})`);
    } else if (isType(event, 'PaymentConfirmed')) {
      log.info(`${where} -> payment receipt for ${event.orderId} (${event.via})`);
    }
  },
});
