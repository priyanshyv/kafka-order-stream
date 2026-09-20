export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

/**
 * Three topics, mirroring how a typical order system is wired:
 *
 *  orders        - the order lifecycle. In a queue-based system this is the
 *                  fan-out that checkout does by hand: a RabbitMQ publish, an
 *                  SQS push, and a few fire-and-forget calls.
 *  order-status  - one message per status transition. Most systems already have
 *                  this as an `order_status_log` table; here it is a real log.
 *  orders-dlq    - poison messages that failed after retries (Phase 5).
 */
export const TOPICS = {
  orders: 'orders',
  orderStatus: 'order-status',
  dlq: 'orders-dlq',
} as const;

/**
 * 3 partitions, not 1. With 1 partition you never *feel* ordering or keying -
 * everything is trivially ordered and every lesson in Phase 3 is invisible.
 */
export const PARTITIONS = 3;

export const API_PORT = Number(process.env.PORT ?? 3000);
