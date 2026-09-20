export const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

/**
 * Three topics. Mirrors the real system:
 *
 *  orders        - the order lifecycle. This is what `postOrderCreationJobs()`
 *                  fans out to today via RabbitMQ + SQS + `void`.
 *  order-status  - one message per status transition. This IS `order_status_log`.
 *                  Your doc already calls that table "effectively your event log" -
 *                  here it becomes a real one.
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
