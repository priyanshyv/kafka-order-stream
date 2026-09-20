import type { Producer } from 'kafkajs';
import { client } from '../lib/kafka.js';
import { TOPICS } from '../config.js';
import type { OrderEvent } from '../events.js';

let producer: Producer | null = null;

export async function getProducer(): Promise<Producer> {
  if (producer) return producer;
  const p = client('order-service').producer({
    // Guarantees no duplicates and no reordering on retry, per partition.
    // Without this, a retried produce can land AFTER a later message.
    idempotent: true,
  });
  await p.connect();
  producer = p;
  return p;
}

/**
 * THE most important line in this repo is `key: event.orderId`.
 *
 * Kafka orders messages within a partition, not within a topic. Same key ->
 * same partition -> guaranteed order. Drop the key and OrderShipped can be
 * consumed before OrderPlaced, because they landed on different partitions and
 * two different consumer threads read them.
 */
export async function publish(event: OrderEvent, topic: string = TOPICS.orders) {
  const p = await getProducer();
  const [record] = await p.send({
    topic,
    messages: [
      {
        key: event.orderId,
        value: JSON.stringify(event),
        headers: {
          'x-event-type': event.type,
          'x-event-id': event.meta.eventId,
          'x-source': event.meta.source,
        },
      },
    ],
  });
  return { partition: record?.partition ?? -1, offset: record?.baseOffset ?? '?' };
}

/** Publish without a key, on purpose. Phase 3 uses this to break ordering. */
export async function publishUnkeyed(event: OrderEvent, topic: string = TOPICS.orders) {
  const p = await getProducer();
  const [record] = await p.send({
    topic,
    messages: [{ value: JSON.stringify(event), headers: { 'x-event-type': event.type } }],
  });
  return { partition: record?.partition ?? -1, offset: record?.baseOffset ?? '?' };
}
