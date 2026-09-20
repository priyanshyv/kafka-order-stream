import express from 'express';
import { API_PORT, TOPICS } from '../config.js';
import { ensureTopics } from '../lib/kafka.js';
import { publish, publishUnkeyed } from './publisher.js';
import { logger } from '../lib/log.js';
import {
  makeMeta,
  type OrderEvent,
  type OrderItem,
  type OrderStatus,
  type PaymentMethod,
} from '../events.js';

const log = logger('order-service');
const app = express();
app.use(express.json());

/**
 * Stand-in for the `orders` / `order_payments` / `order_status_log` tables.
 * The whole point of this project is that this map is NOT the source of truth
 * for anyone but this service - everyone else builds their own from the stream.
 */
interface OrderRow {
  orderId: string;
  userId: string;
  cartId: string;
  paymentMethod: PaymentMethod;
  items: OrderItem[];
  total: number;
  status: OrderStatus;
  sequence: number;
}
const orders = new Map<string, OrderRow>();
/** The idempotency key from your doc: the findOne on user_cart_id. */
const cartToOrder = new Map<string, string>();

let orderSeq = 1000;

/**
 * POST /orders/initiateOrder
 *
 * This is createOrderForMethod. Same nine steps, same order, same lack of a
 * transaction - except step 8 (the fan-out) is now one keyed append to a log
 * instead of a RabbitMQ publish + a Zoho HTTP call + a `void`.
 */
app.post('/orders/initiateOrder', async (req, res) => {
  const {
    userId = 'user-1',
    cartId = `cart-${Date.now()}`,
    paymentMethod = 'COD',
    items = [{ sku: 'MED-001', name: 'Metformin 500mg', qty: 1, unitPrice: 240 }],
    walletDebit = 0,
  } = req.body as Partial<OrderRow> & { walletDebit?: number };

  // Step 2's guard: the uniqueness check that makes the whole flow idempotent.
  const existing = cartToOrder.get(cartId);
  if (existing) {
    log.warn(`cart ${cartId} already ordered -> ${existing} (idempotent, no new event)`);
    return res.status(200).json({ orderId: existing, deduped: true });
  }

  const orderId = `ORD-${++orderSeq}`;
  const total = items.reduce((s, i) => s + i.qty * i.unitPrice, 0);
  const status: OrderStatus =
    paymentMethod === 'PG' ? 'ORDER_PAYMENT_PENDING' : 'ORDER_CREATED';

  // Steps 1-7: local writes. Still not in a transaction - deliberately.
  const row: OrderRow = { orderId, userId, cartId, paymentMethod, items, total, status, sequence: 0 };
  orders.set(orderId, row);
  cartToOrder.set(cartId, orderId);

  // Step 8: the fan-out. One append. No knowledge of who is listening.
  const placed: OrderEvent = {
    type: 'OrderPlaced',
    meta: makeMeta('order-service'),
    orderId, userId, cartId, paymentMethod, items, total,
    walletDebit: walletDebit ?? 0,
  };
  const at = await publish(placed);
  await emitStatus(row, null, status);

  log.info(`${orderId} placed (${paymentMethod}, Rs.${total}) -> partition ${at.partition} offset ${at.offset}`);
  res.status(201).json({ orderId, status, partition: at.partition, offset: at.offset });
});

/** PG path: the gateway callback. POST /orders/:orderId/callback */
app.post('/orders/:orderId/callback', async (req, res) => {
  const row = orders.get(req.params.orderId);
  if (!row) return res.status(404).json({ error: 'no such order' });
  await publish({
    type: 'PaymentConfirmed',
    meta: makeMeta('payment-gateway'),
    orderId: row.orderId,
    userId: row.userId,
    amount: row.total,
    via: 'gateway-callback',
    merchantTransactionId: `MTX-${row.orderId}`,
  });
  await emitStatus(row, row.status, 'ORDER_CREATED');
  log.info(`${row.orderId} payment confirmed via gateway`);
  res.json({ ok: true });
});

/** COD path: money lands at delivery. Same event, different door. */
app.post('/orders/:orderId/collect', async (req, res) => {
  const row = orders.get(req.params.orderId);
  if (!row) return res.status(404).json({ error: 'no such order' });
  await publish({
    type: 'PaymentConfirmed',
    meta: makeMeta('delivery-app'),
    orderId: row.orderId,
    userId: row.userId,
    amount: row.total,
    via: 'cod-collection',
  });
  log.info(`${row.orderId} cash collected`);
  res.json({ ok: true });
});

/**
 * PATCH /orders/:orderId/status
 * This is updateStatus(). Every fulfilment side effect in the real system hangs
 * off this switch; here it just appends to `order-status` and the side effects
 * subscribe.
 */
app.patch('/orders/:orderId/status', async (req, res) => {
  const row = orders.get(req.params.orderId);
  if (!row) return res.status(404).json({ error: 'no such order' });
  const to = (req.body as { status: OrderStatus }).status;
  const from = row.status;
  row.status = to;
  await emitStatus(row, from, to);
  log.info(`${row.orderId} ${from} -> ${to}`);
  res.json({ ok: true, from, to });
});

/**
 * Phase 3 lab. Publishes the same order's lifecycle with NO key, so the events
 * scatter across partitions and consumers see them out of order.
 * POST /lab/unkeyed
 */
app.post('/lab/unkeyed', async (_req, res) => {
  const orderId = `ORD-UNKEYED-${++orderSeq}`;
  const seq: OrderStatus[] = ['ORDER_CREATED', 'PICKING_COMPLETED', 'QC_COMPLETED', 'DISPATCHED', 'DELIVERED'];
  const placements: number[] = [];
  for (let i = 0; i < seq.length; i++) {
    const at = await publishUnkeyed(
      {
        type: 'OrderStatusChanged',
        meta: makeMeta('lab'),
        orderId,
        from: i === 0 ? null : seq[i - 1]!,
        to: seq[i]!,
        sequence: i + 1,
      },
      TOPICS.orderStatus,
    );
    placements.push(at.partition);
  }
  log.warn(`${orderId}: 5 transitions published UNKEYED -> partitions [${placements.join(', ')}]`);
  res.json({ orderId, partitions: placements, hint: 'watch the consumers print them out of order' });
});

app.get('/orders/:orderId', (req, res) => {
  const row = orders.get(req.params.orderId);
  return row ? res.json(row) : res.status(404).json({ error: 'no such order' });
});

async function emitStatus(row: OrderRow, from: OrderStatus | null, to: OrderStatus) {
  row.sequence += 1;
  await publish(
    {
      type: 'OrderStatusChanged',
      meta: makeMeta('order-service'),
      orderId: row.orderId,
      from,
      to,
      sequence: row.sequence,
    },
    TOPICS.orderStatus,
  );
}

await ensureTopics();
app.listen(API_PORT, () => log.info(`listening on http://localhost:${API_PORT}`));
