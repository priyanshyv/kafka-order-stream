/**
 * Event contracts.
 *
 * In a queue-based system these "events" are implicit: a RabbitMQ payload, an
 * SQS message body assembled in middleware, a fire-and-forget function call.
 * Nothing types them and nothing versions them.
 *
 * Here they are the contract. Every message on `orders` is one of these.
 */

export type PaymentMethod = 'COD' | 'PG';

export interface EventMeta {
  /** Unique per message. The idempotency key for every consumer. */
  eventId: string;
  /** Producer-side wall clock. Distinct from the broker's log-append time. */
  occurredAt: string;
  /** Bump this when the payload shape changes; consumers can branch on it. */
  version: 1;
  /** Which service produced it - shows up in headers too. */
  source: string;
}

export interface OrderItem {
  sku: string;
  name: string;
  qty: number;
  unitPrice: number;
}

/** The anchor row exists. Everything downstream hangs off this fact. */
export interface OrderPlaced {
  type: 'OrderPlaced';
  meta: EventMeta;
  orderId: string;
  userId: string;
  cartId: string;
  paymentMethod: PaymentMethod;
  items: OrderItem[];
  total: number;
  walletDebit: number;
}

/**
 * Money confirmed. Cash-on-delivery and prepaid reach this point through two
 * completely different doors - collection at the door vs a gateway callback -
 * but downstream consumers only care that it happened. Same event, different
 * `via`. Collapsing two code paths into one fact is a big part of the win.
 */
export interface PaymentConfirmed {
  type: 'PaymentConfirmed';
  meta: EventMeta;
  orderId: string;
  userId: string;
  amount: number;
  via: 'gateway-callback' | 'cod-collection';
  merchantTransactionId?: string;
}

/** The warehouse actually assigned to fulfil the order. */
export interface WarehouseAssigned {
  type: 'WarehouseAssigned';
  meta: EventMeta;
  orderId: string;
  warehouseId: string;
  promisedDeliveryDate: string;
}

/** One per transition. A row of the status log, as a message. */
export interface OrderStatusChanged {
  type: 'OrderStatusChanged';
  meta: EventMeta;
  orderId: string;
  from: OrderStatus | null;
  to: OrderStatus;
  /** Monotonic per order. Lets you detect gaps/reordering with your own eyes. */
  sequence: number;
}

export type OrderStatus =
  | 'ORDER_CREATED'
  | 'ORDER_PAYMENT_PENDING'
  | 'PICKING_COMPLETED'
  | 'QC_COMPLETED'
  | 'DISPATCHED'
  | 'DELIVERED'
  | 'CANCELLED';

export type OrderEvent =
  | OrderPlaced
  | PaymentConfirmed
  | WarehouseAssigned
  | OrderStatusChanged;

export type OrderEventType = OrderEvent['type'];

/** Narrowing helper so consumers can switch exhaustively. */
export function isType<T extends OrderEventType>(
  e: OrderEvent,
  t: T,
): e is Extract<OrderEvent, { type: T }> {
  return e.type === t;
}

let counter = 0;
export function makeMeta(source: string): EventMeta {
  counter += 1;
  return {
    eventId: `${source}-${Date.now().toString(36)}-${counter}`,
    occurredAt: new Date().toISOString(),
    version: 1,
    source,
  };
}
