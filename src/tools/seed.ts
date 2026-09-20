/**
 * Fire N orders at the API. `npm run seed -- 25`
 * Half COD, half PG, spread over a few users so fraud-detection has something
 * to aggregate when you replay in Phase 4.
 */
const N = Number(process.argv[2] ?? 10);
const BASE = process.env.API ?? 'http://localhost:3000';

const CATALOG = [
  { sku: 'SKU-001', name: 'Wireless Mouse', unitPrice: 240 },
  { sku: 'SKU-002', name: 'Mechanical Keyboard', unitPrice: 410 },
  { sku: 'SKU-003', name: 'USB-C Hub', unitPrice: 330 },
  { sku: 'SKU-004', name: 'Laptop Stand', unitPrice: 180 },
];

for (let i = 0; i < N; i++) {
  const item = CATALOG[i % CATALOG.length]!;
  const body = {
    userId: `user-${(i % 4) + 1}`,
    cartId: `cart-${Date.now()}-${i}`,
    paymentMethod: i % 2 === 0 ? 'COD' : 'PG',
    items: [{ ...item, qty: (i % 3) + 1 }],
    walletDebit: i % 5 === 0 ? 50 : 0,
  };
  const res = await fetch(`${BASE}/orders/initiateOrder`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  const json = (await res.json()) as { orderId: string; partition: number };
  console.log(`seeded ${json.orderId} (${body.paymentMethod}) -> partition ${json.partition}`);

  // Prepaid orders get their gateway callback; COD orders get collected later.
  if (body.paymentMethod === 'PG') {
    await fetch(`${BASE}/orders/${json.orderId}/callback`, { method: 'POST' });
  }
  await new Promise((r) => setTimeout(r, 80));
}
console.log(`\ndone - ${N} orders on the log. They stay there. Replay them any time.`);

export {};
