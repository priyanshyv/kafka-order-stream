/**
 * Fire N orders at the API. `npm run seed -- 25`
 * Half COD, half PG, spread over a few users so fraud-detection has something
 * to aggregate when you replay in Phase 4.
 */
const N = Number(process.argv[2] ?? 10);
const BASE = process.env.API ?? 'http://localhost:3000';

const CATALOG = [
  { sku: 'MED-001', name: 'Metformin 500mg', unitPrice: 240 },
  { sku: 'MED-002', name: 'Atorvastatin 10mg', unitPrice: 410 },
  { sku: 'MED-003', name: 'Telmisartan 40mg', unitPrice: 330 },
  { sku: 'MED-004', name: 'Levothyroxine 50mcg', unitPrice: 180 },
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

  // PG orders get their gateway callback; COD orders get collected later.
  if (body.paymentMethod === 'PG') {
    await fetch(`${BASE}/orders/${json.orderId}/callback`, { method: 'POST' });
  }
  await new Promise((r) => setTimeout(r, 80));
}
console.log(`\ndone - ${N} orders on the log. They stay there. Replay them any time.`);

export {};
