const COLORS = ['36', '32', '35', '33', '34', '31'] as const;
let nextColor = 0;

export function logger(service: string) {
  const color = COLORS[nextColor++ % COLORS.length];
  const tag = `\x1b[${color}m[${service}]\x1b[0m`;
  const stamp = () => new Date().toISOString().slice(11, 23);

  return {
    info: (msg: string, extra?: unknown) =>
      console.log(`${stamp()} ${tag} ${msg}${extra !== undefined ? ' ' + fmt(extra) : ''}`),
    warn: (msg: string, extra?: unknown) =>
      console.log(`${stamp()} ${tag} \x1b[33m${msg}\x1b[0m${extra !== undefined ? ' ' + fmt(extra) : ''}`),
    error: (msg: string, extra?: unknown) =>
      console.log(`${stamp()} ${tag} \x1b[31m${msg}\x1b[0m${extra !== undefined ? ' ' + fmt(extra) : ''}`),
  };
}

function fmt(v: unknown): string {
  return typeof v === 'string' ? v : JSON.stringify(v);
}
