export function compactTimeago(input) {
  const t = typeof input === 'string' ? Date.parse(input) : +new Date(input);
  const s = Math.max(0, Math.floor((Date.now() - t) / 1000));
  if (s < 60) return `${s} ${s === 1 ? 'second' : 'seconds'} ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m} ${m === 1 ? 'minute' : 'minutes'} ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h} ${h === 1 ? 'hour' : 'hours'} ago`;
  const d = Math.floor(h / 24);
  if (d < 30) return `${d} ${d === 1 ? 'day' : 'days'} ago`;
  const mo = Math.floor(d / 30);
  if (mo < 12) return `${mo} ${mo === 1 ? 'month' : 'months'} ago`;
  const y = Math.floor(mo / 12);
  return `${y} ${y === 1 ? 'year' : 'years'} ago`;
}
