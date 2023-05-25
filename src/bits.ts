export function extractBit(mask: number, ix: number): number {
  return (mask & (1 << ix)) >>> 0;
}
export function testBit(mask: number, ix: number): boolean {
  return (mask & (1 << ix)) !== 0;
}
export function setBit(mask: number, ix: number): number {
  return (mask | (1 << ix)) >>> 0;
}
export function clearBit(mask: number, ix: number): number {
  return (mask & ~(1 << ix)) >>> 0;
}
