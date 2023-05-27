export class SlowTeeError extends Error {
  constructor(m: string) {
    super(m);
    // https://github.com/Microsoft/TypeScript/wiki/Breaking-Changes#extending-built-ins-like-error-array-and-map-may-no-longer-work
    Object.setPrototypeOf(this, new.target.prototype);
  }
}
