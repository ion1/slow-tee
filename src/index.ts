import { testBit, clearBit, setBit } from "./bits.js";

import { debugging } from "./debugging.js";

export function slowTee<T>(
  count: number,
  input: ReadableStream<T>
): ReadableStream<T>[] {
  return new SlowTee(count, input).outputs;
}

type SuccessOrFailure<T> =
  | {
      success: true;
      value: ReadableStreamReadResult<T>;
    }
  | {
      success: false;
      reason: any;
    };

type PromiseHandler<T> = {
  resolve: (value: ReadableStreamReadResult<T>) => void;
  reject: (reason: any) => void;
};

class SlowTee<T> {
  count: number;
  reader: ReadableStreamDefaultReader<T>;
  allOutputsMask: number;
  currBlockingMask: number;
  currValue: SuccessOrFailure<T> | null;
  nextWaitingMask: number;
  nextPromiseHandlers: (PromiseHandler<T> | null)[];
  outputs: ReadableStream<T>[];

  constructor(count: number, input: ReadableStream<T>) {
    this.count = Math.trunc(count);
    this.reader = input.getReader();

    if (!(1 <= this.count && this.count <= 32)) {
      throw new RangeError(
        `count must be between 1 and 32, got ${JSON.stringify(this.count)}`
      );
    }

    this.allOutputsMask = 0;

    this.currBlockingMask = 0;
    this.currValue = null;

    this.nextWaitingMask = 0;
    this.nextPromiseHandlers = Array(this.count);

    this.outputs = Array(this.count);
    for (let ix = 0; ix < this.count; ++ix) {
      this.allOutputsMask = setBit(this.allOutputsMask, ix);
      const this_ = this;
      this.outputs[ix] = new ReadableStream<T>({
        async pull(controller) {
          const { value, done } = await this_.pull(ix);

          if (done) {
            controller.close();
            return;
          }

          controller.enqueue(value);
        },
      });
    }
  }

  dumpState(message: string): void {
    if (!debugging) return;

    console.debug(
      `SlowTee: ${message}:`,
      `count=${this.count}`,
      `allOutputsMask=${this.allOutputsMask.toString(2)}`,
      `currBlockingMask=${this.currBlockingMask.toString(2)}`,
      `currValue=${JSON.stringify(this.currValue)}`,
      `nextWaitingMask=${this.nextWaitingMask.toString(2)}`,
      `nextPromiseHandlers=${JSON.stringify(
        this.nextPromiseHandlers?.map((h) => (h ? "(h)" : null))
      )}`
    );
  }

  initiateRead(): void {
    if (debugging) this.dumpState("initiateRead");

    if (this.currValue != null) {
      throw new Error(
        [
          `SlowTee: initiateRead called but there is a current value.`,
          `currValue=${JSON.stringify(this.currValue)}`,
        ].join(" ")
      );
    }

    if (this.currBlockingMask !== 0) {
      // This should never be the case if currValue is null.
      throw new Error(
        [
          `SlowTee: initiateRead called but there are current blockers.`,
          `currBlockingMask=${this.currBlockingMask.toString(2)}`,
        ].join(" ")
      );
    }

    if (this.nextWaitingMask === 0) {
      // There should be someone waiting for this value.
      throw new Error(
        [
          `SlowTee: initiateRead called but there are no next waiters.`,
          `nextWaitingMask=${this.nextWaitingMask.toString(2)}`,
        ].join(" ")
      );
    }

    this.reader.read().then(
      (value) => {
        this.readFinished({ success: true, value });
      },
      (reason) => {
        this.readFinished({ success: false, reason });
      }
    );
  }

  readFinished(result: SuccessOrFailure<T>): void {
    if (debugging)
      this.dumpState(`readFinished result=${JSON.stringify(result)}`);

    if (this.currValue != null) {
      // A read should never have been initiated.
      throw new Error(
        "SlowTee: readFinished called but there is a current value"
      );
    }

    if (this.currBlockingMask !== 0) {
      // This should never be the case if currValue is null.
      throw new Error(
        "SlowTee: readFinished called but there are current blockers"
      );
    }

    if (this.nextWaitingMask === 0) {
      // There should be someone waiting for this value.
      throw new Error(
        "SlowTee: readFinished called but there are no next waiters"
      );
    }

    this.currBlockingMask = this.allOutputsMask;
    this.currValue = result;

    for (let ix = 0; ix < this.count; ++ix) {
      if (testBit(this.nextWaitingMask, ix)) {
        const handler = this.nextPromiseHandlers[ix];
        if (handler == null) {
          throw new Error(
            [
              `SlowTee: Invariant violated:`,
              `ix=${ix}`,
              `nextWaitingMask=${this.nextWaitingMask.toString(2)}`,
              `nextPromiseHandlers=${JSON.stringify(
                this.nextPromiseHandlers?.map((h) => (h ? "(h)" : null))
              )}`,
            ].join(" ")
          );
        }

        this.currBlockingMask = clearBit(this.currBlockingMask, ix);
        if (this.currBlockingMask === 0) this.currValue = null;

        this.nextWaitingMask = clearBit(this.nextWaitingMask, ix);
        this.nextPromiseHandlers[ix] = null;

        this.handlePromise(ix, handler, result);
      }
    }

    if (this.nextWaitingMask !== 0) {
      throw new Error(
        [
          `SlowTee: Invariant violated:`,
          `nextWaitingMask=${this.nextWaitingMask.toString(2)}`,
          `but it should be zero after the handler loop`,
        ].join(" ")
      );
    }
  }

  pull(ix: number): Promise<ReadableStreamReadResult<T>> {
    if (debugging) this.dumpState(`pull ix=${ix}`);

    return new Promise((resolve, reject) => {
      const oldCurrBlockingMask = this.currBlockingMask;
      const oldNextWaitingMask = this.nextWaitingMask;

      if (testBit(this.currBlockingMask, ix)) {
        // There is a value, this reader is pulling it now. Resolve instantly.

        const value = this.currValue;

        if (value == null) {
          // If currBlockingMask is non-zero, currValue must not be null.
          throw new Error(
            [
              `SlowTee: Invariant violated:`,
              `currBlockingMask=${this.currBlockingMask.toString(2)}`,
              `currValue=${JSON.stringify(value)}`,
            ].join(" ")
          );
        }

        // Remove this ix from current blockers.
        this.currBlockingMask = clearBit(this.currBlockingMask, ix);
        if (this.currBlockingMask === 0) this.currValue = null;

        this.handlePromise(ix, { resolve, reject }, value);
      } else {
        // There is no value or the value has already been handled for this ix.
        // Add to the next waiters.

        if (testBit(this.nextWaitingMask, ix)) {
          throw new Error(
            [
              `SlowTee: Invariant violated:`,
              `tried to add ix=${ix} to next waiting mask but it was already there.`,
              `nextWaitingMask=${this.nextWaitingMask.toString(2)}`,
            ].join(" ")
          );
        }

        if (this.nextPromiseHandlers[ix] != null) {
          throw new Error(
            [
              `SlowTee: Invariant violated:`,
              `tried to add ix=${ix} to next promise handlers but one was already there.`,
            ].join(" ")
          );
        }

        this.nextWaitingMask = setBit(this.nextWaitingMask, ix);
        this.nextPromiseHandlers[ix] = { resolve, reject };
      }

      const finalBlockerRemoved =
        oldCurrBlockingMask !== 0 && this.currBlockingMask === 0;
      const firstWaiterAdded =
        oldNextWaitingMask === 0 && this.nextWaitingMask !== 0;

      if (
        (finalBlockerRemoved && this.nextWaitingMask !== 0) ||
        (firstWaiterAdded && this.currBlockingMask === 0)
      ) {
        this.initiateRead();
      }
    });
  }

  handlePromise(
    ix: number,
    handler: PromiseHandler<T>,
    value: SuccessOrFailure<T>
  ): void {
    if (debugging) this.dumpState(`handlePromise ix=${ix}`);

    if (value.success) {
      handler.resolve(value.value);
    } else {
      handler.reject(value.reason);
    }
  }
}
