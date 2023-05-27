import { debugging } from "./debugging.js";
import { SlowTeeError } from "./error.js";
import {
  State,
  BlockedState,
  ReadResult,
  PromiseHandler,
  dispatchState,
  stateToString,
  readResultToString,
} from "./state.js";

/**
 * Distribute the input from a ReadableStream into a number of outputs at the
 * pace of the slowest output, avoiding the possible unbounded memory usage of
 * ReadableStream tee().
 *
 * @param count - The number of outputs.
 * @param input - The input ReadableStream.
 * @param queueingStrategy - Please see the queueingStrategy parameter for the
 *   ReadableStream constructor.
 * @returns An array of output ReadableStreams.
 */
export function slowTee<T>(
  count: number,
  input: ReadableStream<T>,
  queueingStrategy?: QueuingStrategy<T>
): ReadableStream<T>[] {
  const instance = new SlowTee(count, input, queueingStrategy);
  return Array.from(instance.outputs.values());
}

/**
 * Distribute the input from a ReadableStream into a number of outputs at the
 * pace of the slowest output, avoiding the possible unbounded memory usage of
 * ReadableStream tee().
 */
class SlowTee<T> {
  /** The input reader. */
  reader: ReadableStreamDefaultReader<T>;
  /** The ReadableStream values for the uncancelled outputs. */
  outputs: Map<number, ReadableStream<T>>;
  /** The state of the finite state machine. */
  state: State<T>;

  constructor(
    count: number,
    input: ReadableStream<T>,
    queueingStrategy?: QueuingStrategy<T>
  ) {
    this.reader = input.getReader();

    // Construct a ReadableStream for each output.
    this.outputs = new Map();

    for (let ix = 0; ix < count; ++ix) {
      const stream = new ReadableStream<T>(
        {
          pull: async (controller) => {
            const { value, done } = await this.pull(ix);

            if (done) {
              controller.close();
              return;
            }

            controller.enqueue(value);
          },
          cancel: () => {
            this.cancel(ix);
          },
        },
        queueingStrategy
      );

      this.outputs.set(ix, stream);
    }

    this.state = { id: "idle" };
    if (debugging)
      console.debug(`SlowTee: State: ${stateToString(this.state)}`);
  }

  /**
   * Enter the given state. Runs possible on-entry actions and updates the
   * state member variable.
   */
  enter(state: State<T>): void {
    if (debugging)
      console.debug(`SlowTee: Entering state: ${stateToString(state)}`);

    this.state = state;

    dispatchState(state, {
      reading: () => {
        if (debugging) console.debug(`SlowTee: Initiating read`);

        this.reader.read().then(
          (chunk) => {
            this.readFinished({ success: true, chunk });
          },
          (reason) => {
            this.readFinished({ success: false, reason });
          }
        );
      },

      // Nothing to do when entering.
      idle: () => {},
      blocked: () => {},
    });
  }

  /**
   * To be called when a read initiated by the Reading state on-entry code
   * finishes.
   */
  readFinished(result: ReadResult<T>): void {
    if (debugging)
      console.debug(`SlowTee: Read finished: ${readResultToString(result)}`);

    switch (this.state.id) {
      case "reading":
        // Acceptable to continue.
        break;

      case "idle":
      case "blocked":
        throw new SlowTeeError(
          "Internal error: readFinished called while in state: " +
            stateToString(this.state)
        );

      default: {
        const impossible: never = this.state;
        throw new SlowTeeError(
          `Impossible state: ${JSON.stringify(impossible)}`
        );
      }
    }

    // In the next state, every output which did not pull yet will be a
    // blocker.
    const nextBlockers = new Set(this.outputs.keys());

    // Immediately fulfill the pulls for the waiters.
    for (const [ix, handler] of this.state.waiters) {
      this.fulfillPromise(ix, handler, result);

      // These waiters will not be blockers in the next state as their pulls
      // have already been fulfilled.
      nextBlockers.delete(ix);

      // There is no need to delete the waiters from the map because the map
      // will be discarded on the next state transition.
    }

    if (nextBlockers.size === 0) {
      // There are no blockers.
      this.enter({ id: "idle" });
    } else {
      // There are one or more blockers.
      this.enter({
        id: "blocked",
        readResult: result,
        blockers: nextBlockers,
        // Do not use this.state.waiters, those have been fulfilled already.
        nextWaiters: new Map(),
      });
    }
  }

  /**
   * To be called when one of the outputs is trying to pull.
   */
  pull(ix: number): Promise<ReadableStreamReadResult<T>> {
    if (debugging) console.debug(`SlowTee: Pull from output ix=${ix}`);

    return new Promise((resolve, reject) => {
      const handler = { resolve, reject };

      dispatchState(this.state, {
        idle: () => {
          // Initiate a read.

          const waiters = new Map([[ix, handler]]);

          this.enter({ id: "reading", waiters });
        },
        reading: (state) => {
          // A read has already been initiated. Just add to the waiters.

          if (state.waiters.has(ix))
            throw new SlowTeeError(
              `There is already a waiter with ix=${ix}. State: ` +
                stateToString(state)
            );

          state.waiters.set(ix, handler);
        },
        blocked: (state) => {
          if (state.blockers.has(ix)) {
            // The output was a blocker. Fulfill the pull and mark the output
            // as not a blocker.

            state.blockers.delete(ix);

            this.fulfillPromise(ix, handler, state.readResult);
          } else {
            // The output was not a blocker. It has already received the
            // current read result and wants the result from the next read. Add
            // it as a next waiter.

            if (state.nextWaiters.has(ix))
              throw new SlowTeeError(
                `There is already a nextWaiter with ix=${ix}. State: ` +
                  stateToString(state)
              );

            state.nextWaiters.set(ix, handler);
          }

          return this.exitBlockedIfNoBlockersRemain(state);
        },
      });
    });
  }

  /**
   * To be called when one of the outputs wants to cancel.
   */
  cancel(ix: number): void {
    if (debugging)
      console.debug(`SlowTee: Cancel request from output ix=${ix}`);

    this.outputs.delete(ix);

    dispatchState(this.state, {
      idle: () => {
        // Nothing to do.
      },
      reading: (state) => {
        state.waiters
          .get(ix)
          ?.reject(new SlowTeeError("Cancel requested during a pending pull"));
        state.waiters.delete(ix);
      },
      blocked: (state) => {
        state.blockers.delete(ix);

        state.nextWaiters
          .get(ix)
          ?.reject(new SlowTeeError("Cancel requested during a pending pull"));
        state.nextWaiters.delete(ix);

        this.exitBlockedIfNoBlockersRemain(state);
      },
    });
  }

  /**
   * If no blockers remain while in the blocked state, enter idle or initiate
   * a read depending on whether there are waiters for the next read.
   */
  exitBlockedIfNoBlockersRemain(state: BlockedState<T>): void {
    if (state.blockers.size > 0) return;

    // There are no blockers left. Exit the blocked state.
    if (state.nextWaiters.size === 0) {
      // There are no waiters for the next read either.
      return this.enter({ id: "idle" });
    } else {
      // There are one or more waiters for the next read. Initiate a
      // read.
      return this.enter({ id: "reading", waiters: state.nextWaiters });
    }
  }

  /**
   * Send a read result to an output by fulfilling the corresponding promise.
   */
  fulfillPromise(
    ix: number,
    handler: PromiseHandler<T>,
    readResult: ReadResult<T>
  ): void {
    if (debugging)
      console.debug(
        `SlowTee: Sending to output ix=${ix}:`,
        readResultToString(readResult)
      );

    if (readResult.success) {
      handler.resolve(readResult.chunk);
    } else {
      handler.reject(readResult.reason);
    }
  }
}
