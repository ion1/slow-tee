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
 * @param input - The input ReadableStream.
 * @param outputNames - The names of the output streams.
 * @param queuingStrategy - Please see the queuingStrategy parameter for the
 *   ReadableStream constructor.
 * @returns An object of ReadableStreams with a key for each given output name.
 */
export function slowTee<OutputName extends string | number | symbol, T>(
  input: ReadableStream<T>,
  outputNames: OutputName[],
  queuingStrategy?: QueuingStrategy<T>
): Record<OutputName, ReadableStream<T>> {
  const instance = new SlowTee(input);

  const streams = {} as Record<OutputName, ReadableStream<T>>;
  for (const outputName of outputNames) {
    streams[outputName] = instance.addOutputStream(outputName, queuingStrategy);
  }

  return streams;
}

/**
 * Distribute the input from a ReadableStream into a number of outputs at the
 * pace of the slowest output, avoiding the possible unbounded memory usage of
 * ReadableStream tee().
 */
export class SlowTee<OutputName, T> {
  /** The input reader. */
  #reader: ReadableStreamDefaultReader<T>;
  /** The ReadableStream values for the uncancelled outputs. */
  #outputs: Map<OutputName, ReadableStream<T>>;
  /** The state of the finite state machine. */
  #state: State<OutputName, T>;

  constructor(input: ReadableStream<T>) {
    this.#reader = input.getReader();

    this.#outputs = new Map();

    this.#state = { id: "idle" };
    if (debugging)
      console.debug(`SlowTee: State: ${stateToString(this.#state)}`);
  }

  /**
   * Create a ReadableStream and add it to the outputs.
   */
  addOutputStream(
    outputName: OutputName,
    queuingStrategy?: QueuingStrategy<T>
  ): ReadableStream<T> {
    if (this.#outputs.has(outputName))
      throw new SlowTeeError(
        `Output already exists with the name ${JSON.stringify(outputName)}`
      );

    const stream = new ReadableStream<T>(
      {
        pull: async (controller) => {
          const { value, done } = await this.#pull(outputName);

          if (done) {
            controller.close();
            return;
          }

          controller.enqueue(value);
        },
        cancel: () => {
          this.#cancel(outputName);
        },
      },
      queuingStrategy
    );

    this.#outputs.set(outputName, stream);

    return stream;
  }

  /**
   * Enter the given state. Runs possible on-entry actions and updates the
   * state member variable.
   */
  #enter(state: State<OutputName, T>): void {
    if (debugging)
      console.debug(`SlowTee: Entering state: ${stateToString(state)}`);

    this.#state = state;

    dispatchState(state, {
      reading: () => {
        if (debugging) console.debug(`SlowTee: Initiating read`);

        this.#reader.read().then(
          (chunk) => {
            this.#readFinished({ success: true, chunk });
          },
          (reason) => {
            this.#readFinished({ success: false, reason });
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
  #readFinished(result: ReadResult<T>): void {
    if (debugging)
      console.debug(`SlowTee: Read finished: ${readResultToString(result)}`);

    switch (this.#state.id) {
      case "reading":
        // Acceptable to continue.
        break;

      case "idle":
      case "blocked":
        throw new SlowTeeError(
          "Internal error: readFinished called while in state: " +
            stateToString(this.#state)
        );

      default: {
        const impossible: never = this.#state;
        throw new SlowTeeError(
          `Impossible state: ${JSON.stringify(impossible)}`
        );
      }
    }

    // In the next state, every output which did not pull yet will be a
    // blocker.
    const nextBlockers = new Set(this.#outputs.keys());

    // Immediately fulfill the pulls for the waiters.
    for (const [outputName, handler] of this.#state.waiters) {
      this.#fulfillPromise(outputName, handler, result);

      // These waiters will not be blockers in the next state as their pulls
      // have already been fulfilled.
      nextBlockers.delete(outputName);

      // There is no need to delete the waiters from the map because the map
      // will be discarded on the next state transition.
    }

    if (nextBlockers.size === 0) {
      // There are no blockers.
      this.#enter({ id: "idle" });
    } else {
      // There are one or more blockers.
      this.#enter({
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
  #pull(outputName: OutputName): Promise<ReadableStreamReadResult<T>> {
    if (debugging)
      console.debug(`SlowTee: Pull from output ${JSON.stringify(outputName)}`);

    return new Promise((resolve, reject) => {
      const handler = { resolve, reject };

      dispatchState(this.#state, {
        idle: () => {
          // Initiate a read.

          const waiters = new Map([[outputName, handler]]);

          this.#enter({ id: "reading", waiters });
        },
        reading: (state) => {
          // A read has already been initiated. Just add to the waiters.

          if (state.waiters.has(outputName))
            throw new SlowTeeError(
              `Waiter already exists with the name ` +
                JSON.stringify(outputName) +
                `. State: ` +
                stateToString(state)
            );

          state.waiters.set(outputName, handler);
        },
        blocked: (state) => {
          if (state.blockers.has(outputName)) {
            // The output was a blocker. Fulfill the pull and mark the output
            // as not a blocker.

            state.blockers.delete(outputName);

            this.#fulfillPromise(outputName, handler, state.readResult);
          } else {
            // The output was not a blocker. It has already received the
            // current read result and wants the result from the next read. Add
            // it as a next waiter.

            if (state.nextWaiters.has(outputName))
              throw new SlowTeeError(
                `NextWaiter already exists with the name ` +
                  JSON.stringify(outputName) +
                  `. State: ` +
                  stateToString(state)
              );

            state.nextWaiters.set(outputName, handler);
          }

          return this.#exitBlockedIfNoBlockersRemain(state);
        },
      });
    });
  }

  /**
   * To be called when one of the outputs wants to cancel.
   */
  #cancel(outputName: OutputName): void {
    if (debugging)
      console.debug(
        `SlowTee: Cancel request from output ${JSON.stringify(outputName)}`
      );

    this.#outputs.delete(outputName);

    dispatchState(this.#state, {
      idle: () => {
        // Nothing to do.
      },
      reading: (state) => {
        state.waiters
          .get(outputName)
          ?.reject(new SlowTeeError("Cancel requested during a pending pull"));
        state.waiters.delete(outputName);
      },
      blocked: (state) => {
        state.blockers.delete(outputName);

        state.nextWaiters
          .get(outputName)
          ?.reject(new SlowTeeError("Cancel requested during a pending pull"));
        state.nextWaiters.delete(outputName);

        this.#exitBlockedIfNoBlockersRemain(state);
      },
    });
  }

  /**
   * If no blockers remain while in the blocked state, enter idle or initiate
   * a read depending on whether there are waiters for the next read.
   */
  #exitBlockedIfNoBlockersRemain(state: BlockedState<OutputName, T>): void {
    if (state.blockers.size > 0) return;

    // There are no blockers left. Exit the blocked state.
    if (state.nextWaiters.size === 0) {
      // There are no waiters for the next read either.
      return this.#enter({ id: "idle" });
    } else {
      // There are one or more waiters for the next read. Initiate a
      // read.
      return this.#enter({ id: "reading", waiters: state.nextWaiters });
    }
  }

  /**
   * Send a read result to an output by fulfilling the corresponding promise.
   */
  #fulfillPromise(
    outputName: OutputName,
    handler: PromiseHandler<T>,
    readResult: ReadResult<T>
  ): void {
    if (debugging)
      console.debug(
        `SlowTee: Sending to output ${JSON.stringify(outputName)}:`,
        readResultToString(readResult)
      );

    if (readResult.success) {
      handler.resolve(readResult.chunk);
    } else {
      handler.reject(readResult.reason);
    }
  }
}
