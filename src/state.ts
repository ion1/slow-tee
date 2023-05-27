/**
 * The current SlowTee state.
 *
 * - Idle
 * - Reading
 * - Blocked
 */
export type State<OutputName, T> =
  | IdleState
  | ReadingState<OutputName, T>
  | BlockedState<OutputName, T>;

/**
 * No output stream has pulled.
 */
export type IdleState = {
  id: "idle";
};

/**
 * One or more output streams have pulled and are waiting for a chunk. They are
 * referred to as the waiters.
 *
 * A read will be initiated upon entering this state.
 */
export type ReadingState<OutputName, T> = {
  id: "reading";
  waiters: Map<OutputName, PromiseHandler<T>>;
};

/**
 * The read finished and one or more output streams have not pulled the result
 * yet. They are referred to as the blockers.
 *
 * If an output stream which is not a blocker pulls, it has already received
 * the current result and it wants the result from the next read. Such outputs
 * are referred to as the next waiters. They will be passed along when entering
 * the Reading state the next time.
 */
export type BlockedState<OutputName, T> = {
  id: "blocked";
  readResult: ReadResult<T>;
  blockers: Set<OutputName>;
  nextWaiters: Map<OutputName, PromiseHandler<T>>;
};

/**
 * A value corresponding to the result from a read on a ReadableStream reader.
 */
export type ReadResult<T> =
  | {
      success: true;
      chunk: ReadableStreamReadResult<T>;
    }
  | {
      success: false;
      reason: any;
    };

/**
 * The resolve and reject functions for fulfilling a Promise.
 */
export type PromiseHandler<T> = {
  resolve: (chunk: ReadableStreamReadResult<T>) => void;
  reject: (reason: any) => void;
};

export function dispatchState<OutputName, T, R>(
  state: State<OutputName, T>,
  handlers: {
    idle: (state: IdleState) => R;
    reading: (state: ReadingState<OutputName, T>) => R;
    blocked: (state: BlockedState<OutputName, T>) => R;
  }
): R {
  switch (state.id) {
    case "idle":
      return handlers.idle(state);
    case "reading":
      return handlers.reading(state);
    case "blocked":
      return handlers.blocked(state);
    default: {
      const impossible: never = state;
      throw new Error(`Impossible state: ${JSON.stringify(impossible)}`);
    }
  }
}

/**
 * Produce a human-readable string representation of the SlowTee state.
 *
 * @param state - The SlowTee state.
 * @returns The string representation.
 */
export function stateToString<OutputName, T>(
  state: State<OutputName, T>
): string {
  switch (state.id) {
    case "idle":
      return "Idle";

    case "reading": {
      const waiters = Array.from(state.waiters.keys());
      return `Reading(waiters: ${JSON.stringify(waiters)})`;
    }

    case "blocked": {
      const blockers = Array.from(state.blockers.values());
      const nextWaiters = Array.from(state.nextWaiters.keys());
      return (
        `Blocked(readResult: ${readResultToString(state.readResult)}` +
        `, blockers: ${JSON.stringify(blockers)})` +
        `, nextWaiters: ${JSON.stringify(nextWaiters)})`
      );
    }

    default: {
      const impossible: never = state;
      throw new Error(`Impossible state: ${JSON.stringify(impossible)}`);
    }
  }
}

/**
 * Produce a human-readable string representation of a read result.
 *
 * @param result - The read result.
 * @returns The string representation.
 */
export function readResultToString<T>(result: ReadResult<T>): string {
  if (result.success) {
    const chunkString = result.chunk.done
      ? "done"
      : `length: ${JSON.stringify(
          (result.chunk.value as { length?: number })?.length
        )}`;

    return `Success(${chunkString})`;
  } else {
    return `Failure(reason: ${JSON.stringify(result.reason)})`;
  }
}
