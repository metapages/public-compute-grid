/**
 * Builds the lookup that returns the queue for a name, creating and setting it
 * up on first use.
 *
 * A queue is only usable once its async `setup()` has finished (that is what
 * assigns its db), so it is published into `registry` only then. Callers that
 * arrive while setup is still running share the in-flight promise instead of
 * getting the half-built queue or starting a second one.
 * https://github.com/metapages/public-compute-grid/issues/347
 *
 * `registry` stays the source of truth for finished queues: a queue's
 * `dispose()` deletes itself from it, and the next call builds a new one. The
 * in-flight entry is dropped once setup settles, so a failed setup is retried
 * on the next call rather than cached. A queue whose setup failed is disposed
 * before the error is passed on: its constructor already opened a broadcast
 * channel (and maybe timers), which would otherwise stay open with no db.
 *
 * Any arguments after the queue name are passed on to `create()`. They only
 * matter to the call that builds the queue: a caller that finds it built, or
 * still setting up, gets that queue whatever it passed.
 */
export const makeEnsureQueue = <
  Q extends { setup(): Promise<unknown>; dispose?(): void },
  A extends unknown[] = [],
>(
  registry: { [queue: string]: Q },
  create: (queue: string, ...args: A) => Q,
): (queue: string, ...args: A) => Promise<Q> => {
  const inFlight = new Map<string, Promise<Q>>();

  return (queue: string, ...args: A): Promise<Q> => {
    const existing = registry[queue];
    if (existing) {
      return Promise.resolve(existing);
    }
    let pending = inFlight.get(queue);
    if (!pending) {
      pending = (async () => {
        const q = create(queue, ...args);
        try {
          await q.setup();
        } catch (err) {
          // Safe: dispose() only deletes registry[queue], which is not set yet
          try {
            q.dispose?.();
          } catch (disposeErr) {
            console.error(`Disposing queue ${queue} after a failed setup also failed`, disposeErr);
          }
          throw err;
        }
        registry[queue] = q;
        return q;
      })();
      inFlight.set(queue, pending);
      // create() runs inside the async function above, so even a synchronous
      // throw from it arrives here as a rejection, after the set above, and the
      // entry is still cleared rather than left holding a rejected promise
      const clear = () => inFlight.delete(queue);
      pending.then(clear, clear);
    }
    return pending;
  };
};
