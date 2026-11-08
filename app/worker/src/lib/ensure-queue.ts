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
 * on the next call rather than cached.
 */
export const makeEnsureQueue = <Q extends { setup(): Promise<unknown> }>(
  registry: { [queue: string]: Q },
  create: (queue: string) => Q,
): (queue: string) => Promise<Q> => {
  const inFlight = new Map<string, Promise<Q>>();

  return (queue: string): Promise<Q> => {
    const existing = registry[queue];
    if (existing) {
      return Promise.resolve(existing);
    }
    let pending = inFlight.get(queue);
    if (!pending) {
      pending = (async () => {
        const q = create(queue);
        await q.setup();
        registry[queue] = q;
        return q;
      })();
      inFlight.set(queue, pending);
      // Cleared after the set above, so even a synchronous throw from create()
      // does not leave a rejected promise behind
      const clear = () => inFlight.delete(queue);
      pending.then(clear, clear);
    }
    return pending;
  };
};
