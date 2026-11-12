/** A LogStorage in memory, for tests. Not exported from the package. */
import type { LogStorage } from "./joblogs.ts";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * A bucket in memory whose every operation takes a random few milliseconds
 * (or `delay`), so writes, merges and reads really do overlap.
 */
export class MemoryLogStorage implements LogStorage {
  objects = new Map<string, string>();
  delay = 0;
  /** Fail this many puts before succeeding again */
  failPuts = 0;

  private wait() {
    return sleep(this.delay || Math.random() * 3);
  }
  async get(key: string) {
    await this.wait();
    return this.objects.get(key);
  }
  async put(key: string, body: string) {
    await this.wait();
    if (this.failPuts > 0) {
      this.failPuts--;
      throw new Error("put failed");
    }
    this.objects.set(key, body);
  }
  async list(prefix: string) {
    await this.wait();
    return [...this.objects.keys()].filter((key) => key.startsWith(prefix) && !key.slice(prefix.length).includes("/"))
      .sort();
  }
  async delete(key: string) {
    await this.wait();
    this.objects.delete(key);
  }
}
