import { assert, assertEquals } from "std/assert";

import {
  WebsocketMessageTypeWorkerToServer,
  type WebsocketMessageWorkerToServer,
  type WorkerRegistration,
} from "@metapages/compute-queues-shared";
import { closeKv } from "../../shared/src/shared/kv.ts";

// Mock the DockerJobQueue to test periodic registration
class MockDockerJobQueue {
  private registrationInterval: ReturnType<typeof setInterval> | null = null;
  private registrationCount = 0;
  private sender: (message: WebsocketMessageWorkerToServer) => void;

  constructor(sender: (message: WebsocketMessageWorkerToServer) => void) {
    this.sender = sender;
    this.startPeriodicRegistration();
  }

  register() {
    const registration: WorkerRegistration = {
      time: Date.now(),
      version: "test",
      id: "test-worker",
      cpus: 2,
      gpus: 0,
      maxJobDuration: "1h",
    };

    try {
      this.sender({
        type: WebsocketMessageTypeWorkerToServer.WorkerRegistration,
        payload: registration,
      });
      this.registrationCount++;
      console.log(`Mock worker registered successfully (count: ${this.registrationCount})`);
    } catch (err) {
      console.log(`Mock worker registration failed: ${err}`);
    }
  }

  private startPeriodicRegistration() {
    // Register every 1 second for testing (much faster than production)
    this.registrationInterval = setInterval(() => {
      try {
        this.register();
      } catch (err) {
        console.log(`Mock worker periodic registration failed: ${err}`);
      }
    }, 1000); // 1 second for testing
  }

  public stopPeriodicRegistration() {
    if (this.registrationInterval) {
      clearInterval(this.registrationInterval);
      this.registrationInterval = null;
    }
  }

  public getRegistrationCount(): number {
    return this.registrationCount;
  }
}

Deno.test(
  "verify periodic registration mechanism works",
  async () => {
    const sentMessages: WebsocketMessageWorkerToServer[] = [];

    const mockSender = (message: WebsocketMessageWorkerToServer) => {
      sentMessages.push(message);
    };

    const queue = new MockDockerJobQueue(mockSender);

    // Wait for at least 2 registrations (initial + periodic)
    await new Promise((resolve) => setTimeout(resolve, 2500));

    // Stop the periodic registration
    queue.stopPeriodicRegistration();

    // Verify that registrations were sent
    assert(sentMessages.length >= 2, `Expected at least 2 registration messages, got ${sentMessages.length}`);

    // Verify that all messages are registration messages
    for (const message of sentMessages) {
      assertEquals(message.type, WebsocketMessageTypeWorkerToServer.WorkerRegistration);
      assert(message.payload, "Registration message should have payload");
    }

    // Verify the registration count
    assert(queue.getRegistrationCount() >= 2, `Expected at least 2 registrations, got ${queue.getRegistrationCount()}`);

    // console.log(
    //   `✅ Periodic registration test passed: ${sentMessages.length} messages sent, ${queue.getRegistrationCount()} registrations`,
    // );
    closeKv();
  },
);
