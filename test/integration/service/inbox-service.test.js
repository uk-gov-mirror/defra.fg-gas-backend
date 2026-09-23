import { MongoClient } from "mongodb";
import { randomUUID } from "node:crypto";
import { env } from "node:process";
import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
// Its own database, so the running service's poller cannot claim the fixtures.
// The setup files have already loaded config, hence the module reset.
const DATABASE = "fg-gas-backend-inbox-service-test";
vi.stubEnv("MONGO_DATABASE", DATABASE);
vi.resetModules();

const { Inbox } = await import("../../../src/events/models/inbox.js");
const { claimEvents, processExpiredEvents, update } =
  await import("../../../src/events/repositories/inbox.repository.js");
const { clearInboxMessageHandlers, registerInboxMessageHandler } =
  await import("../../../src/events/services/inbox-message-handlers.js");
const { logger } = await import("../../../src/common/logger.js");
const { InboxSubscriber } =
  await import("../../../src/events/subscribers/inbox.subscriber.js");
const { db: serviceDb, mongoClient } =
  await import("../../../src/common/mongo-client.js");

let client;
let db;
let inbox, fifo;

beforeAll(async () => {
  expect(serviceDb.databaseName).toBe(DATABASE);
  client = await MongoClient.connect(env.MONGO_URI);
  db = client.db(DATABASE);
  inbox = db.collection("inbox");
  fifo = db.collection("fifo_locks");
  await fifo.deleteMany({});
  await inbox.deleteMany({});
});

afterEach(() => {
  vi.restoreAllMocks();
});

afterAll(async () => {
  await db?.dropDatabase();
  await client?.close();
  await mongoClient.close();
  vi.unstubAllEnvs();
});

const createMockInbox = (id, time, segregationRef) => {
  return Inbox.createMock({
    _id: id,
    segregationRef,
    event: {
      time,
    },
  });
};

describe("inbox repository claim events", () => {
  beforeEach(async () => {
    await fifo.deleteMany({});
    await inbox.deleteMany({});
    await fifo.insertOne({
      segregationRef: "ref_1",
      locked: true,
      lockedAt: new Date(Date.now()),
      actor: "INBOX",
    });
  });

  it("should claim events in order", async () => {
    await inbox.insertMany([
      createMockInbox("2", new Date(Date.now() - 3000).toISOString(), "ref_1"),
      createMockInbox("3", new Date(Date.now() - 2000).toISOString(), "ref_1"),
      createMockInbox("4", new Date(Date.now() - 1000).toISOString(), "ref_1"),
      createMockInbox("1", new Date(Date.now() - 4000).toISOString(), "ref_1"),
    ]);

    const records = await claimEvents(randomUUID(), "ref_1", 4);
    expect(records).toHaveLength(4);
    expect(records[0]._id).toBe("1");
    expect(records[1]._id).toBe("2");
    expect(records[2]._id).toBe("3");
    expect(records[3]._id).toBe("4");
  });
});

describe("getNextAvailable", () => {
  beforeEach(async () => {
    await fifo.deleteMany({});
    await inbox.deleteMany({});
    await fifo.insertOne({
      segregationRef: "ref_1",
      locked: true,
      lockedAt: new Date(Date.now()),
      actor: "INBOX",
    });
  });

  it("should DLQ events with no segregationRef", async () => {
    await inbox.deleteMany({});
    const inbox1 = createMockInbox(
      "1",
      new Date(Date.now()).toISOString(),
      "ref_1",
    );
    const inbox2 = createMockInbox(
      "2",
      new Date(Date.now()).toISOString(),
      "ref_2",
    );
    inbox1.segregationRef = null;
    await inbox.insertMany([inbox1, inbox2]);
    const getNextAvailableSpy = vi.spyOn(
      InboxSubscriber.prototype,
      "getNextAvailable",
    );
    const processEventsSpy = vi
      .spyOn(InboxSubscriber.prototype, "processEvents")
      .mockResolvedValue(true);
    const subscriber = new InboxSubscriber(1000);
    subscriber.start();
    try {
      await vi.waitFor(() => expect(processEventsSpy).toHaveBeenCalled(), {
        timeout: 5000,
        interval: 20,
      });
    } finally {
      subscriber.stop();
    }
    expect(processEventsSpy).toHaveBeenCalledTimes(1);
    expect(getNextAvailableSpy).toHaveBeenCalledTimes(2);
    const [events] = processEventsSpy.mock.calls[0];
    expect(events).toHaveLength(1);
    expect(events[0]._id).toBe("2");
    expect(events[0].segregationRef).toBe("ref_2");
  });
});

describe("inbox fifo", () => {
  beforeEach(async () => {
    await fifo.deleteMany({});
    await inbox.deleteMany({});
    await fifo.insertOne({
      segregationRef: "ref_1",
      locked: true,
      lockedAt: new Date(Date.now()),
      actor: "INBOX",
    });

    await inbox.insertMany([
      createMockInbox("2", new Date(Date.now() - 3000).toISOString(), "ref_1"),
      createMockInbox("4", new Date(Date.now() - 1000).toISOString(), "ref_1"),
      createMockInbox("1", new Date(Date.now() - 4000).toISOString(), "ref_2"),
      createMockInbox("3", new Date(Date.now() - 2000).toISOString(), "ref_2"),
      createMockInbox("5", new Date(Date.now() - 4000).toISOString(), "ref_3"),
      createMockInbox("6", new Date(Date.now() - 6000).toISOString(), "ref_4"), // should select this one. Oldest record with no lock.
    ]);
  });

  it("should claim unlocked events", async () => {
    const processEventsSpy = vi
      .spyOn(InboxSubscriber.prototype, "processEvents")
      .mockResolvedValue(true);

    vi.spyOn(
      InboxSubscriber.prototype,
      "processResubmittedEvents",
    ).mockResolvedValue(true);

    vi.spyOn(
      InboxSubscriber.prototype,
      "processFailedEvents",
    ).mockResolvedValue(true);

    vi.spyOn(InboxSubscriber.prototype, "processDeadEvents").mockResolvedValue(
      true,
    );

    const subscriber = new InboxSubscriber(1000);
    subscriber.start();
    try {
      await vi.waitFor(() => expect(processEventsSpy).toHaveBeenCalled(), {
        timeout: 5000,
        interval: 20,
      });
    } finally {
      subscriber.stop();
    }
    expect(processEventsSpy).toHaveBeenCalledTimes(1);
    const [events] = processEventsSpy.mock.calls[0];
    expect(events).toHaveLength(1);
    expect(events[0]._id).toBe("6");
    expect(events[0].segregationRef).toBe("ref_4");
  });
});

// A handler that outlives its claim still holds the whole row in memory, and
// its final write would put that copy back over whatever happened since.
describe("inbox final write is fenced on the claim", () => {
  const SOURCE = "CLAIM-FENCE-TEST";
  const REF = "claim_fence_ref";

  const aPublishedRow = () =>
    Inbox.createMock({
      _id: `fence-${randomUUID()}`,
      messageId: `fence-${randomUUID()}`,
      source: SOURCE,
      segregationRef: REF,
      completionAttempts: 0,
    });

  const theRow = (row) => inbox.findOne({ _id: row._id });

  beforeEach(async () => {
    await fifo.deleteMany({});
    await inbox.deleteMany({});
  });

  afterEach(() => {
    clearInboxMessageHandlers();
  });

  it("writes nothing with a stale token", async () => {
    const row = aPublishedRow();
    await inbox.insertOne(row.toDocument());
    const [claimed] = await claimEvents("live-token", REF);
    const before = await theRow(row);

    claimed.markAsComplete();
    const result = await update(claimed, "stale-token");

    expect(result.matchedCount).toBe(0);
    expect(await theRow(row)).toEqual(before);
  });

  it("completes the row with the live token", async () => {
    const row = aPublishedRow();
    await inbox.insertOne(row.toDocument());
    registerInboxMessageHandler(SOURCE, async () => {});

    await new InboxSubscriber().processWithLock(randomUUID(), REF);

    const stored = await theRow(row);
    expect(stored.status).toBe("COMPLETED");
    expect(stored.claimedBy).toBeNull();
  });

  it("leaves a row reclaimed mid-handler as the expiry sweep left it", async () => {
    const row = aPublishedRow();
    await inbox.insertOne(row.toDocument());
    const warn = vi.spyOn(logger, "warn");
    let swept;
    registerInboxMessageHandler(SOURCE, async () => {
      await inbox.updateOne(
        { _id: row._id },
        { $set: { claimExpiresAt: new Date(Date.now() - 1000) } },
      );
      await processExpiredEvents();
      swept = await theRow(row);
    });

    await new InboxSubscriber().processWithLock(randomUUID(), REF);

    expect(swept.status).toBe("FAILED");
    expect(await theRow(row)).toEqual(swept);
    expect(warn).toHaveBeenCalledWith(
      `Inbox event ${row.messageId} was reclaimed before its handler finished`,
    );
  });
});
