import { cleanup, initStream, JetStreamConfig, setup } from "./jstest_util.ts";
import { JetStream, JetStreamManager } from "../src/jetstream.ts";
import { AckPolicy, msgID, PubAck } from "../src/jstypes.ts";
import {
  assert,
  assertEquals,
  assertThrowsAsync,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";
import {
  createInbox,
  Empty,
  StringCodec,
} from "https://deno.land/x/nats/src/mod.ts";
import {
  deferred,
} from "https://deno.land/x/nats/nats-base-client/internal_mod.ts";
import { toJsMsg } from "../src/jsmsg.ts";

Deno.test("jetstream - ephemeral", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  let consumers = await jsm.consumers.list(stream).next();
  assert(consumers.length === 0);

  const sub = await jsm.consumers.ephemeral(stream, {}, { manualAcks: true });
  sub.unsubscribe(1);
  consumers = await jsm.consumers.list(stream).next();
  assert(consumers.length === 1);

  const done = (async () => {
    for await (const m of sub) {
      const jm = toJsMsg(m);
      const h = jm.headers;
      console.log(h);
      const info = jm.info;
      console.log(info);
      jm.ack();
    }
  })();

  const js = await JetStream(nc);
  const pa = await js.publish(subj, Empty, msgID("a"));
  console.log(pa);
  assertEquals(pa.stream, stream);
  assertEquals(pa.duplicate, false);
  assertEquals(pa.seq, 1);
  await done;
  assertEquals(sub.getProcessed(), 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - max ack pending", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const jsm = await JetStreamManager(nc);
  const sc = StringCodec();
  const d = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
  const buf: Promise<PubAck>[] = [];
  const js = await JetStream(nc);
  d.forEach((v) => {
    buf.push(js.publish(subj, sc.encode(v), msgID(v)));
  });
  await Promise.all(buf);

  let consumers = await jsm.consumers.list(stream).next();
  assert(consumers.length === 0);

  const sub = await jsm.consumers.ephemeral(stream, { max_ack_pending: 10 }, {
    manualAcks: true,
    max: 10,
  });
  await (async () => {
    for await (const m of sub) {
      console.log(
        `${sub.getProcessed()} - pending: ${sub.getPending()}: ${
          sc.decode(m.data)
        }`,
      );
      m.respond();
    }
  })();

  await cleanup(ns, nc);
});

Deno.test("jetstream - pull", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true), { debug: true });
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const err = await assertThrowsAsync(async () => {
    await jsm.consumers.pull(stream, "me");
  });
  assertEquals(err.message, "404 No Messages");

  const sc = StringCodec();
  const data = sc.encode("hello");
  const js = await JetStream(nc);
  await js.publish(subj, data, msgID("a"));

  const jm = await jsm.consumers.pull(stream, "me");
  console.log(sc.decode(jm.data));
  jm.ack();
  assertEquals(jm.data, data);

  await cleanup(ns, nc);
});

Deno.test("jetstream - fetch", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true), { debug: true });
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  await jsm.consumers.add(stream, {
    durable_name: "me",
    ack_policy: AckPolicy.Explicit,
  });

  const noMessages = deferred();
  const inbox = createInbox();
  const sub = nc.subscribe(inbox);
  const done = (async () => {
    for await (const m of sub) {
      if (m.headers && m.headers.code === 404) {
        console.log("NO MESSAGES");
        noMessages.resolve();
      } else {
        m.respond();
        sub.unsubscribe();
      }
    }
  })();

  jsm.consumers.fetch(stream, "me", inbox, { no_wait: true });
  await noMessages;

  const js = await JetStream(nc);
  const sc = StringCodec();
  const data = sc.encode("hello");
  await js.publish(subj, data, msgID("a"));

  jsm.consumers.fetch(stream, "me", inbox, { no_wait: true });

  await done;
  const ci = await jsm.consumers.info(stream, "me");
  assertEquals(ci.num_pending, 0);
  assertEquals(ci.num_ack_pending, 0);
  assertEquals(ci.delivered.stream_seq, 1);

  await cleanup(ns, nc);
});

Deno.test("jetstream - date format", () => {
  const d = new Date();
  console.log(d.toISOString());
});
