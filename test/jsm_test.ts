/*
 * Copyright 2021 The NATS Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { cleanup, initStream, JetStreamConfig, setup } from "./jstest_util.ts";
import { connect } from "../src/nats_deno.ts";
import { JetStreamManager, JetstreamNotEnabled } from "../src/jetstream.ts";
import {
  assert,
  assertEquals,
  assertThrows,
  assertThrowsAsync,
} from "https://deno.land/std@0.83.0/testing/asserts.ts";

import { deferred, Empty, headers, JSONCodec, nuid } from "../src/nbc_mod.ts";
import {
  AckPolicy,
  AdvisoryKind,
  StreamConfig,
  StreamInfo,
} from "../src/types.ts";

const StreamNameRequired = "stream name required";
const ConsumerNameRequired = "durable name required";

Deno.test("jsm - jetstream not enabled", async () => {
  // start a regular server - no js conf
  const { ns, nc } = await setup();
  const err = await assertThrowsAsync(async () => {
    await JetStreamManager(nc);
  });
  assertEquals(err.message, JetstreamNotEnabled);
  await cleanup(ns, nc);
});

Deno.test("jsm - account info", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const ai = await jsm.getAccountInfo();
  assert(ai.limits.max_memory > 0);
  await cleanup(ns, nc);
});

Deno.test("jsm - account not enabled", async () => {
  const conf = {
    "no_auth_user": "b",
    accounts: {
      A: {
        jetstream: "enabled",
        users: [{ user: "a", password: "a" }],
      },
      B: {
        users: [{ user: "b" }],
      },
    },
  };
  const { ns, nc } = await setup(JetStreamConfig(conf, true));
  const err = await assertThrowsAsync(async () => {
    await JetStreamManager(nc);
  });
  assertEquals(err.message, JetstreamNotEnabled);

  const a = await connect(
    { port: ns.port, user: "a", pass: "a" },
  );
  await JetStreamManager(a);
  await a.close();
  await cleanup(ns, nc);
});

Deno.test("jsm - empty stream config fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.add({} as StreamConfig);
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - empty stream config update fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  let ci = await jsm.streams.add({ name: name, subjects: [`${name}.>`] });
  assertEquals(ci!.config!.subjects!.length, 1);

  await assertThrowsAsync(async () => {
    await jsm.streams.update({} as StreamConfig);
  });
  ci!.config!.subjects!.push("foo");
  ci = await jsm.streams.update(ci.config);
  assertEquals(ci!.config!.subjects!.length, 2);
  await cleanup(ns, nc);
});

Deno.test("jsm - delete empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.delete("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - info empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.info("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - info msg not found stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.info(name);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - delete msg empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.deleteMessage("", 1);
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - delete msg not found stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.deleteMessage(name, 1);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - no stream lister is empty", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const streams = await jsm.streams.list().next();
  assertEquals(streams.length, 0);
  await cleanup(ns, nc);
});

Deno.test("jsm - add stream", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  let si = await jsm.streams.add({ name });
  assertEquals(si.config.name, name);

  const fn = (i: StreamInfo): boolean => {
    assertEquals(i.config, si.config);
    assertEquals(i.state, si.state);
    assertEquals(i.created, si.created);
    return true;
  };

  fn(await jsm.streams.info(name));
  let lister = await jsm.streams.list().next();
  fn(lister[0]);

  // add some data
  nc.publish(name, Empty);
  si = await jsm.streams.info(name);
  lister = await jsm.streams.list().next();
  fn(lister[0]);

  await cleanup(ns, nc);
});

Deno.test("jsm - purge not found stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const name = nuid.next();
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.purge(name);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - purge empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.purge("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - stream purge", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  nc.publish(subj, Empty);

  let si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 1);

  await jsm.streams.purge(stream);
  si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 0);

  await cleanup(ns, nc);
});

Deno.test("jsm - stream delete", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  nc.publish(subj, Empty);
  await jsm.streams.delete(stream);
  const err = await assertThrowsAsync(async () => {
    await jsm.streams.info(stream);
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - stream delete message", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  nc.publish(subj, Empty);

  let si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 1);
  assertEquals(si.state.first_seq, 1);
  assertEquals(si.state.last_seq, 1);

  assert(await jsm.streams.deleteMessage(stream, 1));
  si = await jsm.streams.info(stream);
  assertEquals(si.state.messages, 0);
  assertEquals(si.state.first_seq, 2);
  assertEquals(si.state.last_seq, 1);

  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on empty stream name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumers.info("", "");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on empty consumer name fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumers.info("foo", "");
  });
  assertEquals(err.message, ConsumerNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on not found stream fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumers.info("foo", "dur");
  });
  assertEquals(err.message, "stream not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info on not found consumer", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  const err = await assertThrowsAsync(async () => {
    await jsm.consumers.info(stream, "dur");
  });
  assertEquals(err.message, "consumer not found");
  await cleanup(ns, nc);
});

Deno.test("jsm - consumer info", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  await jsm.consumers.add(
    stream,
    { durable_name: "dur", ack_policy: AckPolicy.Explicit },
  );
  const ci = await jsm.consumers.info(stream, "dur");
  assertEquals(ci.name, "dur");
  assertEquals(ci.config.durable_name, "dur");
  assertEquals(ci.config.ack_policy, "explicit");
  await cleanup(ns, nc);
});

Deno.test("jsm - no consumer lister with empty stream fails", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const err = assertThrows(() => {
    jsm.consumers.list("");
  });
  assertEquals(err.message, StreamNameRequired);
  await cleanup(ns, nc);
});

Deno.test("jsm - no consumer lister with no consumers empty", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  const consumers = await jsm.consumers.list(stream).next();
  assertEquals(consumers.length, 0);
  await cleanup(ns, nc);
});

Deno.test("jsm - lister", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);
  await jsm.consumers.add(
    stream,
    { durable_name: "dur", ack_policy: AckPolicy.Explicit },
  );
  let consumers = await jsm.consumers.list(stream).next();
  assertEquals(consumers.length, 1);
  assertEquals(consumers[0].config.durable_name, "dur");

  await jsm.consumers.delete(stream, "dur");
  consumers = await jsm.consumers.list(stream).next();
  assertEquals(consumers.length, 0);

  await cleanup(ns, nc);
});

Deno.test("jsm - update stream", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream } = await initStream(nc);
  const jsm = await JetStreamManager(nc);

  let si = await jsm.streams.info(stream);
  assertEquals(si.config!.subjects!.length, 1);

  si.config!.subjects!.push("foo");
  si = await jsm.streams.update(si.config);
  assertEquals(si.config!.subjects!.length, 2);
  await cleanup(ns, nc);
});

Deno.test("jsm - get message", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const { stream, subj } = await initStream(nc);

  const jc = JSONCodec();
  const h = headers();
  h.set("xxx", "a");
  nc.publish(subj, jc.encode(1), { headers: h });
  nc.publish(subj, jc.encode(2));

  const jsm = await JetStreamManager(nc);
  let sm = await jsm.streams.getMessage(stream, 1);
  assertEquals(sm.subject, subj);
  assertEquals(sm.seq, 1);
  assertEquals(jc.decode(sm.data), 1);

  sm = await jsm.streams.getMessage(stream, 2);
  assertEquals(sm.subject, subj);
  assertEquals(sm.seq, 2);
  assertEquals(jc.decode(sm.data), 2);

  const err = await assertThrowsAsync(async () => {
    await jsm.streams.getMessage(stream, 3);
  });
  assertEquals(err.message, "stream store EOF");

  await cleanup(ns, nc);
});

Deno.test("jsm - advisories", async () => {
  const { ns, nc } = await setup(JetStreamConfig({}, true));
  const jsm = await JetStreamManager(nc);
  const iter = jsm.advisories();
  const streamAction = deferred();
  (async () => {
    for await (const a of iter) {
      if (a.kind === AdvisoryKind.StreamAction) {
        streamAction.resolve();
      }
    }
  })().then();
  await initStream(nc);
  await streamAction;
  await cleanup(ns, nc);
});
