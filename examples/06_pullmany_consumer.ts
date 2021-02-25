import {
  connect,
  createInbox,
} from "https://raw.githubusercontent.com/nats-io/nats.deno/js/src/mod.ts";
import { delay } from "https://raw.githubusercontent.com/nats-io/nats.deno/main/nats-base-client/internal_mod.ts";

import { JetStreamManager } from "../src/jetstream.ts";
import { AckPolicy, autoAck } from "../src/jstypes.ts";

const nc = await connect();

const jsm = await JetStreamManager(nc);
const inbox = createInbox();
const sub = nc.subscribe(inbox);
autoAck(sub);

(async () => {
  for await (const m of sub) {
    const noMessages = m.headers && m.headers.code === 404;
    const d = noMessages ? "[no messages]" : m.subject;
    console.log(`${Date.now()} ${d}`);
  }
})().then();

await jsm.consumers.add("A", {
  durable_name: "c",
  ack_policy: AckPolicy.Explicit,
});

jsm.consumers.fetch("A", "c", inbox, { batch: 5 });
await delay(2000);
jsm.consumers.fetch("A", "c", inbox, { batch: 5 });
try {
  await jsm.consumers.pull("A", "c");
} catch (err) {
  if (err.message === "404 No Messages") {
    console.log("no messages!");
  }
}

await nc.close();