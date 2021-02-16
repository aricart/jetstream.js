import { connect } from "https://raw.githubusercontent.com/nats-io/nats.deno/js/src/mod.ts";
import { JetStreamManager } from "../src/jetstream.ts";
import { AckPolicy } from "../src/jstypes.ts";
import { ACK, toJsMsg } from "../src/jsmsg.ts";

const nc = await connect();

// create a subscription
const sub = nc.subscribe("my.messages", { max: 3 });
const done = (async () => {
  for await (const m of sub) {
    console.log(m.subject);
    m.respond(ACK);
  }
})();

// create an ephemeral consumer - the subscription must exist
const jsm = await JetStreamManager(nc);
await jsm.consumers.add("A", {
  ack_policy: AckPolicy.Explicit,
  deliver_subject: "my.messages",
});

await done;
await nc.close();
