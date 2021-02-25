import { connect } from "https://deno.land/x/nats/src/mod.ts";
import { JetStreamManager } from "../src/jetstream.ts";
import { AckPolicy } from "../src/jstypes.ts";
import { toJsMsg } from "../src/jsmsg.ts";

const nc = await connect();

// create a subscription
const sub = nc.subscribe("my.messages", { max: 3 });
const done = (async () => {
  for await (const m of sub) {
    const jm = toJsMsg(m);
    console.log(`stream msg# ${jm.info.streamSequence}`);
    if (jm.redelivered) {
      console.log("seen this before");
    }
    jm.ack();
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
