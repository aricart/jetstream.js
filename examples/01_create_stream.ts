import { connect } from "https://deno.land/x/nats/src/mod.ts";
import { JetStreamManager } from "../src/jetstream.ts";

const nc = await connect();

// create a JSM
const jsm = await JetStreamManager(nc);
// add a stream
await jsm.streams.add({ name: "A", subjects: ["a", "a.>"] });

// publish some messages that match the stream
nc.publish("a");
nc.publish("a.b");
nc.publish("a.b.c");

await nc.drain();
