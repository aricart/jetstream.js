import { connect, Empty } from "../src/nats_deno.ts";
import { JetStream } from "../src/jetstream.ts";
import { expectLastSequence, expectStream, msgID } from "../src/jstypes.ts";
import { JetStreamManager } from "../src/jsm.ts";

const nc = await connect();

const jsm = await JetStreamManager(nc);
await jsm.streams.add({ name: "B", subjects: ["b.a"] });

const c = await JetStream(nc);
let pa = await c.publish("b.a", Empty, msgID("a"), expectStream("B"));
console.log(pa.duplicate, pa.seq, pa.stream);

pa = await c.publish(
  "b.a",
  Empty,
  msgID("a"),
  expectLastSequence(1),
);
console.log(pa.duplicate, pa.seq, pa.stream);

await jsm.streams.delete("B");

await nc.drain();
