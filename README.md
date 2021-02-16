# jetstream.js

The JetStream client for JavaScript is work in progress. Use at your own risk.

Note that the API is subject to change without warning, and while you are free
to submit patches and issues, note that until released, the client won't be
officially supported.

The explicit options specified are
[documented here](https://github.com/nats-io/jetstream). This rough
documentation focuses on how to use JetStream from within JavaScript, not on
documenting JetStream through JavaScript.

Currently, the API is split into administrative functionality (JetStreamManager)
and a JetStream _client_. Note that the JetStream doesn't require client, and
plain old NATS can be used.

The jetstream.js library will be built-into the nats-base-client once it is
stable. This library is only compatible with nats.js v2, nats.deno and nats.ws.

## JetStreamManager (JSM)

The JetStreamManager functionality is a programmatic interface to the `nats`
cli. In most cases you should be using the `nats` cli. With that said, creating
and managing streams and consumers programmatically can be helpful in learning
how to use JetStream. JetStreamManager makes it trivial to perform many
operations.

To create a JSM client:

```typescript
const jsm = await JetStreamManager(nc);
```

To create a stream:

```typescript
const si = await jsm.streams.create({ name: "A", subjects: ["a", "a.>"] });
```

To add a message to a stream:

```typescript
// regular nats publish will add the message to the stream
nc.publish("a");
```

Create a push consumer:

```typescript
const sc = StringCodec();
const ci = await jsm.consumers.add("A", {
  durable_name: "dur",
  ack_policy: AckPolicy.Explicit,
  deliver_to: "mymessages",
});

// to get messages:
const sub = nc.subscribe("mymessages");
(async () => {
  for await (const m of sub) {
    // do something with the message
    console.log(m);
    // because the policy was AckPolicy.Explicit, the message
    // was to be ack'ed or the server will resend it
    m.respond(sc.encode("+ACK"));
  }
})();
```

To auto-ack messages:

```typescript
// to get messages:
import { autoAck } from "./jstypes";

const sub = nc.subscribe("mymessages");
autoAck(sub);
(async () => {
  for await (const m of sub) {
    // do something with the message
    console.log(m);
  }
})();
```
