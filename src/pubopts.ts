import { JetStreamPublishConstraints } from "./jstypes.ts";

export type JetStreamPubConstraint = (
  opts: JetStreamPublishConstraints,
) => void;

export function expectLastMsgID(id: string): JetStreamPubConstraint {
  return (opts: JetStreamPublishConstraints) => {
    opts.lid = id;
  };
}

export function expectLastSequence(seq: number): JetStreamPubConstraint {
  return (opts: JetStreamPublishConstraints) => {
    opts.seq = seq;
  };
}

export function expectStream(stream: string): JetStreamPubConstraint {
  return (opts: JetStreamPublishConstraints) => {
    opts.str = stream;
  };
}

export function msgID(id: string): JetStreamPubConstraint {
  return (opts: JetStreamPublishConstraints) => {
    opts.id = id;
  };
}

export function ttl(n: number): JetStreamPubConstraint {
  return (opts: JetStreamPublishConstraints) => {
    opts.ttl = n;
  };
}
