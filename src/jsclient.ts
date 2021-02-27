import { JetStreamClient, JetStreamOptions, PubAck } from "./jstypes.ts";
import { InvalidJestreamAck } from "./jetstream.ts";
import { BaseApiClient } from "./base_api.ts";
import {
  headers,
  NatsConnection,
  NatsError,
  RequestOptions,
} from "https://deno.land/x/nats/src/mod.ts";

export enum PubHeaders {
  MsgIdHdr = "Nats-Msg-Id",
  ExpectedStreamHdr = "Nats-Expected-Stream",
  ExpectedLastSeqHdr = "Nats-Expected-Last-Sequence",
  ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id",
}

export class JetStreamClientImpl extends BaseApiClient
  implements JetStreamClient {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async publish(
    subj: string,
    data: Uint8Array,
    ...options: JetStreamPubConstraint[]
  ): Promise<PubAck> {
    const o = {} as JetStreamPublishConstraints;
    const mh = headers();
    if (options) {
      options.forEach((fn) => {
        fn(o);
      });
      if (o.id) {
        mh.set(PubHeaders.MsgIdHdr, o.id);
      }
      if (o.lid) {
        mh.set(PubHeaders.ExpectedLastMsgIdHdr, o.lid);
      }
      if (o.str) {
        mh.set(PubHeaders.ExpectedStreamHdr, o.str);
      }
      if (o.seq && o.seq > 0) {
        mh.set(PubHeaders.ExpectedLastSeqHdr, `${o.seq}`);
      }
    }

    const to = o.ttl ?? this.timeout;
    const ro = {} as RequestOptions;
    if (to) {
      ro.timeout = to;
    }
    if (options) {
      ro.headers = mh;
    }

    const r = await this.nc.request(subj, data, ro);
    const pa = this.parseJsResponse(r) as PubAck;
    if (pa.stream === "") {
      throw NatsError.errorForCode(InvalidJestreamAck);
    }
    pa.duplicate = pa.duplicate ? pa.duplicate : false;
    return pa;
  }
}

interface JetStreamPublishConstraints {
  id?: string;
  lid?: string; // expected last message id
  str?: string; // stream name
  seq?: number; // expected last sequence
  ttl?: number; // max wait
}

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
