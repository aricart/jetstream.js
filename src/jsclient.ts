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

import { InvalidJestreamAck, JetStreamOptions } from "./jetstream.ts";
import { BaseApiClient } from "./base_api.ts";
import {
  Empty,
  headers,
  NatsConnection,
  NatsError,
  RequestOptions,
} from "./nbc_mod.ts";
import { AckPolicy, ConsumerConfig, DeliverPolicy } from "./types.ts";
import { validateDurableName } from "./util.ts";

export enum PubHeaders {
  MsgIdHdr = "Nats-Msg-Id",
  ExpectedStreamHdr = "Nats-Expected-Stream",
  ExpectedLastSeqHdr = "Nats-Expected-Last-Sequence",
  ExpectedLastMsgIdHdr = "Nats-Expected-Last-Msg-Id",
}

export interface PubAck {
  stream: string;
  seq: number;
  duplicate: boolean;
}

export interface JetStreamClient {
  publish(
    subj: string,
    data: Uint8Array,
    options?: Partial<JetStreamPublishOptions>,
  ): Promise<PubAck>;

  // subscribe(
  //   subj: string,
  //   opts: JetStreamSubOptions,
  //   ...options: JetStreamSubOption[]
  // ): Promise<Subscription>;
}

export class JetStreamClientImpl extends BaseApiClient
  implements JetStreamClient {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async publish(
    subj: string,
    data: Uint8Array = Empty,
    opts?: Partial<JetStreamPublishOptions>,
  ): Promise<PubAck> {
    opts = opts ?? {};
    opts.expect = opts.expect ?? {};
    const mh = headers();
    if (opts) {
      if (opts.msgID) {
        mh.set(PubHeaders.MsgIdHdr, opts.msgID);
      }
      if (opts.expect.lastMsgID) {
        mh.set(PubHeaders.ExpectedLastMsgIdHdr, opts.expect.lastMsgID);
      }
      if (opts.expect.streamName) {
        mh.set(PubHeaders.ExpectedStreamHdr, opts.expect.streamName);
      }
      if (opts.expect.lastSequence) {
        mh.set(PubHeaders.ExpectedLastSeqHdr, `${opts.expect.lastSequence}`);
      }
    }

    const to = opts.timeout ?? this.timeout;
    const ro = {} as RequestOptions;
    if (to) {
      ro.timeout = to;
    }
    if (opts) {
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

interface JetStreamPublishOptions {
  msgID: string;
  timeout: number;
  expect: Partial<{
    lastMsgID: string;
    streamName: string;
    lastSequence: number;
  }>;
}

export type JetStreamSubOption = (opts: JetStreamSubOpts) => void;

export interface JetStreamSubOpts {
  stream: string;
  consumer: string;
  pull: number;
  mack: boolean;
  cfg: ConsumerConfig;
}

export function ackNone(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.ack_policy = AckPolicy.None;
  };
}

export function ackAll(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.ack_policy = AckPolicy.All;
  };
}

export function ackExplicit(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.ack_policy = AckPolicy.Explicit;
  };
}

export function manualAck(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.mack = true;
  };
}

export function deliverAll(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.All;
  };
}

export function deliverLast(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.Last;
  };
}

export function deliverNew(): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.New;
  };
}

export function durable(name: string): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    validateDurableName(name);
    opts.cfg.durable_name = name;
  };
}

export function attach(deliverSubject: string): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_subject = deliverSubject;
  };
}

export function pull(batchSize: number): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    if (batchSize <= 0) {
      throw new Error("batchsize must be greater than 0");
    }
    opts.pull = batchSize;
  };
}

export function pullDirect(
  stream: string,
  consumer: string,
  batchSize: number,
): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.stream = stream;
    opts.consumer = consumer;
    pull(batchSize)(opts);
  };
}

export function startSequence(seq: number): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.FromSequence;
    opts.cfg.opt_start_seq = seq;
  };
}

export function startTime(nanos: number): JetStreamSubOption {
  return (opts: JetStreamSubOpts) => {
    opts.cfg.deliver_policy = DeliverPolicy.FromTime;
    opts.cfg.opt_start_time = nanos;
  };
}
