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
import { AckPolicy, ConsumerConfig, DeliverPolicy, Nanos } from "./types.ts";
import {
  defaultConsumer,
  ns,
  validateDurableName,
  validateStreamName,
} from "./util.ts";

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

export interface ConsumerSubOpts {
  consumer: string;
  mack: boolean;
  pull: number;
  queue: string;
  stream: string;
  config: ConsumerConfig;
}

function consumerOpts(): ConsumerOpts {
  return new ConsumerOptsImpl();
}

export interface ConsumerOpts {
  ackAll(): void;
  ackExplicit(): void;
  ackNone(): void;
  deliverAll(): void;
  deliverLast(): void;
  deliverNew(): void;
  deliverTo(subject: string): void;
  durable(name: string): void;
  manualAck(): void;
  maxAckPending(n: number): void;
  maxDeliver(n: number): void;
  maxWaiting(n: number): void;
  pull(batch: number): void;
  pullDirect(stream: string, consumer: string, batch: number): void;
  queue(name: string): void;
  startSequence(seq: number): void;
  startTime(date: Date | Nanos): void;
}

class ConsumerOptsImpl implements ConsumerOpts {
  config: Partial<ConsumerConfig>;
  consumer: string;
  mack: boolean;
  pullCount: number;
  subQueue: string;
  stream: string;

  constructor() {
    this.stream = "";
    this.consumer = "";
    this.pullCount = 0;
    this.subQueue = "";
    this.mack = false;
    this.config = defaultConsumer("");
    // not set
    this.config.ack_policy = AckPolicy.None;
  }

  pull(batch: number) {
    if (batch <= 0) {
      throw new Error("batch must be greater than 0");
    }
    this.pullCount = batch;
  }

  pullDirect(
    stream: string,
    consumer: string,
    batchSize: number,
  ): void {
    validateStreamName(stream);
    this.stream = stream;
    this.consumer = consumer;
    this.pull(batchSize);
  }

  deliverTo(subject: string) {
    this.config.deliver_subject = subject;
  }

  queue(name: string) {
    this.subQueue = name;
  }

  manualAck() {
    this.mack = true;
  }

  durable(name: string) {
    validateDurableName(name);
    this.config.durable_name = name;
  }

  deliverAll() {
    this.config.deliver_policy = DeliverPolicy.All;
  }

  deliverLast() {
    this.config.deliver_policy = DeliverPolicy.Last;
  }

  deliverNew() {
    this.config.deliver_policy = DeliverPolicy.New;
  }

  startSequence(seq: number) {
    if (seq <= 0) {
      throw new Error("sequence must be greater than 0");
    }
    this.config.deliver_policy = DeliverPolicy.StartSequence;
    this.config.opt_start_seq = seq;
  }

  startTime(time: Date | Nanos) {
    let n: Nanos;
    if (typeof time === "number") {
      n = time as Nanos;
    } else {
      const d = time as Date;
      n = ns(d.getTime());
    }
    this.config.deliver_policy = DeliverPolicy.StartTime;
    this.config.opt_start_time = n;
  }

  ackNone() {
    this.config.ack_policy = AckPolicy.None;
  }

  ackAll() {
    this.config.ack_policy = AckPolicy.All;
  }

  ackExplicit() {
    this.config.ack_policy = AckPolicy.Explicit;
  }

  maxDeliver(max: number) {
    this.config.max_deliver = max;
  }

  maxAckPending(max: number) {
    this.config.max_ack_pending = max;
  }

  maxWaiting(max: number) {
    this.config.max_waiting = max;
  }
}
