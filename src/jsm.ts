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

import {
  AccountInfo,
  AccountInfoResponse,
  Advisory,
  AdvisoryKind,
  ApiResponse,
  Consumer,
  ConsumerAPI,
  ConsumerConfig,
  ConsumerInfo,
  ConsumerListResponse,
  CreateConsumerRequest,
  EphemeralConsumer,
  ephemeralConsumer,
  JetStreamOptions,
  JetStreamSubscription,
  JetStreamSubscriptionOptions,
  JSM,
  JsMsg,
  Lister,
  MsgRequest,
  PullOptions,
  PushConsumer,
  StreamAPI,
  StreamConfig,
  StreamInfo,
  StreamListResponse,
  StreamMsg,
  StreamMsgImpl,
  StreamMsgResponse,
  StreamNameBySubject,
  StreamNames,
  SuccessResponse,
  validateDurableName,
} from "./jstypes.ts";
import { ListerFieldFilter, ListerImpl } from "./jslister.ts";
import { ApiClient } from "./jsclient.ts";
import { ACK, toJsMsg } from "./jsmsg.ts";
import type {
  Msg,
  NatsConnection,
  Subscription,
} from "https://deno.land/x/nats/src/mod.ts";
import {
  createInbox,
  QueuedIterator,
  SubscriptionImpl,
} from "https://deno.land/x/nats/nats-base-client/internal_mod.ts";

export const StreamNameRequired = "stream name required";
export const ConsumerNameRequired = "consumer name required";

class StreamAPIImpl extends ApiClient implements StreamAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(cfg = {} as Partial<StreamConfig>): Promise<StreamInfo> {
    if (!cfg.name) {
      throw Error(StreamNameRequired);
    }
    const r = await this._request(
      `${this.prefix}.STREAM.CREATE.${cfg.name}`,
      cfg,
    );
    return r as StreamInfo;
  }

  async delete(stream: string): Promise<boolean> {
    if (!stream) {
      throw new Error(StreamNameRequired);
    }
    const r = await this._request(`${this.prefix}.STREAM.DELETE.${stream}`);
    const cr = r as SuccessResponse;
    return cr.success;
  }

  async update(cfg = {} as StreamConfig): Promise<StreamInfo> {
    if (!cfg.name) {
      throw new Error(StreamNameRequired);
    }
    const r = await this._request(
      `${this.prefix}.STREAM.UPDATE.${cfg.name}`,
      cfg,
    );
    return r as StreamInfo;
  }

  async info(name: string): Promise<StreamInfo> {
    if (name === "") {
      throw new Error(StreamNameRequired);
    }
    const r = await this._request(`${this.prefix}.STREAM.INFO.${name}`);
    return r as StreamInfo;
  }

  list(): Lister<StreamInfo> {
    const filter: ListerFieldFilter<StreamInfo> = (
      v: unknown,
    ): StreamInfo[] => {
      const slr = v as StreamListResponse;
      return slr.streams;
    };
    const subj = `${this.prefix}.STREAM.LIST`;
    return new ListerImpl<StreamInfo>(subj, filter, this);
  }

  async purge(name: string): Promise<void> {
    if (!name) {
      throw new Error(StreamNameRequired);
    }
    await this._request(`${this.prefix}.STREAM.PURGE.${name}`);
    return Promise.resolve();
  }

  async find(subject: string): Promise<string> {
    const q = { subject } as StreamNameBySubject;
    const r = await this._request(`${this.prefix}.STREAM.NAMES`, q);
    const names = r as StreamNames;
    if (!names.streams || names.streams.length !== 1) {
      throw new Error("no stream matches subject");
    }
    return names.streams[0];
  }

  async deleteMessage(stream: string, seq: number): Promise<boolean> {
    if (!stream) {
      throw new Error(StreamNameRequired);
    }
    const dr = { seq } as MsgRequest;
    const r = await this._request(
      `${this.prefix}.STREAM.MSG.DELETE.${stream}`,
      dr,
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  async getMessage(stream: string, seq: number): Promise<StreamMsg> {
    if (!stream) {
      throw new Error(StreamNameRequired);
    }
    const dr = { seq } as MsgRequest;
    const r = await this._request(
      `${this.prefix}.STREAM.MSG.GET.${stream}`,
      dr,
    );
    const sm = r as StreamMsgResponse;
    return new StreamMsgImpl(sm);
  }
}

class ConsumerAPIImpl extends ApiClient implements ConsumerAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(
    stream: string,
    cfg: ConsumerConfig,
  ): Promise<ConsumerInfo> {
    if (!stream) {
      throw new Error(StreamNameRequired);
    }
    const cr = {} as CreateConsumerRequest;
    cr.config = cfg;
    cr.stream_name = stream;

    if (cfg.durable_name) {
      validateDurableName(cfg.durable_name);
    }

    const subj = cfg.durable_name
      ? `${this.prefix}.CONSUMER.DURABLE.CREATE.${stream}.${cfg.durable_name}`
      : `${this.prefix}.CONSUMER.CREATE.${stream}`;
    const r = await this._request(subj, cr);
    return r as ConsumerInfo;
  }

  async info(stream: string, name: string): Promise<ConsumerInfo> {
    if (!stream) {
      throw new Error(StreamNameRequired);
    }
    if (!name) {
      throw new Error(ConsumerNameRequired);
    }
    const r = await this._request(
      `${this.prefix}.CONSUMER.INFO.${stream}.${name}`,
    );
    return r as ConsumerInfo;
  }

  async delete(stream: string, name: string): Promise<boolean> {
    if (!stream) {
      throw new Error(StreamNameRequired);
    }
    if (!name) {
      throw new Error(ConsumerNameRequired);
    }
    validateDurableName(name);
    const r = await this._request(
      `${this.prefix}.CONSUMER.DELETE.${stream}.${name}`,
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  list(stream: string): Lister<ConsumerInfo> {
    if (!stream) {
      throw new Error(StreamNameRequired);
    }
    const filter: ListerFieldFilter<ConsumerInfo> = (
      v: unknown,
    ): ConsumerInfo[] => {
      const clr = v as ConsumerListResponse;
      return clr.consumers;
    };
    const subj = `${this.prefix}.CONSUMER.LIST.${stream}`;
    return new ListerImpl<ConsumerInfo>(subj, filter, this);
  }

  async ephemeral(
    stream: string,
    cfg: Partial<EphemeralConsumer> = {},
    opts: JetStreamSubscriptionOptions = {},
  ): Promise<Subscription> {
    const c = ephemeralConsumer(stream, cfg);
    const sub = this.nc.subscribe(
      c.config.deliver_subject,
      opts,
    ) as SubscriptionImpl;

    if (!opts.manualAcks) {
      sub.setYieldedCb((msg: Msg) => {
        msg.respond(ACK);
      });
    }
    sub.info = this.toJetStreamSubscription(c, true);
    try {
      await this.add(stream, c.config);
      return sub;
    } catch (err) {
      sub.unsubscribe();
      throw err;
    }
  }

  async find(subject: string): Promise<string> {
    const q = { subject } as StreamNameBySubject;
    const r = await this._request(`${this.prefix}.STREAM.NAMES`, q);
    const names = r as StreamNames;
    if (!names.streams || names.streams.length !== 1) {
      throw new Error("no stream matches subject");
    }
    return names.streams[0];
  }

  async bySubject(
    subject: string,
    opts: JetStreamSubscriptionOptions = {},
  ): Promise<Subscription> {
    const stream = await this.find(subject);
    return this.ephemeral(stream, {}, opts);
  }

  async pull(stream: string, durable: string): Promise<JsMsg> {
    const m = await this.nc.request(
      `${this.prefix}.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      this.jc.encode({ no_wait: true, batch: 1 }),
      { noMux: true, timeout: this.timeout },
    );
    if (m.headers && (m.headers.code === 404 || m.headers.code === 503)) {
      throw new Error("no messages");
    }
    return toJsMsg(m);
  }

  pullBatch(
    stream: string,
    durable: string,
    opts: Partial<PullOptions> = { batch: 1 },
  ): QueuedIterator<JsMsg> {
    opts.batch = opts.batch ?? 1;
    opts.no_wait = true;
    const qi = new QueuedIterator<JsMsg>();
    const wants = opts.batch;
    let received = 0;
    qi.yieldedCb = (m: JsMsg) => {
      received++;
      // if we have one pending, this is all we have
      if (qi.getPending() === 1 && m.info.pending === 0 || wants == received) {
        qi.stop();
      }
    };
    const inbox = createInbox();
    this.nc.subscribe(inbox, {
      max: opts.batch,
      callback: (err, msg) => {
        if (err) {
          qi.stop(err);
        }
        if (
          msg.headers && (msg.headers.code === 404 || msg.headers.code === 503)
        ) {
          qi.stop();
        }
        qi.push(toJsMsg(msg));
      },
    });

    this.nc.publish(
      `${this.prefix}.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      this.jc.encode(opts),
      { reply: inbox },
    );
    return qi;
  }

  fetch(
    stream: string,
    durable: string,
    deliver: string,
    opts: Partial<PullOptions> = {
      batch: 1,
    },
  ): void {
    type po = { batch: number; expires?: string; "no_wait"?: boolean };
    const batch = opts.batch ? opts.batch : 1;
    const args = { batch } as po;
    if (opts.expires) {
      args.expires = opts.expires.toISOString();
    }
    if (opts.no_wait) {
      args.no_wait = opts.no_wait;
    }
    this.nc.publish(
      `${this.prefix}.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      this.jc.encode(args),
      {
        reply: deliver,
      },
    );
  }

  toJetStreamSubscription(
    c: (Consumer & PushConsumer),
    attached: boolean,
    pull = 0,
  ): JetStreamSubscription {
    return {
      jsm: this,
      stream: c.stream_name,
      consumer: c.config.name,
      deliverSubject: c.config.deliver_subject,
      durable: c.config.durable_name !== "",
      pull: pull,
      attached: attached,
    } as JetStreamSubscription;
  }
}

export class JetStreamManagerImpl extends ApiClient implements JSM {
  streams: StreamAPI;
  consumers: ConsumerAPI;
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
    this.streams = new StreamAPIImpl(nc, opts);
    this.consumers = new ConsumerAPIImpl(nc, opts);
  }

  async getAccountInfo(): Promise<AccountInfo> {
    const r = await this._request(`${this.prefix}.INFO`);
    return r as AccountInfoResponse;
  }

  advisories(): AsyncIterable<Advisory> {
    const iter = new QueuedIterator<Advisory>();
    this.nc.subscribe(`$JS.EVENT.ADVISORY.>`, {
      callback: (err, msg) => {
        try {
          const d = this.parseJsResponse(msg) as ApiResponse;
          const chunks = d.type.split(".");
          const kind = chunks[chunks.length - 1];
          iter.push({ kind: kind as AdvisoryKind, data: d });
        } catch (err) {
          iter.stop(err);
        }
      },
    });

    return iter;
  }
}
