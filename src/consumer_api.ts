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

import { BaseApiClient } from "./base_api.ts";
import { Lister, ListerFieldFilter, ListerImpl } from "./lister.ts";
import { ACK, JsMsg, toJsMsg } from "./jsmsg.ts";
import {
  createInbox,
  Msg,
  NatsConnection,
  QueuedIterator,
  Subscription,
  SubscriptionImpl,
  SubscriptionOptions,
  Timeout,
  timeout,
} from "./nbc_mod.ts";
import {
  AckPolicy,
  Consumer,
  ConsumerConfig,
  ConsumerInfo,
  ConsumerListResponse,
  CreateConsumerRequest,
  DeliverPolicy,
  Nanos,
  ReplayPolicy,
  SuccessResponse,
} from "./types.ts";
import {
  ephemeralConsumer,
  ns,
  validateDurableName,
  validateStreamName,
} from "./util.ts";
import { JetStreamOptions } from "./jetstream.ts";

export interface PullOptions {
  batch: number;
  "no_wait": boolean; // no default here
  expires: number;
}

export interface JetStreamSubscriptionOptions extends SubscriptionOptions {
  manualAcks?: boolean;
}

export interface JetStreamSubOptions extends SubscriptionOptions {
  name?: string;
  stream?: string;
  consumer?: string;
  pull?: number;
  mack?: boolean;
  cfg?: ConsumerConfig;
}

export interface ConsumerAPI {
  info(stream: string, consumer: string): Promise<ConsumerInfo>;

  add(stream: string, cfg: Partial<ConsumerConfig>): Promise<ConsumerInfo>;

  delete(stream: string, consumer: string): Promise<boolean>;

  list(stream: string): Lister<ConsumerInfo>;

  ephemeral(
    stream: string,
    cfg: Partial<EphemeralConsumer>,
    opts?: JetStreamSubscriptionOptions,
  ): Promise<Subscription>;

  bySubject(
    subject: string,
    opts?: JetStreamSubscriptionOptions,
  ): Promise<Subscription>;

  pull(stream: string, durable: string): Promise<JsMsg>;

  fetch(
    stream: string,
    durable: string,
    deliver: string,
    opts: Partial<PullOptions>,
  ): void;

  pullBatch(
    stream: string,
    durable: string,
    opts: Partial<PullOptions>,
  ): QueuedIterator<JsMsg>;
}

export interface EphemeralConsumer {
  name: string;
  "deliver_subject"?: string;
  "deliver_policy": DeliverPolicy;
  "opt_start_seq"?: number;
  "opt_start_time"?: number;
  "ack_policy": AckPolicy;
  "ack_wait"?: number;
  "max_deliver"?: number;
  "filter_subject"?: string;
  "replay_policy": ReplayPolicy;
  "rate_limit_bps"?: number;
  "sample_freq"?: string;
  "max_waiting"?: number;
  "max_ack_pending"?: number;
}

export interface PushConsumerConfig extends ConsumerConfig {
  "deliver_subject": string;
}

export interface PushConsumer extends Consumer {
  config: PushConsumerConfig;
}

export interface JetStreamSubscription {
  jsm: ConsumerAPI;
  consumer: string;
  stream: string;
  deliverSubject?: string;
  pull: number;
  durable: boolean;
  attached: boolean;
}

export class ConsumerAPIImpl extends BaseApiClient implements ConsumerAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(
    stream: string,
    cfg: ConsumerConfig,
  ): Promise<ConsumerInfo> {
    validateStreamName(stream);

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
    validateStreamName(stream);
    validateDurableName(name);
    const r = await this._request(
      `${this.prefix}.CONSUMER.INFO.${stream}.${name}`,
    );
    return r as ConsumerInfo;
  }

  async delete(stream: string, name: string): Promise<boolean> {
    validateStreamName(stream);
    validateDurableName(name);
    const r = await this._request(
      `${this.prefix}.CONSUMER.DELETE.${stream}.${name}`,
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  list(stream: string): Lister<ConsumerInfo> {
    validateStreamName(stream);
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
    sub.info = this.setJetStreamInfo(c, true);
    try {
      await this.add(stream, c.config);
      return sub;
    } catch (err) {
      sub.unsubscribe();
      throw err;
    }
  }

  async bySubject(
    subject: string,
    opts: JetStreamSubscriptionOptions = {},
  ): Promise<Subscription> {
    const stream = await this.findStream(subject);
    return this.ephemeral(stream, {}, opts);
  }

  // FIXME: this will jam the server - maybe pulls for 10s
  async pull(stream: string, durable: string): Promise<JsMsg> {
    validateStreamName(stream);
    validateDurableName(durable);
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

  // server has 1 message, sends message followed by 404
  // jsm.consumers.pullBatch(stream, "me", {batch: 25, no_wait: true,})
  // FIXME: expires and no_wait are mutually exclusive
  // if no expires, it will fail after 512 batch max - ERR 408 when the max error triggers
  // this will need a timeout
  // jsm.consumers.pullBatch(stream, "me", {batch: 25});

  // no_wait disables max waiting pulls
  // FIXME: Err 408 - max waiting pulls 512 results in 408
  // FIXME: Err 409 - max ack pending exceeded
  pullBatch(
    stream: string,
    durable: string,
    opts: Partial<PullOptions> = { batch: 1 },
  ): QueuedIterator<JsMsg> {
    validateStreamName(stream);
    validateDurableName(durable);

    const args: Partial<PullOptions> = {};
    args.batch = opts.batch ?? 1;
    args.no_wait = opts.no_wait ?? false;
    let expires = opts.expires ?? 0;
    if (expires) {
      args.expires = ns(expires);
    }
    if (expires === 0 && args.no_wait === false) {
      throw new Error("expires or no_wait is required");
    }

    let expireLock: Timeout<void> | null = null;

    const qi = new QueuedIterator<JsMsg>();
    const wants = opts.batch;
    let received = 0;
    qi.yieldedCb = (m: JsMsg) => {
      received++;
      if (expireLock && m.info.pending === 0) {
        // the expiration will close it
        return;
      }
      // if we have one pending and we got the expected
      // or there are no more stop the iterator
      if (
        qi.getPending() === 1 && m.info.pending === 0 || wants === received
      ) {
        qi.stop();
      }
    };

    const inbox = createInbox();
    const sub = this.nc.subscribe(inbox, {
      max: opts.batch,
      callback: (err, msg) => {
        if (err) {
          if (expireLock) {
            expireLock.cancel();
            expireLock = null;
          }
          qi.stop(err);
        } else if (msg.headers && msg.headers.hasError) {
          if (expireLock) {
            expireLock.cancel();
            expireLock = null;
          }
          // so we have an error - let them know otherwise
          let err: Error | undefined;
          switch (msg.headers!.code) {
            case 404:
              console.log("no messages");
              // no messages, this is the only error we ignore
              // this can come at the start, or at the end
              break;
            case 408:
              err = new Error("too many pulls");
              break;
            case 409:
              err = new Error("max ack pending exceeded");
              break;
            default:
              err = new Error(msg.headers.status);
              break;
          }
          qi.stop(err);
        } else {
          qi.received++;
          qi.push(toJsMsg(msg));
        }
      },
    });

    // expireLock on the client  the issue is that the request
    // is started on the client, which means that it will expire
    // on the client first
    if (expires) {
      expireLock = timeout<void>(expires);
      expireLock.catch(() => {
        if (!sub.isClosed()) {
          sub.drain();
          expireLock = null;
        }
      });
    }

    (async (lock: Timeout<void> | null) => {
      // close the iterator if the connection or subscription closes unexpectedly
      await (sub as SubscriptionImpl).closed;
      if (lock !== null) {
        lock.cancel();
      }
      qi.stop();
    })(expireLock).catch();

    this.nc.publish(
      `${this.prefix}.CONSUMER.MSG.NEXT.${stream}.${durable}`,
      this.jc.encode(args),
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
    validateStreamName(stream);
    validateDurableName(durable);

    const args: { batch?: number; expires?: Nanos; "no_wait"?: boolean } = {};
    args.batch = opts.batch ?? 1;
    if (opts.expires) {
      args.expires = ns(opts.expires);
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

  setJetStreamInfo(
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

export function autoAck(sub: Subscription) {
  const s = sub as SubscriptionImpl;
  s.setYieldedCb((msg) => {
    msg.respond(ACK);
  });
}
