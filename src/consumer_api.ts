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
import {
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
  JsMsg,
  Lister,
  PullOptions,
  PushConsumer,
  SuccessResponse,
  validateDurableName,
  validateStreamName,
} from "./jstypes.ts";
import { ListerFieldFilter, ListerImpl } from "./jslister.ts";
import { ACK, toJsMsg } from "./jsmsg.ts";
import {
  createInbox,
  Msg,
  NatsConnection,
  QueuedIterator,
  Subscription,
  SubscriptionImpl,
} from "https://deno.land/x/nats@v1.0.0-rc4/nats-base-client/internal_mod.ts";

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
    sub.info = this.toJetStreamSubscription(c, true);
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

  pullBatch(
    stream: string,
    durable: string,
    opts: Partial<PullOptions> = { batch: 1 },
  ): QueuedIterator<JsMsg> {
    validateStreamName(stream);
    validateDurableName(durable);

    opts.batch = opts.batch ?? 1;
    opts.no_wait = true;
    const qi = new QueuedIterator<JsMsg>();
    const wants = opts.batch;
    let received = 0;
    qi.yieldedCb = (m: JsMsg) => {
      received++;
      // if we have one pending and we got the expected
      // or there are no more stop the iterator
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
        } else if (
          msg.headers && (msg.headers.code === 404 || msg.headers.code === 503)
        ) {
          qi.stop();
        } else {
          qi.received++;
          qi.push(toJsMsg(msg));
        }
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
    validateStreamName(stream);
    validateDurableName(durable);

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