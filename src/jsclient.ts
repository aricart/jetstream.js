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
  defaultPrefix,
  defaultTimeout,
  InvalidJestreamAck,
  JetstreamNotEnabled,
} from "./jetstream.ts";

import type { Codec, NatsConnection, RequestOptions } from "./nbc.ts";

import { Empty, headers, JSONCodec, Msg, NatsError } from "./nbc.ts";
import {
  ApiResponse,
  JetStreamClient,
  JetStreamOptions,
  JetStreamPubConstraint,
  JetStreamPublishConstraints,
  PubAck,
  PubAckResponse,
  PubHeaders,
} from "./jstypes.ts";

export class ApiClient {
  nc: NatsConnection;
  opts: JetStreamOptions;
  prefix: string;
  timeout: number;
  jc: Codec<unknown>;

  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    this.nc = nc;
    this.opts = opts ? opts : {} as JetStreamOptions;
    this._parseOpts();
    this.prefix = this.opts.apiPrefix!;
    this.timeout = this.opts.timeout!;
    this.jc = JSONCodec();
  }

  _parseOpts() {
    let prefix = this.opts.apiPrefix || defaultPrefix;
    if (!prefix || prefix.length === 0) {
      throw new Error("invalid empty prefix");
    }
    const c = prefix[prefix.length - 1];
    if (c === ".") {
      prefix = prefix.substr(0, prefix.length - 1);
    }
    this.opts.apiPrefix = prefix;
    this.opts.timeout = this.opts.timeout || defaultTimeout;
  }

  async _request(
    subj: string,
    data: unknown = null,
    opts?: RequestOptions,
  ): Promise<unknown> {
    opts = opts || {} as RequestOptions;
    opts.timeout = this.timeout;

    let a: Uint8Array = Empty;
    if (data) {
      a = this.jc.encode(data);
    }

    const m = await this.nc.request(
      subj,
      a,
      opts,
    );
    return this.parseJsResponse(m);
  }

  parseJsResponse(m: Msg): unknown {
    const v = this.jc.decode(m.data);
    const r = v as ApiResponse;

    if (r.error) {
      if (r.error.code === 503) {
        throw NatsError.errorForCode(
          JetstreamNotEnabled,
          new Error(r.error.description),
        );
      }
      throw new NatsError(r.error.description, `${r.error.code}`);
    }
    return v;
  }
}
//
export class JetStreamClientImpl extends ApiClient implements JetStreamClient {
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

    const ro = {} as RequestOptions;
    if (this.timeout) {
      ro.timeout = this.timeout;
    }
    if (options) {
      ro.headers = mh;
    }

    const r = await this.nc.request(subj, data, ro);
    const pa = this.parseJsResponse(r) as PubAckResponse;
    if (pa.stream === "") {
      throw NatsError.errorForCode(InvalidJestreamAck);
    }
    pa.duplicate = pa.duplicate ? pa.duplicate : false;
    return pa;
  }
  //
  //   _initSubOpts(
  //     args = {} as JetStreamSubOptions,
  //     ...options: JetStreamSubOption[]
  //   ): JetStreamSubOpts {
  //     const opts = {} as JetStreamSubOpts;
  //     opts.name = args.name ? args.name : "";
  //     opts.stream = args.stream ? args.stream : "";
  //     opts.consumer = args.consumer ? args.consumer : "";
  //     opts.pull = args.pull ? args.pull : 0;
  //     opts.mack = args.mack ? args.mack : false;
  //     opts.cfg = args.cfg ? args.cfg : {} as ConsumerConfig;
  //     opts.queue = args.queue ? args.queue : "";
  //     opts.callback = args.callback ? args.callback : undefined;
  //     opts.max = args.max ? args.max : 0;
  //
  //     opts.cfg.deliver_policy = opts.cfg.deliver_policy || DeliverPolicy.All;
  //     opts.cfg.replay_policy = opts.cfg.replay_policy || ReplayPolicy.Instant;
  //
  //     opts.cfg.ack_policy = args && args.cfg && args.cfg.ack_policy
  //       ? opts.cfg.ack_policy
  //       : AckPolicy.NotSet;
  //     options.forEach((fn) => {
  //       fn(opts);
  //     });
  //     return opts;
  //   }
  //
  //   async subscribe(
  //     subj: string,
  //     opts = {} as JetStreamSubOptions,
  //     ...options: JetStreamSubOption[]
  //   ): Promise<Subscription> {
  //     const o = this._initSubOpts(opts, ...options);
  //
  //     const pullMode = o.pull > 0;
  //     let stream = o.stream;
  //     let consumer = o.consumer;
  //     let attached = false;
  //     let deliver = createInbox();
  //
  //     let ccfg: ConsumerConfig;
  //     let shouldCreate = false;
  //     const requiresApi = (!stream && !consumer) && !o.cfg.deliver_subject;
  //
  //     if (this.opts.direct && requiresApi) {
  //       throw new Error("jsm api use is not allowed on direct mode");
  //     }
  //
  //     if (this.opts.direct) {
  //       if (o.cfg.deliver_subject) {
  //         deliver = o.cfg.deliver_subject;
  //       } else {
  //         // use the subject given
  //         deliver = subj;
  //       }
  //       attached = true;
  //     } else {
  //       // find the stream...
  //       const jsm = this.jsm!;
  //       stream = await jsm.streamNameBySubject(subj);
  //       let info: ConsumerInfo;
  //       consumer = o.cfg.durable_name || "";
  //       if (consumer) {
  //         // match to the consumer
  //         info = await jsm.consumerInfo(stream, consumer);
  //         ccfg = info.config;
  //         attached = true;
  //         if (ccfg.filter_subject && subj != ccfg.filter_subject) {
  //           throw new Error("subject doesn't match consumer");
  //         }
  //         if (ccfg.deliver_subject) {
  //           deliver = ccfg.deliver_subject;
  //         }
  //       } else {
  //         // create the consumer
  //         shouldCreate = true;
  //         if (!pullMode) {
  //           o.cfg.deliver_subject = deliver;
  //         }
  //         o.cfg.filter_subject = subj;
  //       }
  //       if (!this.jsm) {
  //         throw new Error("no jsm when trying to create a stream");
  //       }
  //     }
  //     const sub = this._subscribe(deliver, o.mack, o);
  //
  //     if (shouldCreate) {
  //       if (!this.jsm) {
  //         throw new Error("no jsm when trying to create a stream");
  //       }
  //       const jsm = this.jsm;
  //
  //       if (o.cfg.ack_policy === AckPolicy.NotSet) {
  //         o.cfg.ack_policy = AckPolicy.Explicit;
  //       }
  //       const isDurable = o.cfg.deliver_subject !== "";
  //       try {
  //         const ci = await jsm.addConsumer(stream, o.cfg);
  //         sub.info.stream = ci.stream_name;
  //         sub.info.consumer = ci.name;
  //         sub.info.deliver = ci.config.deliver_subject;
  //         sub.info.durable = isDurable;
  //       } catch (err) {
  //         sub.unsubscribe();
  //         throw err;
  //       }
  //     } else {
  //       sub.info.stream = stream;
  //       sub.info.consumer = consumer;
  //       sub.info.deliver = this.opts.direct ? o.cfg.deliver_subject : deliver;
  //     }
  //
  //     sub.info.attached = attached;
  //
  //     if (o.pull > 0) {
  //       const psub = sub as PullSubscription<JsMsg>;
  //       sub.info.pull = o.pull;
  //       psub.pull();
  //     }
  //
  //     return sub as PullSubscription<JsMsg>;
  //   }
  //
  //   _subscribe(
  //     subject: string,
  //     manualAcks: boolean,
  //     opts: JetStreamSubOptions = {},
  //   ): Subscription {
  //     if (this.nc.isClosed()) {
  //       throw NatsError.errorForCode(ErrorCode.CONNECTION_CLOSED);
  //     }
  //     if (this.nc.isDraining()) {
  //       throw NatsError.errorForCode(ErrorCode.CONNECTION_DRAINING);
  //     }
  //     subject = subject || "";
  //     if (subject.length === 0) {
  //       throw NatsError.errorForCode(ErrorCode.BAD_SUBJECT);
  //     }
  //
  //     const o = {} as SubscriptionOptions;
  //     if (opts.callback) {
  //       o.callback = opts.callback;
  //     }
  //     o.queue = opts.queue;
  //     o.max = opts.max;
  //
  //     const nci = this.nc as NatsConnectionImpl;
  //     const sub = new JsSubscriptionImpl<JsMsg>(
  //       nci,
  //       subject,
  //       this.prefix,
  //       o,
  //       manualAcks,
  //     );
  //     const sx = sub as SubscriptionImpl<unknown>;
  //     return nci.protocol.subscribe(sx) as JsSubscriptionImpl<JsMsg>;
  //   }
}
