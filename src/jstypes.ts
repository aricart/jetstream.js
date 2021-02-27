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
  Subscription,
  SubscriptionOptions,
} from "https://deno.land/x/nats/src/mod.ts";
import { SubscriptionImpl } from "https://deno.land/x/nats/nats-base-client/internal_mod.ts";

import { ACK } from "./jsmsg.ts";
import {
  Advisory,
  ConsumerConfig,
  JetStreamAccountStats,
  PurgeResponse,
  StreamConfig,
  StreamInfo,
} from "./types.ts";
import { StoredMsg } from "./stream_api.ts";
import { JetStreamPubConstraint } from "./jsclient.ts";
import { Lister } from "./jslister.ts";
import { ConsumerAPI } from "./consumer_api.ts";

export interface JetStreamClient {
  publish(
    subj: string,
    data: Uint8Array,
    ...options: JetStreamPubConstraint[]
  ): Promise<PubAck>;
  // subscribe(
  //   subj: string,
  //   opts: JetStreamSubOptions,
  //   ...options: JetStreamSubOption[]
  // ): Promise<Subscription>;
}

export interface StreamAPI {
  info(name: string): Promise<StreamInfo>;
  add(cfg: Partial<StreamConfig>): Promise<StreamInfo>;
  update(cfg: StreamConfig): Promise<StreamInfo>;
  purge(name: string): Promise<PurgeResponse>;
  delete(name: string): Promise<boolean>;
  list(): Lister<StreamInfo>;
  deleteMessage(name: string, seq: number): Promise<boolean>;
  getMessage(name: string, seq: number): Promise<StoredMsg>;
  find(subject: string): Promise<string>;
}

export function autoAck(sub: Subscription) {
  const s = sub as SubscriptionImpl;
  s.setYieldedCb((msg) => {
    msg.respond(ACK);
  });
}

export interface JSM {
  consumers: ConsumerAPI;
  streams: StreamAPI;
  getAccountInfo(): Promise<JetStreamAccountStats>;
  advisories(): AsyncIterable<Advisory>;
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

export interface PubAck {
  stream: string;
  seq: number;
  duplicate: boolean;
}

export interface JetStreamOptions {
  apiPrefix?: string;
  timeout?: number;
}
