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
  createInbox,
  Empty,
  headers,
  MsgHdrs,
  Subscription,
  SubscriptionOptions,
} from "https://deno.land/x/nats/src/mod.ts";
import {
  MsgHdrsImpl,
  nuid,
  QueuedIterator,
  SubscriptionImpl,
} from "https://deno.land/x/nats/nats-base-client/internal_mod.ts";

import { ACK } from "./jsmsg.ts";
import { JetStreamPubConstraint } from "./pubopts.ts";

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

export interface Advisory {
  kind: AdvisoryKind;
  data: unknown;
}

export enum AdvisoryKind {
  API = "api_audit",
  StreamAction = "stream_action",
  ConsumerAction = "consumer_action",
  SnapshotCreate = "snapshot_create",
  SnapshotComplete = "snapshot_complete",
  RestoreCreate = "restore_create",
  RestoreComplete = "restore_complete",
  MaxDeliver = "max_deliver",
  Terminated = "terminated",
  Ack = "consumer_ack",
  StreamLeaderElected = "stream_leader_elected",
  StreamQuorumLost = "stream_quorum_lost",
  ConsumerLeaderElected = "consumer_leader_elected",
  ConsumerQuorumLost = "onsumer_quorum_lost",
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
    opts: { batch?: number; no_wait?: boolean; expires?: Date },
  ): void;

  pullBatch(
    stream: string,
    durable: string,
    opts: Partial<PullOptions>,
  ): QueuedIterator<JsMsg>;
}

export function autoAck(sub: Subscription) {
  const s = sub as SubscriptionImpl;
  s.setYieldedCb((msg) => {
    msg.respond(ACK);
  });
}

export interface PullOptions {
  batch: number;
  "no_wait": boolean; // no default here
  expires: Date; // duration - min is 10s
}

export interface JSM {
  consumers: ConsumerAPI;
  streams: StreamAPI;
  getAccountInfo(): Promise<JetStreamAccountStats>;
  advisories(): AsyncIterable<Advisory>;
}

export interface JetStreamPublishConstraints {
  id?: string;
  lid?: string; // expected last message id
  str?: string; // stream name
  seq?: number; // expected last sequence
  ttl?: number; // max wait
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

export function validateDurableName(name?: string) {
  return validateName("durable", name);
}

export function validateStreamName(name?: string) {
  return validateName("stream", name);
}

function validateName(context: string, name = "") {
  if (name === "") {
    throw Error(`${context} name required`);
  }
  const bad = [".", "*", ">"];
  bad.forEach((v) => {
    if (name.indexOf(v) !== -1) {
      throw Error(
        `invalid ${context} name - ${context} name cannot contain '${v}'`,
      );
    }
  });
}

export interface ApiError {
  code: number;
  description: string;
}

export interface ApiResponse {
  type: string;
  error?: ApiError;
}

export interface ApiPaged {
  total: number;
  offset: number;
  limit: number;
}

export interface ApiPagedRequest {
  offset: number;
}

// FIXME: sources and mirrors
export interface StreamInfo {
  config: StreamConfig;
  created: number; // in ns
  state: StreamState;
  cluster?: ClusterInfo;
  mirror?: StreamSourceInfo;
  sources?: StreamSourceInfo[];
}

export interface StreamConfig {
  name: string;
  subjects?: string[];
  retention: RetentionPolicy;
  "max_consumers": number;
  "max_msgs": number;
  "max_bytes": number;
  discard?: DiscardPolicy;
  "max_age": number;
  "max_msg_size"?: number;
  storage: StorageType;
  "num_replicas": number;
  "no_ack"?: boolean;
  "template_owner"?: string;
  "duplicate_window"?: number; // duration
  placement?: Placement;
  mirror?: StreamSource; // same as a source
  sources?: StreamSource[];
}

export interface StreamSource {
  name: string;
  "opt_start_seq": number;
  "opt_start_time": string;
  "filter_subject": string;
}

export interface Placement {
  cluster: string;
  tags: string[];
}

export type Mirror = StreamSource;

export enum RetentionPolicy {
  Limits = "limits",
  Interest = "interest",
  WorkQueue = "workqueue",
}

// default is old
export enum DiscardPolicy {
  Old = "old",
  New = "new",
}

// default is file
export enum StorageType {
  File = "file",
  Memory = "memory",
}

export interface StreamState {
  messages: number;
  bytes: number;
  "first_seq": number;
  "first_ts": number;
  "last_seq": number;
  "last_ts": string;
  deleted: number[];
  lost: LostStreamData;
  "consumer_count": number;
}

export interface LostStreamData {
  msgs: number;
  bytes: number;
}

export interface ClusterInfo {
  name?: string;
  leader?: string;
  replicas?: PeerInfo[];
}

export interface PeerInfo {
  name: string;
  current: boolean;
  offline: boolean;
  active: number; //ns
  lag: number;
}

export interface StreamSourceInfo {
  name: string;
  lag: number;
  active: number;
  error?: ApiError;
}

export interface PurgeResponse extends Success {
  purged: number;
}

export interface ConsumerListResponse extends ApiResponse, ApiPaged {
  consumers: ConsumerInfo[];
}

export interface StreamListResponse extends ApiResponse, ApiPaged {
  streams: StreamInfo[];
}

export interface Success {
  success: boolean;
}

export type SuccessResponse = ApiResponse & Success;

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

export interface ConsumerConfig {
  name: string;
  "durable_name"?: string;
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

export interface Consumer {
  "stream_name": string;
  config: ConsumerConfig;
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

export function defaultConsumer(
  name: string,
  opts: Partial<ConsumerConfig> = {},
): ConsumerConfig {
  return Object.assign({
    name: name,
    deliver_policy: DeliverPolicy.All,
    ack_policy: AckPolicy.Explicit,
    ack_wait: ns(30 * 1000),
    replay_policy: ReplayPolicy.Instant,
  }, opts);
}

export function defaultPushConsumer(
  name: string,
  deliverSubject: string,
  opts: Partial<ConsumerConfig> = {},
): PushConsumerConfig {
  return Object.assign(defaultConsumer(name), {
    deliver_subject: deliverSubject,
  }, opts);
}

// FIXME: need to health check them to prevent stall
export function ephemeralConsumer(
  stream: string,
  cfg: Partial<ConsumerConfig> = {},
): PushConsumer {
  validateStreamName(stream);
  if (cfg.durable_name) {
    throw new Error("ephemeral subscribers cannot be durable");
  }
  cfg.name = cfg.name ? cfg.name : nuid.next();
  const deliver = cfg.deliver_subject ? cfg.deliver_subject : createInbox();
  const c = defaultPushConsumer(cfg.name, deliver, cfg);
  return { stream_name: stream, config: c };
}

export function pushConsumer(
  stream: string,
  cfg: Partial<ConsumerConfig> = {},
): Consumer {
  validateStreamName(stream);
  if (!cfg.durable_name) {
    throw new Error("durable_name is required");
  }
  if (!cfg.deliver_subject) {
    throw new Error("deliver_subject is required");
  }
  cfg.name = cfg.name ? cfg.name : nuid.next();
  const c = defaultPushConsumer(cfg.name, cfg.durable_name, cfg);
  return { stream_name: stream, config: c };
}

export interface CreateConsumerRequest {
  "stream_name": string;
  config: Partial<ConsumerConfig>;
}

export interface MsgRequest {
  seq: number;
}

export interface MsgDeleteRequest extends MsgRequest {
  "no_erase"?: boolean;
}

export interface StreamMsgResponse extends ApiResponse {
  message: {
    subject: string;
    seq: number;
    data: string;
    hdrs: string;
    time: string;
  };
}

export interface StoredMsg {
  subject: string;
  seq: number;
  header?: MsgHdrs;
  data: Uint8Array;
  time: Date;
}

export class StoredMsgImpl implements StoredMsg {
  subject: string;
  seq: number;
  data: Uint8Array;
  time: Date;
  header?: MsgHdrs;

  constructor(smr: StreamMsgResponse) {
    this.subject = smr.message.subject;
    this.seq = smr.message.seq;
    this.time = new Date(smr.message.time);
    this.data = smr.message.data === "" ? Empty : this._parse(smr.message.data);
    if (smr.message.hdrs) {
      const hd = this._parse(smr.message.hdrs);
      this.header = MsgHdrsImpl.decode(hd);
    } else {
      this.header = headers();
    }
  }

  _parse(s: string): Uint8Array {
    const bs = window.atob(s);
    const len = bs.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
      bytes[i] = bs.charCodeAt(i);
    }
    return bytes;
  }
}

export enum DeliverPolicy {
  All = "all",
  Last = "last",
  New = "new",
  ByStartSequence = "by_start_sequence",
  ByStartTime = "by_start_time",
}

export enum AckPolicy {
  None = "none",
  All = "all",
  Explicit = "explicit",
  NotSet = "",
}

export enum ReplayPolicy {
  Instant = "instant",
  Original = "original",
}

export interface ConsumerInfo {
  "stream_name": string;
  name: string;
  created: number;
  config: ConsumerConfig;
  delivered: SequencePair;
  "ack_floor": SequencePair;
  "num_ack_pending": number;
  "num_redelivered": number;
  "num_waiting": number;
  "num_pending": number;
  cluster?: ClusterInfo;
}

export interface SequencePair {
  "consumer_seq": number;
  "stream_seq": number;
}

export interface Lister<T> {
  next(): Promise<T[]>;
}

export interface JetStreamAccountStats {
  memory: number;
  storage: number;
  streams: number;
  consumers: number;
  api: JetStreamApiStats;
  limits: AccountLimits;
}

export interface JetStreamApiStats {
  total: number;
  errors: number;
}

export interface AccountInfoResponse
  extends ApiResponse, JetStreamAccountStats {}

export interface AccountLimits {
  "max_memory": number;
  "max_storage": number;
  "max_streams": number;
  "max_consumers": number;
}

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

export type PubAckResponse = ApiResponse & PubAck;
export type StreamInfoResponse = ApiResponse & StreamInfo;

export interface JsMsg {
  redelivered: boolean;
  info: DeliveryInfo;
  seq: number;
  headers: MsgHdrs | undefined;
  data: Uint8Array;
  subject: string;
  sid: number;

  ack(): void;
  nak(): void;
  working(): void;
  next(subj?: string): void;
  term(): void;
}

export interface DeliveryInfo {
  stream: string;
  consumer: string;
  redeliveryCount: number;
  streamSequence: number;
  deliverySequence: number;
  timestampNanos: number;
  pending: number;
  redelivered: boolean;
}

export interface StreamNames {
  streams: string[];
}

export type StreamNamesResponse = StreamNames & ApiResponse & ApiPaged;

export interface StreamNameBySubject {
  subject: string;
}

export interface NextRequest {
  expires: number;
  batch: number;
  "no_wait": boolean;
}

export function ns(millis: number) {
  return millis * 1000000;
}

export function ms(ns: number) {
  return ns / 1000000;
}

export interface JetStreamOptions {
  apiPrefix?: string;
  timeout?: number;
}
