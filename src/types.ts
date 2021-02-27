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

export type RetentionPolicy = "limits" | "interest" | "workqueue";
export type DiscardPolicy = "old" | "new";
export type StorageType = "file" | "memory";
export type DeliverPolicy =
  | "all"
  | "last"
  | "new"
  | "by_start_sequence"
  | "by_start_time";
export type AckPolicy = "none" | "all" | "explicit" | "";

export type ReplayPolicy = "instant" | "original";

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

export interface CreateConsumerRequest {
  "stream_name": string;
  config: Partial<ConsumerConfig>;
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

export interface SequencePair {
  "consumer_seq": number;
  "stream_seq": number;
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

export interface MsgRequest {
  seq: number;
}

export interface MsgDeleteRequest extends MsgRequest {
  "no_erase"?: boolean;
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

export interface Consumer {
  "stream_name": string;
  config: ConsumerConfig;
}

export interface StreamNames {
  streams: string[];
}

export interface StreamNameBySubject {
  subject: string;
}

export interface NextRequest {
  expires: number;
  batch: number;
  "no_wait": boolean;
}