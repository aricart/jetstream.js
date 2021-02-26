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
  JetStreamOptions,
  Lister,
  MsgDeleteRequest,
  MsgRequest,
  PurgeResponse,
  StoredMsg,
  StoredMsgImpl,
  StreamAPI,
  StreamConfig,
  StreamInfo,
  StreamListResponse,
  StreamMsgResponse,
  SuccessResponse,
  validateStreamName,
} from "./jstypes.ts";
import { ListerFieldFilter, ListerImpl } from "./jslister.ts";
import { NatsConnection } from "https://deno.land/x/nats@v1.0.0-rc4/nats-base-client/mod.ts";

export class StreamAPIImpl extends BaseApiClient implements StreamAPI {
  constructor(nc: NatsConnection, opts?: JetStreamOptions) {
    super(nc, opts);
  }

  async add(cfg = {} as Partial<StreamConfig>): Promise<StreamInfo> {
    validateStreamName(cfg.name);
    const r = await this._request(
      `${this.prefix}.STREAM.CREATE.${cfg.name}`,
      cfg,
    );
    return r as StreamInfo;
  }

  async delete(stream: string): Promise<boolean> {
    validateStreamName(stream);
    const r = await this._request(`${this.prefix}.STREAM.DELETE.${stream}`);
    const cr = r as SuccessResponse;
    return cr.success;
  }

  // FIXME: number of keys not editable - like replicas
  // storage_type, mirror, placement
  async update(cfg = {} as StreamConfig): Promise<StreamInfo> {
    validateStreamName(cfg.name);
    const r = await this._request(
      `${this.prefix}.STREAM.UPDATE.${cfg.name}`,
      cfg,
    );
    return r as StreamInfo;
  }

  async info(name: string): Promise<StreamInfo> {
    validateStreamName(name);
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

  async purge(name: string): Promise<PurgeResponse> {
    validateStreamName(name);
    const v = await this._request(`${this.prefix}.STREAM.PURGE.${name}`);
    return v as PurgeResponse;
  }

  async deleteMessage(
    stream: string,
    seq: number,
    erase = true,
  ): Promise<boolean> {
    validateStreamName(stream);
    const dr = { seq } as MsgDeleteRequest;
    if (!erase) {
      dr.no_erase = true;
    }
    const r = await this._request(
      `${this.prefix}.STREAM.MSG.DELETE.${stream}`,
      dr,
    );
    const cr = r as SuccessResponse;
    return cr.success;
  }

  async getMessage(stream: string, seq: number): Promise<StoredMsg> {
    validateStreamName(stream);
    const dr = { seq } as MsgRequest;
    const r = await this._request(
      `${this.prefix}.STREAM.MSG.GET.${stream}`,
      dr,
    );
    const sm = r as StreamMsgResponse;
    return new StoredMsgImpl(sm);
  }

  find(subject: string): Promise<string> {
    return this.findStream(subject);
  }
}
