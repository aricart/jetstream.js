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

import type { JetStreamClient, JetStreamOptions, JSM } from "./jstypes.ts";

import {
  ErrorCode,
  NatsConnection,
  NatsError,
} from "https://deno.land/x/nats/src/mod.ts";

import { JetStreamManagerImpl } from "./jsm.ts";
import { JetStreamClientImpl } from "./jsclient.ts";

export const defaultPrefix = "$JS.API";
export const defaultTimeout = 5000;

export const JetstreamNotEnabled = "jetstream-not-enabled";
export const InvalidJestreamAck = "invalid-jetstream-ack";

export async function JetStream(
  nc: NatsConnection,
  opts: JetStreamOptions = {} as JetStreamOptions,
): Promise<JetStreamClient> {
  return new JetStreamClientImpl(nc, opts);
}

export async function JetStreamManager(
  nc: NatsConnection,
  opts: JetStreamOptions = {},
): Promise<JSM> {
  const adm = new JetStreamManagerImpl(nc, opts);
  try {
    await adm.getAccountInfo();
  } catch (err) {
    let ne = err as NatsError;
    if (ne.code === ErrorCode.NO_RESPONDERS) {
      ne = new NatsError(JetstreamNotEnabled, JetstreamNotEnabled);
    }
    throw ne;
  }
  return adm;
}