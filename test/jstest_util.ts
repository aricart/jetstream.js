import * as path from "https://deno.land/std@0.83.0/path/mod.ts";

import { NatsServer } from "https://deno.land/x/nats/tests/helpers/mod.ts";
import { JetStreamManager } from "../src/jetstream.ts";

import {
  connect,
  ConnectionOptions,
  NatsConnection,
} from "https://deno.land/x/nats/src/mod.ts";

import {
  extend,
  nuid,
} from "https://deno.land/x/nats/nats-base-client/internal_mod.ts";

export const jsopts = {
  // debug: true,
  // trace: true,
  jetstream: {
    max_file_store: 1024 * 1024,
    max_memory_store: 1024 * 1024,
    store_dir: "/tmp",
  },
};

export function JetStreamConfig(opts = {}, randomStoreDir = true): any {
  const conf = Object.assign(opts, jsopts);
  if (randomStoreDir) {
    conf.jetstream.store_dir = path.join("/tmp", "jetstream", nuid.next());
  }
  Deno.mkdirSync(conf.jetstream.store_dir, { recursive: true });

  return opts;
}

export async function setup(
  serverConf?: any,
  clientOpts?: any,
): Promise<{ ns: NatsServer; nc: NatsConnection }> {
  const ns = await NatsServer.start(serverConf);
  clientOpts = clientOpts ? clientOpts : {};
  const copts = extend({ port: ns.port }, clientOpts) as ConnectionOptions;
  const nc = await connect(copts);
  return { ns, nc };
}

export async function cleanup(
  ns: NatsServer,
  nc: NatsConnection,
): Promise<void> {
  await nc.close();
  await ns.stop();
}

export async function initStream(
  nc: NatsConnection,
  stream: string = nuid.next(),
): Promise<{ stream: string; subj: string }> {
  const jsm = await JetStreamManager(nc);
  const subj = `${stream}.A`;
  await jsm.streams.add(
    { name: stream, subjects: [subj] },
  );
  return { stream, subj };
}
