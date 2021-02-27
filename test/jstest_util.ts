import * as path from "https://deno.land/std@0.83.0/path/mod.ts";
import { NatsServer } from "https://deno.land/x/nats/tests/helpers/mod.ts";
import { connect } from "../src/nats_deno.ts";

import {
  ConnectionOptions,
  extend,
  NatsConnection,
  nuid,
} from "../src/nbc_mod.ts";
import { JetStreamManager } from "../src/jetstream.ts";

export const jsopts = {
  // debug: true,
  // trace: true,
  jetstream: {
    max_file_store: 1024 * 1024,
    max_memory_store: 1024 * 1024,
    store_dir: "/tmp",
  },
};

export function JetStreamConfig(
  opts = {},
  randomStoreDir = true,
): Record<string, unknown> {
  const conf = Object.assign(opts, jsopts);
  if (randomStoreDir) {
    conf.jetstream.store_dir = path.join("/tmp", "jetstream", nuid.next());
  }
  Deno.mkdirSync(conf.jetstream.store_dir, { recursive: true });

  return opts;
}
export async function setup(
  serverConf?: Record<string, unknown>,
  clientOpts?: Partial<ConnectionOptions>,
): Promise<{ ns: NatsServer; nc: NatsConnection }> {
  const dt = serverConf as { debug: boolean; trace: boolean };
  const debug = dt && (dt.debug || dt.trace);
  const ns = await NatsServer.start(serverConf, debug);
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
