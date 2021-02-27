import { Consumer, ConsumerConfig } from "./types.ts";
import {
  createInbox,
  nuid,
} from "https://deno.land/x/nats/nats-base-client/internal_mod.ts";
import { PushConsumer, PushConsumerConfig } from "./consumer_api.ts";

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

export function defaultConsumer(
  name: string,
  opts: Partial<ConsumerConfig> = {},
): ConsumerConfig {
  return Object.assign({
    name: name,
    deliver_policy: "all",
    ack_policy: "explicit",
    ack_wait: ns(30 * 1000),
    replay_policy: "instant",
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

export function ns(millis: number) {
  return millis * 1000000;
}

export function ms(ns: number) {
  return ns / 1000000;
}
