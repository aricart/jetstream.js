import { DeliveryInfo, JsMsg } from "./jstypes.ts";
import {
  Msg,
  MsgHdrs,
  RequestOptions,
  Subscription,
} from "https://deno.land/x/nats/src/mod.ts";

export const ACK = Uint8Array.of(43, 65, 67, 75);
const NAK = Uint8Array.of(45, 78, 65, 75);
const WPI = Uint8Array.of(43, 87, 80, 73);
const NXT = Uint8Array.of(43, 78, 88, 84);
const TERM = Uint8Array.of(43, 84, 69, 82, 77);

export function toJsMsg(m: Msg): JsMsg {
  return new JsMsgImpl(m);
}

class JsMsgImpl implements JsMsg {
  msg: Msg;
  di?: DeliveryInfo;

  constructor(msg: Msg) {
    this.msg = msg;
  }

  get subject(): string {
    return this.msg.subject;
  }

  get sid(): number {
    return this.msg.sid;
  }

  get data(): Uint8Array {
    return this.msg.data;
  }

  get headers(): MsgHdrs | undefined {
    return this.msg.headers;
  }

  get info(): DeliveryInfo {
    // "$JS.ACK.<stream>.<consumer>.<redeliveryCount><streamSeq><deliverySequence>.<timestamp>.<pending>"

    if (!this.di) {
      const tokens = this.reply.split(".");
      if (tokens.length !== 9 && tokens[0] !== "$JS" && tokens[1] !== "ACK") {
        throw new Error("not js message");
      }

      const di = {} as DeliveryInfo;
      di.stream = tokens[2];
      di.consumer = tokens[3];
      di.redeliveryCount = parseInt(tokens[4], 10);
      di.streamSequence = parseInt(tokens[5], 10);
      di.deliverySequence = parseInt(tokens[6], 10);
      di.timestampNanos = parseInt(tokens[7], 10);
      di.pending = parseInt(tokens[8], 10);
      this.di = di;
    }
    return this.di;
  }

  get redelivered(): boolean {
    return this.info.redeliveryCount > 1;
  }

  get reply(): string {
    return this.msg.reply!;
  }

  get seq(): number {
    return this.info.streamSequence;
  }

  ack() {
    this.msg.respond(ACK);
  }

  nak() {
    this.msg.respond(NAK);
  }

  working() {
    this.msg.respond(WPI);
  }

  next(subj?: string) {
    const opts = subj ? { reply: subj } as RequestOptions : undefined;
    this.msg.respond(NXT, opts);
  }

  term() {
    this.msg.respond(TERM);
  }
}
