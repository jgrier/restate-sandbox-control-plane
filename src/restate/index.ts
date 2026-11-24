import * as restate from "@restatedev/restate-sdk";
import { ports } from "../config.js";
import { controlPlaneLog } from "./common.js";
import { Conversation } from "./conversation.js";
import { Disk } from "./disk.js";
import { Sandbox } from "./sandbox.js";
import { Session } from "./session.js";

restate.serve({
  services: [Conversation, Session, Sandbox, Disk],
  port: ports.services,
  logger: controlPlaneLog,
});
