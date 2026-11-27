import { readFileSync } from "node:fs";

const requiredTools = [
  "whoami",
  "list_participants",
  "send_message",
  "ask_user",
  "blob_put",
].map((name) => `mcp__cooplet__${name}`);

export default function participant(pi) {
  // Pi can start package-manager children before this extension loads.
  // Keep participant credentials in the private startup file instead of the environment.
  let startup;
  try {
    startup = JSON.parse(readFileSync(new URL("./startup.json", import.meta.url), "utf8"));
  } catch {
    throw new Error("Cooplet participant startup configuration is unavailable");
  }
  const { credential, executable, instructions } = startup ?? {};
  if (typeof instructions !== "string" || !instructions.trim()) {
    throw new Error("Frozen Cooplet instructions are unavailable");
  }
  if (typeof credential !== "string" || !credential || typeof executable !== "string" || !executable) {
    throw new Error("Cooplet participant configuration is unavailable");
  }

  pi.registerMcpServer("cooplet", {
    command: executable,
    args: ["node", "mcp", "--participant-cred", credential],
    exposure: "direct",
  });
  pi.on("before_agent_start", (event) => {
    event.systemPromptOptions.sections.cooplet = instructions;
  });

  pi.registerCommand("cooplet-check-participant", {
    description: "Check the active Cooplet participant tools",
    handler: async (_args, ctx) => {
      const registered = new Set(pi.getAllTools()
        .filter((tool) => tool.exposure === "direct" && tool.sourceInfo?.path === "builtin:mcp")
        .map((tool) => tool.name));
      const active = new Set(pi.getActiveTools());
      const tools = requiredTools.filter((name) => registered.has(name) && active.has(name));
      ctx.ui.notify(JSON.stringify({ coopletParticipantTools: tools }), "info");
    },
  });
}
