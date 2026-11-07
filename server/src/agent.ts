// agent.ts — terminal demo of one search agent.
//
//   npm run agent -- A
//   npm run agent -- B
//   npm run agent -- C
//
// Type `off` / `on` + Enter to cut this agent's radio, `status` to print its
// map, `quit` to exit. Set CASUALTY=x,y to give every agent the same target.

import readline from 'node:readline';
import { SearchAgent, parseCell } from './search-agent.js';

const id = process.argv[2] ?? `agent-${Math.floor(Math.random() * 1000)}`;
const agent = new SearchAgent({ id, casualty: parseCell(process.env.CASUALTY) });
agent.on('log', (s: string) => console.log(`[${id}] ${s}`));
await agent.start();

const rl = readline.createInterface({ input: process.stdin });
rl.on('line', (line: string) => {
	const cmd = line.trim();
	if (cmd === 'off') agent.setOffline(true);
	else if (cmd === 'on') agent.setOffline(false);
	else if (cmd === 'status') console.log(`[${id}] ${agent.map()}`);
	else if (cmd === 'quit') { agent.stop(); process.exit(0); }
});
