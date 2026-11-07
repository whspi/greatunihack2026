// main.ts — runs one search agent and exposes it over HTTP for the web UI.
//
//   npm run server -- A 8081
//   npm run server -- B 8082
//
// args: [agentId] [httpPort]   (or env AGENT_ID / PORT; CASUALTY=x,y for a shared target)
//
// API (CORS open, JSON):
//   GET  /state           current AgentState
//   GET  /events          Server-Sent Events: one AgentState per change (~1/s)
//   POST /radio           body {"offline": true|false}  -> cut / restore this agent's radio
//   GET  /health          {"ok": true}

import http from 'node:http';
import { SearchAgent, parseCell } from './search-agent.js';

const id = process.argv[2] ?? process.env.AGENT_ID ?? `agent-${Math.floor(Math.random() * 1000)}`;
const port = Number(process.argv[3] ?? process.env.PORT ?? 8080);

const agent = new SearchAgent({ id, casualty: parseCell(process.env.CASUALTY) });
agent.on('log', (s: string) => console.log(`[${id}] ${s}`));

// ---- Server-Sent Events -------------------------------------------------------
const clients = new Set<http.ServerResponse>();
agent.on('change', () => {
	if (clients.size === 0) return;
	const frame = `data: ${JSON.stringify(agent.state())}\n\n`;
	for (const res of clients) res.write(frame);
});
// keep idle connections (and proxies) from timing out
setInterval(() => { for (const res of clients) res.write(': ping\n\n'); }, 15000).unref();

// ---- helpers ------------------------------------------------------------------
function setCors(res: http.ServerResponse): void {
	res.setHeader('Access-Control-Allow-Origin', '*');
	res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
	res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

function sendJson(res: http.ServerResponse, status: number, body: unknown): void {
	res.writeHead(status, { 'Content-Type': 'application/json' });
	res.end(JSON.stringify(body));
}

function readJson(req: http.IncomingMessage, limit = 64 * 1024): Promise<unknown> {
	return new Promise((resolve, reject) => {
		let data = '';
		req.setEncoding('utf8');
		req.on('data', (chunk: string) => {
			data += chunk;
			if (data.length > limit) { reject(new Error('body too large')); req.destroy(); }
		});
		req.on('end', () => {
			try { resolve(data ? JSON.parse(data) : {}); } catch { reject(new Error('invalid JSON')); }
		});
		req.on('error', reject);
	});
}

// ---- routes -------------------------------------------------------------------
const server = http.createServer(async (req, res) => {
	setCors(res);
	const { pathname } = new URL(req.url ?? '/', 'http://localhost');

	try {
		if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

		if (req.method === 'GET' && pathname === '/health') return sendJson(res, 200, { ok: true, id });

		if (req.method === 'GET' && pathname === '/state') return sendJson(res, 200, agent.state());

		if (req.method === 'GET' && pathname === '/events') {
			res.writeHead(200, {
				'Content-Type': 'text/event-stream',
				'Cache-Control': 'no-cache',
				Connection: 'keep-alive',
			});
			res.write(`data: ${JSON.stringify(agent.state())}\n\n`);
			clients.add(res);
			req.on('close', () => clients.delete(res));
			return;
		}

		if (req.method === 'POST' && pathname === '/radio') {
			const body = await readJson(req) as { offline?: unknown };
			if (typeof body.offline !== 'boolean') return sendJson(res, 400, { error: 'expected {"offline": true|false}' });
			agent.setOffline(body.offline);
			return sendJson(res, 200, agent.state());
		}

		sendJson(res, 404, { error: `no route ${req.method} ${pathname}` });
	} catch (err) {
		sendJson(res, 400, { error: err instanceof Error ? err.message : String(err) });
	}
});

await agent.start();
server.listen(port, '0.0.0.0', () => console.log(`[${id}] http on http://localhost:${port}`));

const shutdown = () => { agent.stop(); server.close(); process.exit(0); };
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
