// search-agent.ts — the search agent itself, as a reusable class.
//
// Each agent "sweeps" cells of a SIZE x SIZE grid, gossips what it has swept
// over the transport, and merges what it hears. While its radio is off it keeps
// searching on its own; when it comes back, maps merge with no conflicts.
//
// Used by agent.ts (terminal demo) and main.ts (HTTP API for the web UI).
//
// Events:
//   'log'    (line: string)  human-readable event, e.g. "peer B joined"
//   'change' ()              state changed (tick, merge, peer, radio) — re-read state()

import { EventEmitter } from 'node:events';
import { UdpTransport, type Packet } from './transport.js';

export type Pos = [number, number];
export type Cell = `${number},${number}`;

export interface Found {
	cell: Cell;
	by: string;
	t: number;
}

/** What goes over the wire in a 'state' message. */
export interface StateBody {
	swept: Record<string, string[]>;
	found: Found | null;
	/** sender's current position, so peers can draw where everyone is */
	pos?: Pos;
}

export interface PeerState {
	id: string;
	pos: Pos | null;
	lastSeen: number;
}

/** Everything the web UI needs, as plain JSON. */
export interface AgentState {
	id: string;
	size: number;
	pos: Pos;
	offline: boolean;
	peers: PeerState[];
	/** cell "x,y" -> ids of agents known to have swept it */
	swept: Record<string, string[]>;
	sweptCount: number;
	found: Found | null;
	done: boolean;
	t: number;
}

export interface SearchAgentOptions {
	id: string;
	size?: number;
	tickMs?: number;
	/** where the casualty is; random if omitted (so give all agents the same one for a real demo) */
	casualty?: Cell;
	transport?: UdpTransport;
}

/** Parse "x,y" into a Cell inside a size x size grid, or undefined if invalid. */
export function parseCell(s: string | undefined, size = 10): Cell | undefined {
	const m = s?.trim().match(/^(\d+),(\d+)$/);
	if (!m) return undefined;
	const x = Number(m[1]), y = Number(m[2]);
	return x < size && y < size ? `${x},${y}` : undefined;
}

const randInt = (n: number) => Math.floor(Math.random() * n);

export class SearchAgent extends EventEmitter {
	readonly id: string;
	readonly size: number;
	readonly net: UdpTransport;
	private readonly tickMs: number;
	private readonly casualty: Cell;

	// ---- State that merges cleanly (grow-only) --------------------------------
	// swept[cell] = Set of agent ids that swept it. Merge = set union, so the
	// result is the same whatever order messages arrive in, and repeats are harmless.
	private readonly swept = new Map<string, Set<string>>();
	private found: Found | null = null; // once set, never unset

	private pos: Pos;
	private readonly peerPos = new Map<string, Pos>();
	private timer?: NodeJS.Timeout;

	constructor({ id, size = 10, tickMs = 1000, casualty, transport }: SearchAgentOptions) {
		super();
		this.id = id;
		this.size = size;
		this.tickMs = tickMs;
		this.casualty = casualty ?? `${randInt(size)},${randInt(size)}`;
		this.pos = [randInt(size), randInt(size)];
		this.net = transport ?? new UdpTransport({ id });
	}

	async start(): Promise<void> {
		this.net.onPeer((ev) => {
			if (ev.status === 'lost') this.peerPos.delete(ev.id);
			this.log(`peer ${ev.id} ${ev.status}`);
			this.emit('change');
		});
		this.net.onMessage((msg: Packet) => {
			if (msg.type !== 'state') return;
			const body = msg.body as Partial<StateBody>;
			if (Array.isArray(body.pos)) this.peerPos.set(msg.from, body.pos);
			const n = this.merge(body);
			if (n) this.log(`merged ${n} new sweep(s) from ${msg.from}`);
			this.emit('change');
		});
		await this.net.start();
		this.log(`online — group ${this.net.group}:${this.net.port}`);
		this.timer = setInterval(() => this.tick(), this.tickMs);
	}

	stop(): void {
		clearInterval(this.timer);
		this.net.stop();
	}

	get offline(): boolean { return this.net.offline; }

	setOffline(offline: boolean): void {
		if (offline === this.net.offline) return;
		this.net.setOffline(offline);
		if (offline) this.peerPos.clear();
		this.log(offline ? 'radio OFF — searching alone' : 'radio ON');
		this.emit('change');
	}

	state(): AgentState {
		return {
			id: this.id,
			size: this.size,
			pos: this.pos,
			offline: this.net.offline,
			peers: [...this.net.peers].map(([id, p]) => ({ id, pos: this.peerPos.get(id) ?? null, lastSeen: p.lastSeen })),
			swept: this.sweptObject(),
			sweptCount: this.swept.size,
			found: this.found,
			done: this.found !== null || this.swept.size >= this.size * this.size,
			t: Date.now(),
		};
	}

	/** ASCII map, '#' = swept, '.' = not yet, '@' = me. */
	map(): string {
		const rows: string[] = [];
		for (let y = 0; y < this.size; y++) {
			let row = '';
			for (let x = 0; x < this.size; x++) {
				row += x === this.pos[0] && y === this.pos[1] ? '@' : this.swept.has(`${x},${y}`) ? '#' : '.';
			}
			rows.push(row);
		}
		const peers = [...this.net.peers.keys()].join(', ') || 'none';
		return `map (${this.swept.size}/${this.size * this.size} swept, peers: ${peers})\n${rows.join('\n')}`;
	}

	// ---- internals --------------------------------------------------------------

	private tick(): void {
		this.step();
		this.net.send('state', { swept: this.sweptObject(), found: this.found, pos: this.pos } satisfies StateBody);
		this.emit('change');
	}

	private step(): void {
		if (this.found) return;
		const target = this.nextCell();
		if (!target) return;
		this.pos = target;
		const cell: Cell = `${target[0]},${target[1]}`;
		this.markSwept(cell, this.id);
		if (cell === this.casualty) {
			this.found = { cell, by: this.id, t: Date.now() };
			this.log(`FOUND casualty at ${cell}`);
		} else if (this.swept.size >= this.size * this.size) {
			this.log('grid fully swept');
		}
	}

	/** Independent decision: the nearest cell nobody is known to have swept. */
	private nextCell(): Pos | null {
		let best: Pos | null = null, bestD = Infinity;
		for (let x = 0; x < this.size; x++) for (let y = 0; y < this.size; y++) {
			if (this.swept.has(`${x},${y}`)) continue;
			const d = Math.abs(x - this.pos[0]) + Math.abs(y - this.pos[1]) + Math.random() * 0.5; // jitter spreads agents out
			if (d < bestD) { bestD = d; best = [x, y]; }
		}
		return best;
	}

	private markSwept(cell: string, by: string): void {
		let set = this.swept.get(cell);
		if (!set) { set = new Set(); this.swept.set(cell, set); }
		set.add(by);
	}

	private merge(remote: Partial<StateBody>): number {
		let learned = 0;
		for (const [cell, by] of Object.entries(remote.swept ?? {})) {
			if (!Array.isArray(by)) continue;
			for (const who of by) {
				if (!this.swept.get(cell)?.has(who)) learned++;
				this.markSwept(cell, who);
			}
		}
		if (!this.found && remote.found) {
			this.found = remote.found;
			this.log(`heard: casualty found at ${this.found.cell} by ${this.found.by}`);
		}
		return learned;
	}

	private sweptObject(): Record<string, string[]> {
		const obj: Record<string, string[]> = {};
		for (const [cell, by] of this.swept) obj[cell] = [...by];
		return obj;
	}

	private log(line: string): void { this.emit('log', line); }
}
