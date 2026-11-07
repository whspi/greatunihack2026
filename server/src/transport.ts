
// transport.ts — serverless peer-to-peer messaging over a local network.
//
// No internet, no broker, no central server. Every agent joins a UDP multicast
// group on whatever local network exists (venue Wi-Fi, a phone hotspot with
// mobile data OFF, a travel router, or just one laptop). Anything an agent sends
// reaches every other agent on that network; each agent decides for itself what
// to do with it.
//
// Swap this file for a LoRa / Bluetooth transport later: agents only use
// start(), send(), onMessage(), onPeer() and stop().

import dgram from 'node:dgram';
import os from 'node:os';
import { EventEmitter } from 'node:events';

const DEFAULT_GROUP = '239.255.42.99'; // "administratively scoped" multicast: stays on the local network
const DEFAULT_PORT = 41234;

/** Every packet on the wire looks like this. */
export interface Packet<B = unknown> {
	v: 1;
	from: string;
	seq: number;
	t: number;
	type: string;
	body: B;
}

export interface PacketMeta {
	address: string;
	port: number;
}

export interface PeerInfo {
	address: string;
	lastSeen: number;
}

export type PeerEvent =
	| { id: string; status: 'joined'; address: string }
	| { id: string; status: 'lost' };

export interface TransportOptions {
	/** this agent's id */
	id: string;
	/** multicast group */
	group?: string;
	/** UDP port shared by all agents */
	port?: number;
	/** how often to announce "I'm here" */
	heartbeatMs?: number;
	/** forget a peer after this much silence */
	peerTimeoutMs?: number;
	/**
	 * Optional receive-side filter, e.g. simulated range / line of sight.
	 * Return false to drop the packet as if the radio never heard it.
	 */
	radioFilter?: (msg: Packet, meta: PacketMeta) => boolean;
}

export class UdpTransport extends EventEmitter {
	readonly id: string;
	readonly group: string;
	readonly port: number;
	readonly heartbeatMs: number;
	readonly peerTimeoutMs: number;
	radioFilter: (msg: Packet, meta: PacketMeta) => boolean;
	readonly peers = new Map<string, PeerInfo>();
	offline = false; // set true to simulate "radio off" for this agent

	private seq = 0;
	private sock?: dgram.Socket;
	private hb?: NodeJS.Timeout;
	private sweep?: NodeJS.Timeout;

	constructor({
		id,
		group = DEFAULT_GROUP,
		port = DEFAULT_PORT,
		heartbeatMs = 1000,
		peerTimeoutMs = 4000,
		radioFilter = () => true,
	}: TransportOptions) {
		super();
		if (!id) throw new Error('id is required');
		this.id = String(id);
		this.group = group;
		this.port = port;
		this.heartbeatMs = heartbeatMs;
		this.peerTimeoutMs = peerTimeoutMs;
		this.radioFilter = radioFilter;
	}

	start(): Promise<void> {
		return new Promise((resolve, reject) => {
			// reuseAddr lets several agents share one port, including on one laptop.
			const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });
			this.sock = sock;
			sock.on('error', (err: Error) => {
				this.emit('error', err);
				reject(err);
			});
			sock.on('message', (buf: Buffer, rinfo: dgram.RemoteInfo) => this.#receive(buf, rinfo));
			sock.bind(this.port, () => {
				try {
					for (const iface of localIPv4Interfaces()) {
						try { sock.addMembership(this.group, iface); } catch { /* interface may not support multicast */ }
					}
					sock.setMulticastTTL(1);         // never leave the local network
					sock.setMulticastLoopback(true); // agents on the same machine hear each other
				} catch (err) {
					return reject(err);
				}
				this.hb = setInterval(() => this.#heartbeat(), this.heartbeatMs);
				this.sweep = setInterval(() => this.#expirePeers(), 500);
				this.#heartbeat();
				resolve();
			});
		});
	}

	/** Send to everyone in range. `type` is free-form, `body` must be JSON-serialisable. */
	send(type: string, body: unknown = {}): void {
		if (this.offline || !this.sock) return;
		const packet: Packet = { v: 1, from: this.id, seq: ++this.seq, t: Date.now(), type, body };
		const buf = Buffer.from(JSON.stringify(packet));
		if (buf.length > 60000) throw new Error(`message too large for one UDP packet: ${buf.length} bytes`);
		this.sock.send(buf, this.port, this.group);
	}

	onMessage(cb: (msg: Packet, meta: PacketMeta) => void): this { this.on('message', cb); return this; }
	onPeer(cb: (ev: PeerEvent) => void): this { this.on('peer', cb); return this; }

	setOffline(offline: boolean): void {
		this.offline = offline;
		if (offline) {
			// radio off: we can no longer hear anyone, so drop peers now rather than waiting for the timeout
			for (const id of [...this.peers.keys()]) {
				this.peers.delete(id);
				this.emit('peer', { id, status: 'lost' } satisfies PeerEvent);
			}
		}
		this.emit('radio', offline ? 'off' : 'on');
	}

	stop(): void {
		clearInterval(this.hb);
		clearInterval(this.sweep);
		try { this.sock?.close(); } catch { /* already closed */ }
	}

	#heartbeat(): void { this.send('hello'); }

	#receive(buf: Buffer, rinfo: dgram.RemoteInfo): void {
		if (this.offline) return; // our own radio is off: we hear nothing
		let msg: Packet;
		try { msg = JSON.parse(buf.toString()); } catch { return; }
		if (msg?.v !== 1 || !msg.from || msg.from === this.id) return; // ignore junk and our own echo
		const meta: PacketMeta = { address: rinfo.address, port: rinfo.port };
		if (!this.radioFilter(msg, meta)) return;

		const known = this.peers.has(msg.from);
		this.peers.set(msg.from, { address: rinfo.address, lastSeen: Date.now() });
		if (!known) this.emit('peer', { id: msg.from, status: 'joined', address: rinfo.address } satisfies PeerEvent);

		if (msg.type !== 'hello') this.emit('message', msg, meta);
	}

	#expirePeers(): void {
		const now = Date.now();
		for (const [id, p] of this.peers) {
			if (now - p.lastSeen > this.peerTimeoutMs) {
				this.peers.delete(id);
				this.emit('peer', { id, status: 'lost' } satisfies PeerEvent);
			}
		}
	}
}

function localIPv4Interfaces(): string[] {
	const out: string[] = [];
	for (const addrs of Object.values(os.networkInterfaces())) {
		for (const a of addrs ?? []) if (a.family === 'IPv4') out.push(a.address);
	}
	return out.length ? out : ['0.0.0.0'];
}
