import os from "os";
import net from "net";
import dgram from "dgram";
import crypto from "crypto";
import readline from "readline";
import MapContext from "./map.js";

// Configuration
const DISCOVERY_PORT = 41234;
const BROADCAST_INTERVAL_MS = 3000;
const PEER_TIMEOUT_MS = 10000;

interface PeerInfo {
	id: string;
	name: string;
	address: string;
	tcpPort: number;
	lastSeen: number;
	socket?: net.Socket;
}

interface WireMessage {
	type: 'discovery' | 'chat';
	id: string;
	senderId: string;
	senderName: string;
	payload?: string;
	tcpPort?: number;
}

interface AppOptions {
	username?: string;
	mapWidth?: number;
	mapHeight?: number;
	viewDistance?: number;
}

class P2PContext {
	private peerId: string;
	private username: string;
	private tcpPort: number = 0;
	private tcpServer!: net.Server;
	private udpSocket!: dgram.Socket;
	private peers: Map<string, PeerInfo> = new Map();
	private seenMessages: Set<string> = new Set();
	private rl!: readline.Interface;

	#map: MapContext;

	constructor(options?: AppOptions) {
		options ||= {};
		const id = crypto.randomBytes(4).toString('hex');

		this.#map = new MapContext(options.mapWidth || 100, options.mapHeight || 80, options.viewDistance || 5);
		this.peerId = id;
		this.username = options.username || "User-" + id;
	}

	public async start(): Promise<void> {
		await this.startTcpServer();
		await this.startUdpDiscovery();
		this.setupReadline();
		this.startHeartbeat();

		this.logSystem(`Joined P2P Mesh as "${this.username}" (ID: ${this.peerId})`);
		this.logSystem(`Listening for local peers on UDP :${DISCOVERY_PORT}`);
		this.rl.prompt();
	}

	// --- Network Interface Helpers ---

	private getBroadcastAddresses(): string[] {
		const addresses = new Set<string>(['255.255.255.255']);
		const ifaces = os.networkInterfaces();

		for (const name of Object.keys(ifaces)) {
			for (const iface of ifaces[name] || []) {
				if (iface.family === 'IPv4' && !iface.internal) {
					const ipParts = iface.address.split('.').map(Number);
					const maskParts = iface.netmask.split('.').map(Number);
					const bcastParts = ipParts.map((ip, i) => (ip | (~maskParts[i] & 255)));
					addresses.add(bcastParts.join('.'));
				}
			}
		}
		return Array.from(addresses);
	}

	// --- TCP Messaging Layer ---

	private startTcpServer(): Promise<void> {
		return new Promise((resolve) => {
			this.tcpServer = net.createServer((socket) => {
				this.handleIncomingConnection(socket);
			});

			// Bind to dynamic port
			this.tcpServer.listen(0, '0.0.0.0', () => {
				const addr = this.tcpServer.address() as net.AddressInfo;
				this.tcpPort = addr.port;
				resolve();
			});
		});
	}

	private handleIncomingConnection(socket: net.Socket) {
		let buffer = '';

		socket.on('data', (chunk) => {
			buffer += chunk.toString();
			const lines = buffer.split('\n');
			buffer = lines.pop() || ''; // Keep incomplete trailing fragment

			for (const line of lines) {
				if (!line.trim()) continue;
				try {
					const msg: WireMessage = JSON.parse(line);
					this.processMessage(msg, socket);
				} catch {
					// Ignore malformed payloads
				}
			}
		});

		socket.on('error', () => socket.destroy());
		socket.on('close', () => {
			// Find and remove matching peer socket
			for (const [id, peer] of this.peers) {
				if (peer.socket === socket) {
					peer.socket = undefined;
					break;
				}
			}
		});
	}

	private connectToPeer(peer: PeerInfo) {
		if (peer.socket || peer.id === this.peerId) return;

		const socket = net.createConnection({ port: peer.tcpPort, host: peer.address }, () => {
			peer.socket = socket;
			this.handleIncomingConnection(socket);
			this.logSystem(`Connected directly to "${peer.name}" (${peer.address}:${peer.tcpPort})`);
		});

		socket.on('error', () => {
			peer.socket = undefined;
			socket.destroy();
		});
	}

	// --- UDP Discovery Layer ---

	private startUdpDiscovery(): Promise<void> {
		return new Promise((resolve) => {
			this.udpSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });

			this.udpSocket.on('message', (msgBuffer, rinfo) => {
				try {
					const msg: WireMessage = JSON.parse(msgBuffer.toString());
					if (msg.type === 'discovery' && msg.senderId !== this.peerId) {
						this.handleDiscovery(msg, rinfo.address);
					}
				} catch {
					// Ignore malformed packets
				}
			});

			this.udpSocket.bind(DISCOVERY_PORT, '0.0.0.0', () => {
				this.udpSocket.setBroadcast(true);
				resolve();
			});
		});
	}

	private handleDiscovery(msg: WireMessage, address: string) {
		if (!msg.tcpPort) return;

		let peer = this.peers.get(msg.senderId);

		if (!peer) {
			peer = {
				id: msg.senderId,
				name: msg.senderName,
				address: address,
				tcpPort: msg.tcpPort,
				lastSeen: Date.now(),
			};
			this.peers.set(msg.senderId, peer);
			this.logSystem(`Discovered peer: "${peer.name}" on ${peer.address}:${peer.tcpPort}`);

			// Tie-breaker: Lower peerId initiates the TCP connection to avoid duplicates
			if (this.peerId < peer.id) {
				this.connectToPeer(peer);
			}
		} else {
			peer.name = msg.senderName;
			peer.lastSeen = Date.now();
			peer.address = address;
			peer.tcpPort = msg.tcpPort;

			// Reconnect if socket was dropped
			if (!peer.socket && this.peerId < peer.id) {
				this.connectToPeer(peer);
			}
		}
	}

	private startHeartbeat() {
		setInterval(() => {
			const packet = Buffer.from(
				JSON.stringify({
					type: 'discovery',
					id: crypto.randomUUID(),
					senderId: this.peerId,
					senderName: this.username,
					tcpPort: this.tcpPort,
				} as WireMessage)
			);

			for (const bcast of this.getBroadcastAddresses()) {
				this.udpSocket.send(packet, 0, packet.length, DISCOVERY_PORT, bcast);
			}

			// Cleanup stale peers
			const now = Date.now();
			for (const [id, peer] of this.peers.entries()) {
				if (now - peer.lastSeen > PEER_TIMEOUT_MS) {
					this.logSystem(`Peer "${peer.name}" timed out.`);
					peer.socket?.destroy();
					this.peers.delete(id);
				}
			}
		}, BROADCAST_INTERVAL_MS);
	}

	// --- Message Handling ---

	private processMessage(msg: WireMessage, socket?: net.Socket) {
		if (this.seenMessages.has(msg.id)) return;
		this.seenMessages.add(msg.id);

		// Limit memory usage of seen set
		if (this.seenMessages.size > 2000) {
			const first = this.seenMessages.values().next().value;
			if (first) this.seenMessages.delete(first);
		}

		if (msg.type === 'chat' && msg.payload) {
			const { x, y } = JSON.parse(msg.payload);
			this.#map.setVisited(x, y);

			// Associate socket if not already linked
			const peer = this.peers.get(msg.senderId);
			if (peer && !peer.socket && socket) {
				peer.socket = socket;
			}
		}
	}

	private broadcastChat(text: string) {
		const msgId = crypto.randomUUID();
		this.seenMessages.add(msgId);

		const message: WireMessage = {
			type: 'chat',
			id: msgId,
			senderId: this.peerId,
			senderName: this.username,
			payload: text,
		};

		const serialized = JSON.stringify(message) + '\n';

		for (const peer of this.peers.values()) {
			if (peer.socket && !peer.socket.destroyed) {
				peer.socket.write(serialized);
			}
		}
	}

	// --- Terminal UI ---

	private setupReadline() {
		this.rl = readline.createInterface({
			input: process.stdin,
			output: process.stdout,
			prompt: '\x1b[32m> \x1b[0m',
		});

		this.rl.on('line', (line) => {
			const text = line.trim();
			if (text.length > 0) {
				if (text[0] === "/")
					this.handleCommand(text);

				else {
					const [x, y] = text.split(",", 2);
					const rx = parseFloat(x) || -100;
					const ry = parseFloat(y) || -100;

					if (rx < 0 || ry < 0)
						console.error("Invalid coordinate.");

					else {
						this.#map.setVisited(rx, ry);
						this.broadcastChat(JSON.stringify({ x, y }, void 0, "\t"));
					}
				}
			}

			this.rl.prompt();
		});

		this.rl.on('close', () => {
			this.logSystem('Leaving chat...');
			this.udpSocket.close();
			this.tcpServer.close();
			for (const peer of this.peers.values()) {
				peer.socket?.destroy();
			}
			process.exit(0);
		});
	}

	private handleCommand(cmd: string) {
		const [command, ...args] = cmd.slice(1).split(' ');
		switch (command) {
			case 'peers':
				this.logSystem(`Connected Peers (${this.peers.size}):`);
				for (const peer of this.peers.values()) {
					const status = peer.socket && !peer.socket.destroyed ? 'CONNECTED' : 'DISCOVERED';
					console.log(` - ${peer.name} [${peer.id}] (${peer.address}:${peer.tcpPort}) [${status}]`);
				}
				break;
			case 'nick':
				if (args[0]) {
					this.username = args[0];
					this.logSystem(`Changed nickname to "${this.username}"`);
				}
				break;
			case 'help':
				this.logSystem('Available commands:');
				console.log(' /peers       - List active peers on LAN');
				console.log(' /nick <name> - Change your username');
				console.log(' /exit        - Exit the app');
				break;
			case 'exit':
				this.rl.close();
				break;
			case "map":
				console.log("Map: \r\n", this.#map.toString());
				break;
			default:
				this.logSystem(`Unknown command: /${command}`);
		}
	}

	private logSystem(text: string) {
		readline.cursorTo(process.stdout, 0);
		readline.clearLine(process.stdout, 0);
		console.log(`\x1b[33m* ${text}\x1b[0m`);
		if (this.rl) this.rl.prompt(true);
	}


}

await new P2PContext().start();



