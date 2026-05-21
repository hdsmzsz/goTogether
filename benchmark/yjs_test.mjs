// End-to-end Yjs CRDT test.
//   1. Connect two clients to the same doc
//   2. Each makes concurrent edits
//   3. Verify they converge
//   4. Close, reopen one, verify state recovered from server
// Usage: DOC_ID=<id> node yjs_test.mjs

import * as Y from 'yjs';
import WebSocket from 'ws';

const docID = process.env.DOC_ID;
if (!docID) {
    console.error('Usage: DOC_ID=<id> node yjs_test.mjs');
    process.exit(1);
}
const WS_URL = `ws://localhost:8082/ws?doc_id=${docID}`;

function uint8ToBase64(arr) {
    return Buffer.from(arr).toString('base64');
}
function base64ToUint8(b64) {
    return new Uint8Array(Buffer.from(b64, 'base64'));
}

class Client {
    constructor(name) {
        this.name = name;
        this.ydoc = new Y.Doc();
        this.ytext = this.ydoc.getText('content');
        this.ws = null;
        this.applied = 0;
        this.sent = 0;
    }
    async connect() {
        return new Promise((resolve) => {
            this.ws = new WebSocket(WS_URL);
            this.ws.on('open', () => {
                const state = Y.encodeStateAsUpdate(this.ydoc);
                if (state.length > 2) {
                    this.ws.send(JSON.stringify({type: 'update', payload: uint8ToBase64(state)}));
                }
                resolve();
            });
            this.ws.on('message', (data) => {
                const msg = JSON.parse(data.toString());
                if (msg.type === 'sync' && Array.isArray(msg.updates)) {
                    for (const u of msg.updates) {
                        Y.applyUpdate(this.ydoc, base64ToUint8(u), 'remote');
                        this.applied++;
                    }
                } else if (msg.type === 'update' && msg.payload) {
                    Y.applyUpdate(this.ydoc, base64ToUint8(msg.payload), 'remote');
                    this.applied++;
                }
            });
            this.ydoc.on('update', (update, origin) => {
                if (origin === 'remote') return;
                if (this.ws.readyState === WebSocket.OPEN) {
                    this.ws.send(JSON.stringify({type: 'update', payload: uint8ToBase64(update)}));
                    this.sent++;
                }
            });
        });
    }
    insert(pos, text) {
        this.ytext.insert(pos, text);
    }
    close() {
        if (this.ws) this.ws.close();
    }
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
    console.log(`Connecting two clients to doc ${docID}...`);
    const alice = new Client('alice');
    const bob = new Client('bob');
    await alice.connect();
    await bob.connect();
    await sleep(200);

    console.log('\nPhase 1: Concurrent edits');
    alice.insert(0, 'Hello from Alice. ');
    bob.insert(0, 'Bob writes here. ');
    await sleep(500);
    console.log(`  alice text: "${alice.ytext.toString()}"`);
    console.log(`  bob text:   "${bob.ytext.toString()}"`);
    const converged1 = alice.ytext.toString() === bob.ytext.toString();
    console.log(`  Converged: ${converged1 ? 'YES' : 'NO'}`);

    console.log('\nPhase 2: More edits');
    alice.insert(alice.ytext.length, '\nAlice line 2.');
    bob.insert(0, '[BOB-PREFIX] ');
    await sleep(500);
    console.log(`  alice text: "${alice.ytext.toString()}"`);
    console.log(`  bob text:   "${bob.ytext.toString()}"`);
    const converged2 = alice.ytext.toString() === bob.ytext.toString();
    console.log(`  Converged: ${converged2 ? 'YES' : 'NO'}`);

    const finalText = alice.ytext.toString();

    console.log('\nPhase 3: Wait for snapshot persistence (idle 5s + 1s slack)');
    await sleep(7000);

    console.log('\nPhase 4: Both disconnect, then reconnect a fresh client');
    alice.close();
    bob.close();
    await sleep(500);

    const charlie = new Client('charlie');
    await charlie.connect();
    await sleep(1500);
    console.log(`  charlie text: "${charlie.ytext.toString()}"`);
    const recovered = charlie.ytext.toString() === finalText;
    console.log(`  State recovered: ${recovered ? 'YES' : 'NO'}`);
    charlie.close();

    console.log('\n========================================');
    const allPassed = converged1 && converged2 && recovered;
    console.log(`Result: ${allPassed ? 'PASS' : 'FAIL'}`);
    console.log('========================================');
    process.exit(allPassed ? 0 : 1);
}

main().catch(e => { console.error(e); process.exit(1); });
