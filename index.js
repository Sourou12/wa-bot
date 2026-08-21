const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const express = require('express');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json({ limit: '10mb' }));

const JOB_FILE = path.join(__dirname, 'bulk_job.json');
let sock;
let isReady = false;
let bulkJob = loadJobFromFile();

// --- GESTION DU FICHIER DE PERSISTANCE ---
function loadJobFromFile() {
    try {
        if (fs.existsSync(JOB_FILE)) {
            const data = fs.readFileSync(JOB_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (e) {
        console.error('Erreur lecture job local :', e);
    }
    return null;
}

function saveJobToFile() {
    if (!bulkJob) return;
    try {
        fs.writeFileSync(JOB_FILE, JSON.stringify(bulkJob, null, 2));
    } catch (e) {
        console.error('Erreur sauvegarde job local :', e);
    }
}

// --- CONNEXION WHATSAPP ---
async function connectToWhatsApp() {
    try {
        const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');

        sock = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            browser: ["Ecole Marie Auxiliatrice", "Chrome", "1.0.0"],
            keepAliveIntervalMs: 30000,
            connectTimeoutMs: 60000
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', (update) => {
            const { connection, lastDisconnect, qr } = update;

            if (qr) {
                isReady = false;
                console.log('--- SCANNEZ LE QR CODE ---');
                qrcode.generate(qr, { small: true });
            }

            if (connection === 'close') {
                isReady = false;
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                console.log(`Connexion fermée (Code: ${statusCode}). Reconnexion :`, shouldReconnect);
                if (shouldReconnect) setTimeout(connectToWhatsApp, 5000);
            } else if (connection === 'open') {
                isReady = true;
                console.log('✅ Connecté à WhatsApp avec succès !');
                // Reprendre un job interrompu lors d'un redémarrage
                if (bulkJob && (bulkJob.status === 'running' || bulkJob.status === 'pending')) {
                    console.log('🔄 Reprise de l\'envoi en masse en cours...');
                    processBulkJob();
                }
            }
        });
    } catch (err) {
        console.error('Erreur d\'initialisation Baileys :', err);
    }
}

connectToWhatsApp();

// --- UTILITAIRES ---
function formatNumber(rawNumber) {
    let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');
    if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) cleanNumber = '229' + cleanNumber;
    else if (cleanNumber.length === 8) cleanNumber = '229' + cleanNumber;
    return `${cleanNumber}@s.whatsapp.net`;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function randomDelayMs(minSeconds, maxSeconds) {
    return Math.floor(Math.random() * (maxSeconds - minSeconds + 1) + minSeconds) * 1000;
}

// --- MOTEUR D'ENVOI EN MASSE OPTIMISÉ ---
async function processBulkJob() {
    if (!bulkJob || bulkJob.status === 'completed' || bulkJob.cancelled) return;

    bulkJob.status = 'running';
    saveJobToFile();

    for (let i = bulkJob.currentIndex; i < bulkJob.items.length; i++) {
        if (bulkJob.cancelled) {
            bulkJob.status = 'cancelled';
            saveJobToFile();
            return;
        }

        while (!isReady && !bulkJob.cancelled) {
            console.log('[BULK] Attente de la connexion WhatsApp...');
            await sleep(10000);
        }

        const item = bulkJob.items[i];
        bulkJob.currentIndex = i;

        try {
            const recipientJid = formatNumber(item.number);
            let targetJid = recipientJid;

            try {
                const [exists] = await sock.onWhatsApp(recipientJid);
                if (exists && exists.jid) targetJid = exists.jid;
            } catch (e) {}

            // 1. Simulation d'écriture pour passer sous les radars anti-bot
            await sock.sendPresenceUpdate('composing', targetJid);
            await sleep(3000); // Fait croire que vous tapez depuis 3s

            // 2. Envoi du message
            const result = await sock.sendMessage(targetJid, { text: item.message });

            bulkJob.results.push({ number: item.number, jid: targetJid, status: 'success', id_message: result?.key?.id });
            bulkJob.sentCount++;
            console.log(`[BULK ${i + 1}/${bulkJob.items.length}] ✅ Envoyé à ${targetJid}`);

        } catch (error) {
            bulkJob.results.push({ number: item.number, status: 'error', error: error.message });
            bulkJob.failedCount++;
            console.error(`[BULK ${i + 1}/${bulkJob.items.length}] ❌ Échec ${item.number} :`, error.message);
        }

        saveJobToFile(); // Sauvegarde de l'avancement après chaque message

        // 3. Gestion des délais
        const isLast = i === bulkJob.items.length - 1;
        if (!isLast) {
            // Pause de sécurité toutes les 10 expéditions (pause de 5 à 8 minutes)
            if ((i + 1) % 10 === 0) {
                const pauseMs = randomDelayMs(300, 480);
                console.log(`[PAUSE DE SÉCURITÉ] ☕ Pause de ${Math.round(pauseMs / 60000)} minutes pour protéger le compte...`);
                await sleep(pauseMs);
            } else {
                // Pause standard aléatoire (entre 35s et 60s par défaut)
                const delayMs = randomDelayMs(bulkJob.minDelaySec, bulkJob.maxDelaySec);
                bulkJob.nextSendAt = Date.now() + delayMs;
                console.log(`[BULK] Prochain message dans ${Math.round(delayMs / 1000)}s...`);
                await sleep(delayMs);
            }
        }
    }

    bulkJob.status = 'completed';
    bulkJob.finishedAt = new Date().toISOString();
    saveJobToFile();
    console.log(`[BULK] Terminé : ${bulkJob.sentCount} succès, ${bulkJob.failedCount} échecs.`);
}

// --- ROUTES API ---
app.get('/ping', (req, res) => res.status(200).send('pong'));

app.get('/', (req, res) => {
    res.json({ status: isReady ? 'connected' : 'waiting_qr' });
});

app.post('/send-bulk-messages', (req, res) => {
    if (!isReady) return res.status(503).json({ error: 'WhatsApp non connecté.' });
    if (bulkJob && bulkJob.status === 'running') return res.status(409).json({ error: 'Un envoi est déjà en cours.' });

    const messages = req.body.messages;
    if (!Array.isArray(messages) || messages.length === 0) return res.status(400).json({ error: 'Tableau "messages" requis.' });

    bulkJob = {
        id: Date.now().toString(),
        items: messages.map(m => ({ number: m.number || m.phone, message: m.message || m.text })),
        currentIndex: 0,
        sentCount: 0,
        failedCount: 0,
        results: [],
        status: 'pending',
        cancelled: false,
        minDelaySec: Math.max(Number(req.body.minDelaySeconds) || 35, 30),
        maxDelaySec: Math.max(Number(req.body.maxDelaySeconds) || 60, 45),
        startedAt: new Date().toISOString()
    };

    saveJobToFile();
    processBulkJob();

    return res.status(202).json({ status: 'accepted', job_id: bulkJob.id, suivi: 'GET /bulk-status' });
});

app.get('/bulk-status', (req, res) => {
    if (!bulkJob) return res.json({ status: 'idle' });
    return res.json({
        status: bulkJob.status,
        total: bulkJob.items.length,
        envoyes: bulkJob.sentCount,
        echecs: bulkJob.failedCount,
        restants: bulkJob.items.length - (bulkJob.sentCount + bulkJob.failedCount)
    });
});

app.post('/bulk-cancel', (req, res) => {
    if (!bulkJob) return res.status(400).json({ error: 'Aucun job à annuler.' });
    bulkJob.cancelled = true;
    saveJobToFile();
    return res.json({ status: 'success', message: 'Annulation enregistrée.' });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur prêt sur le port ${PORT}`));
