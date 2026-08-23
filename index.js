const { default: makeWASocket, initAuthCreds, BufferJSON, DisconnectReason, proto } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const express = require('express');
const mongoose = require('mongoose');

const app = express();
app.use(express.json({ limit: '5mb' }));

  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  next();
});
const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI;

let sock;
let isReady = false;
let connectionOpenCount = 0;

// Modèle MongoDB pour la session Baileys
const AuthSchema = new mongoose.Schema({
  _id: { type: String, required: true },
  data: { type: String, required: true }
});
const AuthModel = mongoose.models.AuthState || mongoose.model('AuthState', AuthSchema);

async function useMongoDBAuthState() {
  const readData = async (id) => {
    try {
      const doc = await AuthModel.findById(id);
      if (!doc) return null;
      return JSON.parse(doc.data, BufferJSON.reviver);
    } catch {
      return null;
    }
  };

  const writeData = async (id, data) => {
    try {
      const value = JSON.stringify(data, BufferJSON.replacer);
      await AuthModel.findByIdAndUpdate(id, { data: value }, { upsert: true });
    } catch (err) {
      console.error(`Erreur écriture Mongo (${id}):`, err);
    }
  };

  const removeData = async (id) => {
    try {
      await AuthModel.findByIdAndDelete(id);
    } catch (err) {
      console.error(`Erreur suppression Mongo (${id}):`, err);
    }
  };

  const creds = (await readData('creds')) || initAuthCreds();

  return {
    state: {
      creds,
      keys: {
        get: async (type, ids) => {
          const data = {};
          await Promise.all(
            ids.map(async (id) => {
              let value = await readData(`${type}-${id}`);
              if (type === 'app-state-sync-key' && value) {
                value = proto.Message.AppStateSyncKeyData.fromObject(value);
              }
              data[id] = value;
            })
          );
          return data;
        },
        set: async (data) => {
          const tasks = [];
          for (const category in data) {
            for (const id in data[category]) {
              const value = data[category][id];
              const key = `${category}-${id}`;
              tasks.push(value ? writeData(key, value) : removeData(key));
            }
          }
          await Promise.all(tasks);
        }
      }
    },
    saveCreds: () => writeData('creds', creds)
  };
}

function formatNumber(rawNumber) {
    let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');
    if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) {
        cleanNumber = '229' + cleanNumber;
    } else if (cleanNumber.length === 8) {
        cleanNumber = '229' + cleanNumber;
    }
    return `${cleanNumber}@s.whatsapp.net`;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function randomDelayMs(minSeconds, maxSeconds) {
    const min = Math.ceil(minSeconds);
    const max = Math.floor(maxSeconds);
    const seconds = Math.floor(Math.random() * (max - min + 1)) + min;
    return seconds * 1000;
}

// Initialisation Bot
async function startBot() {
    try {
        console.log("Connexion à MongoDB Atlas...");
        await mongoose.connect(MONGO_URI);

        const { state, saveCreds } = await useMongoDBAuthState();

        sock = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            syncFullHistory: false,
            browser: ["Ecole Marie Auxiliatrice", "Chrome", "1.0.0"],
            keepAliveIntervalMs: 30000,
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: undefined
        });

        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', (update) => {
            const { connection, lastDisconnect, qr } = update;

            if (qr) {
                isReady = false;
                console.log('\n===========================================');
                console.log('--- SCANNEZ CE QR CODE DANS LES LOGS RENDER ---');
                console.log('===========================================\n');
                qrcode.generate(qr, { small: true });
            }

            if (connection === 'close') {
                isReady = false;
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                console.log(`Connexion fermée (Code: ${statusCode}). Reconnexion :`, shouldReconnect);
                if (shouldReconnect) {
                    setTimeout(startBot, 5000);
                }
            } else if (connection === 'open') {
                isReady = true;
                connectionOpenCount++;
                console.log(`✅ Connecté à WhatsApp avec succès via Baileys ! (connexion n°${connectionOpenCount})`);
            }
        });

    } catch (err) {
        console.error('Erreur d\'initialisation du bot :', err);
        setTimeout(startBot, 10000);
    }
}

// Routes Express
app.get('/ping', (req, res) => res.status(200).send('pong'));

app.get('/', (req, res) => {
    res.json({
        status: isReady ? 'connected' : 'waiting_qr',
        message: isReady ? 'Le bot WhatsApp est prêt !' : 'En attente du scan du QR Code...'
    });
});

app.post('/send-message', async (req, res) => {
    if (!isReady) {
        return res.status(503).json({ status: 'error', error: 'Le client WhatsApp n\'est pas encore connecté.' });
    }

    const rawNumber = req.body.number || req.body.phone;
    const message = req.body.message || req.body.text;

    if (!rawNumber || !message) {
        return res.status(400).json({ status: 'error', error: 'Les champs "number" et "message" sont requis.' });
    }

    try {
        const recipientJid = formatNumber(rawNumber);
        const result = await sock.sendMessage(recipientJid, { text: message });

        return res.json({
            status: 'success',
            message: 'Message envoyé avec succès !',
            jid_destinataire: recipientJid,
            id_message: result?.key?.id
        });
    } catch (error) {
        console.error('Erreur lors de l\'envoi :', error);
        return res.status(500).json({ status: 'error', error: error.message });
    }
});

// Logique Bulk Send
const DEFAULT_MIN_DELAY_SEC = 60;
const DEFAULT_MAX_DELAY_SEC = 120;
const MIN_ALLOWED_DELAY_SEC = 45;
const DEFAULT_BATCH_SIZE = 10;
const DEFAULT_BATCH_PAUSE_MINUTES = 3;
const MIN_ALLOWED_BATCH_PAUSE_MINUTES = 3;
const WITHIN_BATCH_MIN_DELAY_SEC = 8;
const WITHIN_BATCH_MAX_DELAY_SEC = 20;
const LONG_BREAK_EVERY = 40;
const LONG_BREAK_MIN_MINUTES = 8;
const LONG_BREAK_MAX_MINUTES = 15;
const DEFAULT_DAILY_LIMIT = 500;

let bulkJob = null;

function todayStr() {
    return new Date().toISOString().slice(0, 10);
}

async function processBulkJob() {
    if (!bulkJob) return;

    bulkJob.status = 'running';

    for (let i = bulkJob.currentIndex; i < bulkJob.items.length; i++) {
        if (bulkJob.cancelled) {
            bulkJob.status = 'cancelled';
            return;
        }

        while (!isReady && !bulkJob.cancelled) {
            await sleep(10000);
        }
        if (bulkJob.cancelled) {
            bulkJob.status = 'cancelled';
            return;
        }

        if (todayStr() !== bulkJob.currentDay) {
            bulkJob.currentDay = todayStr();
            bulkJob.sentToday = 0;
        }
        if (bulkJob.sentToday >= bulkJob.dailyLimit) {
            bulkJob.status = 'paused_daily_limit';
            while (todayStr() === bulkJob.currentDay && !bulkJob.cancelled) {
                await sleep(15 * 60 * 1000);
            }
            if (bulkJob.cancelled) {
                bulkJob.status = 'cancelled';
                return;
            }
            bulkJob.currentDay = todayStr();
            bulkJob.sentToday = 0;
            bulkJob.status = 'running';
        }

        const item = bulkJob.items[i];
        bulkJob.currentIndex = i;

        try {
            const recipientJid = formatNumber(item.number);

            try {
                await sock.sendPresenceUpdate('composing', recipientJid);
                await sleep(1500 + Math.floor(Math.random() * 2500));
                await sock.sendPresenceUpdate('paused', recipientJid);
            } catch (e) {}

            const result = await sock.sendMessage(recipientJid, { text: item.message });

            bulkJob.results.push({
                number: item.number,
                jid: recipientJid,
                status: 'success',
                id_message: result?.key?.id
            });
            bulkJob.sentCount++;
            bulkJob.sentToday++;

        } catch (error) {
            bulkJob.results.push({
                number: item.number,
                status: 'error',
                error: error.message
            });
            bulkJob.failedCount++;
        }

        const isLastMessage = i === bulkJob.items.length - 1;
        if (!isLastMessage) {
            const messagesSent = i - bulkJob.startIndex + 1;
            const dueForLongBreak = messagesSent > 0 && messagesSent % LONG_BREAK_EVERY === 0;
            const dueForBatchPause = !dueForLongBreak && bulkJob.batchSize > 0 && messagesSent % bulkJob.batchSize === 0;

            let delayMs;
            if (dueForLongBreak) {
                const minutes = LONG_BREAK_MIN_MINUTES + Math.random() * (LONG_BREAK_MAX_MINUTES - LONG_BREAK_MIN_MINUTES);
                delayMs = Math.round(minutes * 60 * 1000);
            } else if (dueForBatchPause) {
                delayMs = Math.round(bulkJob.batchPauseMinutes * 60 * 1000);
            } else {
                delayMs = randomDelayMs(WITHIN_BATCH_MIN_DELAY_SEC, WITHIN_BATCH_MAX_DELAY_SEC);
            }

            bulkJob.nextSendAt = Date.now() + delayMs;
            await sleep(delayMs);
        }
    }

    bulkJob.status = 'completed';
    bulkJob.finishedAt = new Date().toISOString();
}

app.post('/send-bulk-messages', (req, res) => {
    if (!isReady) {
        return res.status(503).json({ status: 'error', error: 'Le client WhatsApp n\'est pas encore connecté.' });
    }

    if (bulkJob && (bulkJob.status === 'running' || bulkJob.status === 'pending')) {
        return res.status(409).json({ status: 'error', error: 'Un envoi en masse est déjà en cours.' });
    }

    const messages = req.body.messages;
    const minDelaySeconds = Number(req.body.minDelaySeconds) || DEFAULT_MIN_DELAY_SEC;
    const maxDelaySeconds = Number(req.body.maxDelaySeconds) || DEFAULT_MAX_DELAY_SEC;
    const dailyLimit = Number(req.body.dailyLimit) || DEFAULT_DAILY_LIMIT;
    const batchSize = Number(req.body.batchSize) || DEFAULT_BATCH_SIZE;
    const batchPauseMinutes = Number(req.body.batchPauseMinutes) || DEFAULT_BATCH_PAUSE_MINUTES;

    if (!Array.isArray(messages) || messages.length === 0) {
        return res.status(400).json({ status: 'error', error: 'Le champ "messages" doit être un tableau non vide.' });
    }

    if (minDelaySeconds < MIN_ALLOWED_DELAY_SEC || batchPauseMinutes < MIN_ALLOWED_BATCH_PAUSE_MINUTES) {
        return res.status(400).json({ status: 'error', error: 'Délais imposés non respectés.' });
    }

    bulkJob = {
        id: Date.now().toString(),
        items: messages.map(m => ({ number: m.number || m.phone, message: m.message || m.text })),
        currentIndex: 0,
        startIndex: 0,
        sentCount: 0,
        failedCount: 0,
        results: [],
        status: 'pending',
        cancelled: false,
        minDelaySec: minDelaySeconds,
        maxDelaySec: maxDelaySeconds,
        batchSize: batchSize,
        batchPauseMinutes: batchPauseMinutes,
        dailyLimit: dailyLimit,
        sentToday: 0,
        currentDay: todayStr(),
        startedAt: new Date().toISOString(),
        finishedAt: null,
        nextSendAt: null
    };

    processBulkJob();

    return res.status(202).json({
        status: 'accepted',
        message: `Envoi en masse lancé pour ${bulkJob.items.length} messages.`,
        job_id: bulkJob.id
    });
});

app.get('/bulk-status', (req, res) => {
    if (!bulkJob) return res.json({ status: 'idle', message: 'Aucun envoi en masse lancé.' });
    return res.json(bulkJob);
});

app.post('/bulk-cancel', (req, res) => {
    if (!bulkJob || (bulkJob.status !== 'running' && bulkJob.status !== 'pending')) {
        return res.status(400).json({ status: 'error', error: 'Aucun envoi actif.' });
    }
    bulkJob.cancelled = true;
    return res.json({ status: 'success', message: 'Annulation demandée.' });
});

app.get('/bulk-results', (req, res) => {
    if (!bulkJob) return res.json({ status: 'idle', results: [] });
    return res.json({ job_id: bulkJob.id, status: bulkJob.status, results: bulkJob.results });
});

app.listen(PORT, () => {
    console.log(`Serveur Web prêt sur le port ${PORT}`);
    startBot();
});
