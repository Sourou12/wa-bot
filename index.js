/**
 * WHATSAPP BOT v3.1 - RENDER FREE OPTIMIZED
 * Version ANTI-BOUCLE INFINIE
 * 
 * Corrections :
 * - Protection anti-double connexion
 * - Gestion propre des redémarrages Render
 * - Rate limiting intégré
 */

const { 
    default: makeWASocket, 
    initAuthCreds, 
    BufferJSON, 
    DisconnectReason,
    proto,
    makeCacheableSignalKeyStore,
    useMultiFileAuthState
} = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const express = require('express');
const mongoose = require('mongoose');
const pino = require('pino');

const app = express();
app.use(express.json({ limit: '5mb' }));

// ==================== MIDDLEWARES ====================
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// ==================== CONFIGURATION ====================
const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI;

// ==================== VARIABLES GLOBALES ====================
let sock = null;
let isReady = false;
let connectionOpenCount = 0;
let isBotStarting = false; // ⭐ PROTECTION ANTI-BOUCLE
let reconnectTimeout = null;

// ==================== MODÈLE MONGODB ====================
const AuthSchema = new mongoose.Schema({
    _id: { type: String, required: true },
    data: { type: String, required: true }
});
const AuthModel = mongoose.models.AuthState || mongoose.model('AuthState', AuthSchema);

// Modèle BulkJob pour persistance
const BulkJobSchema = new mongoose.Schema({
    jobId: { type: String, unique: true },
    items: [{ number: String, message: String }],
    currentIndex: { type: Number, default: 0 },
    sentCount: { type: Number, default: 0 },
    failedCount: { type: Number, default: 0 },
    results: [{
        number: String,
        jid: String,
        status: String,
        id_message: String,
        error: String,
        timestamp: Date
    }],
    status: { 
        type: String, 
        enum: ['pending', 'running', 'completed', 'cancelled', 'paused_daily_limit', 'failed'],
        default: 'pending'
    },
    cancelled: { type: Boolean, default: false },
    config: {
        minDelaySec: Number,
        maxDelaySec: Number,
        batchSize: Number,
        batchPauseMinutes: Number,
        dailyLimit: Number
    },
    sentToday: { type: Number, default: 0 },
    currentDay: String,
    startedAt: Date,
    finishedAt: Date,
    nextSendAt: Date,
    createdAt: { type: Date, default: Date.now }
}, { collection: 'bulkjobs' });

const BulkJobModel = mongoose.models.BulkJob || mongoose.model('BulkJob', BulkJobSchema);
let bulkJob = null;
let isProcessing = false;

// ==================== AUTH MONGODB ====================
async function useMongoDBAuthState() {
    console.log('📦 Initialisation auth MongoDB...');
    
    const readData = async (id) => {
        try {
            const doc = await AuthModel.findById(id).lean().maxTimeMS(5000);
            if (!doc) return undefined;
            return JSON.parse(doc.data, BufferJSON.reviver);
        } catch (err) {
            console.error(`❌ Erreur lecture Mongo (${id}):`, err.message);
            return undefined;
        }
    };

    const writeData = async (id, data) => {
        try {
            if (data === undefined || data === null) {
                await AuthModel.findByIdAndDelete(id);
            } else {
                const value = JSON.stringify(data, BufferJSON.replacer);
                await AuthModel.findByIdAndUpdate(id, { data: value }, { upsert: true, new: true });
            }
        } catch (err) {
            console.error(`❌ Erreur écriture Mongo (${id}):`, err.message);
        }
    };

    let creds = await readData('creds');
    if (!creds) {
        console.log('🆕 Nouvelle session - Génération creds initiaux...');
        creds = initAuthCreds();
    } else {
        console.log('✅ Session existante chargée depuis MongoDB');
    }

    const saveCreds = async () => await writeData('creds', creds);

    const keys = {
        get: async (type, ids) => {
            const data = {};
            await Promise.allSettled(ids.map(async (id) => {
                try {
                    let value = await readData(`${type}-${id}`);
                    if (type === 'app-state-sync-key' && value && typeof value === 'object') {
                        try { value = proto.Message.AppStateSyncKeyData.fromObject(value); } catch(e) {}
                    }
                    data[id] = value;
                } catch (error) {
                    data[id] = undefined;
                }
            }));
            return data;
        },
        
        set: async (data) => {
            const tasks = [];
            for (const category in data) {
                for (const id in data[category]) {
                    const value = data[category][id];
                    const key = `${category}-${id}`;
                    tasks.push(value ? writeData(key, value) : AuthModel.findByIdAndDelete(key));
                }
            }
            await Promise.allSettled(tasks);
        }
    };

    return { state: { creds, keys }, saveCreds };
}

// ==================== UTILITAIRES ====================
function formatNumber(rawNumber) {
    let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');
    if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) cleanNumber = '229' + cleanNumber.slice(2);
    else if (cleanNumber.length === 8) cleanNumber = '229' + cleanNumber;
    cleanNumber = cleanNumber.replace(/^\+/, '');
    return `${cleanNumber}@s.whatsapp.net`;
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function randomDelayMs(minSeconds, maxSeconds) {
    const min = Math.ceil(minSeconds * 1000);
    const max = Math.floor(maxSeconds * 1000);
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function todayStr() {
    return new Date().toISOString().slice(0, 10);
}

// ==================== CONFIG RATE LIMITING ====================
const RATE_CONFIG = {
    MIN_DELAY_SEC: 8,
    MAX_DELAY_SEC: 20,
    BATCH_SIZE: 10,
    BATCH_PAUSE_MINUTES: 3,
    LONG_BREAK_EVERY: 40,
    LONG_BREAK_MIN_MINUTES: 8,
    LONG_BREAK_MAX_MINUTES: 15,
    DEFAULT_DAILY_LIMIT: 500,
    MAX_RETRIES: 3
};

// ==================== CONNEXION WHATSAPP (AVEC PROTECTION) ====================
async function connectWhatsApp() {
    // ⭐ PROTECTION ANTI-BOUCLE : Ne pas lancer si déjà en cours
    if (isBotStarting) {
        console.log('⚠️ Connexion déjà en cours, skip...');
        return null;
    }
    
    isBotStarting = true;
    
    try {
        console.log('\n' + '='.repeat(50));
        console.log('🔐 CONNEXION WHATSAPP...');
        console.log('='.repeat(50) + '\n');

        // Connexion MongoDB
        console.log('🗄️ Connexion MongoDB Atlas...');
        await mongoose.connect(MONGO_URI, {
            serverSelectionTimeoutMS: 10000,
            socketTimeoutMS: 45000,
            maxPoolSize: 10
        });
        console.log('✅ MongoDB connecté !\n');

        // Préparer auth
        const { state, saveCreds } = await useMongoDBAuthState();

        // ⭐ Fermer l'ancienne connexion si elle existe
        if (sock) {
            console.log('🔄 Fermeture ancienne connexion...');
            try { sock.end(); } catch(e) {}
            sock = null;
            isReady = false;
            await sleep(2000); // Attendre fermeture complète
        }

        console.log('📱 Création socket WhatsApp...');
        
        sock = makeWASocket({
            auth: state,
            printQRInTerminal: true,
            syncFullHistory: false,
            shouldSyncHistoryMessage: () => false,
            browser: ["Ecole Marie Auxiliatrice", "Chrome", "6.0"],
            keepAliveIntervalMs: 30000,
            connectTimeoutMs: 60000,
            logger: pino({ level: 'warn' }),
            markOnlineOnConnect: false
        });

        // Gestion événements
        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;

            if (qr) {
                isReady = false;
                console.log('\n📸 QR CODE GÉNÉRÉ - Scannez avec WhatsApp MESSENGER (VERT) !\n');
                qrcode.generate(qr, { small: true });
            }

            if (connection === 'close') {
                isReady = false;
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                
                console.log(`❌ Connexion fermée (Code: ${statusCode})`);
                
                // ⭐ NE PAS relancer immédiatement si conflit
                if (statusCode === 440 || statusCode === DisconnectReason.restartRequired) {
                    console.log('⏳ Attente 30 secondes avant reconnexion (anti-boucle)...');
                    isBotStarting = false; // Reset le flag
                    
                    // Annuler tout timeout précédent
                    if (reconnectTimeout) clearTimeout(reconnectTimeout);
                    
                    // Nouveau timeout avec délai plus long
                    reconnectTimeout = setTimeout(async () => {
                        console.log('🔄 Tentative de reconnexion programmée...');
                        await connectWhatsApp();
                    }, 30000); // 30 secondes minimum !
                    
                } else if (statusCode !== DisconnectReason.loggedOut) {
                    isBotStarting = false;
                    reconnectTimeout = setTimeout(() => connectWhatsApp(), 5000);
                } else {
                    console.log('🔒 Session expirée. Faites /reset-auth');
                    isBotStarting = false;
                }
            }

            if (connection === 'open') {
                isReady = true;
                connectionOpenCount++;
                isBotStarting = false; // Reset flag succès
                
                console.log('\n' + '✅'.repeat(25));
                console.log(`✅ CONNECTÉ ! Connexion #${connectionOpenCount}`);
                console.log('✅'.repeat(25) + '\n');

                // Reprendre job si en attente
                if (bulkJob && ['pending', 'paused_daily_limit'].includes(bulkJob.status)) {
                    console.log('📤 Reprise automatique du job...');
                    setTimeout(() => processBulkJob(), 3000);
                }
            }
        });

        sock.ev.on('error', (error) => {
            const msg = error?.message || '';
            if (msg.includes('stream') || msg.includes('conflict')) {
                console.warn('⚠️ Erreur stream (normale sur Render)');
            } else {
                console.error('❌ Erreur socket:', msg.substring(0, 100));
            }
        });

        return sock;

    } catch (err) {
        console.error('💥 Erreur connexion:', err.message);
        isBotStarting = false;
        
        // Relancer avec délai progressif
        const delay = Math.min(10000, 5000); // Max 10 secondes
        setTimeout(() => connectWhatsApp(), delay);
        
        return null;
    }
}

// ==================== PROCESSING BULK JOB ====================
async function processBulkJob() {
    if (!bulkJob || isProcessing) return;
    
    isProcessing = true;
    bulkJob.status = 'running';
    
    console.log('\n' + '🚀'.repeat(25));
    console.log(' DÉMARRAGE ENVOI BULK');
    console.log('🚀'.repeat(25) + `\n`);

    try {
        for (let i = bulkJob.currentIndex; i < bulkJob.items.length; i++) {
            if (bulkJob.cancelled) {
                bulkJob.status = 'cancelled';
                break;
            }

            while (!isReady && !bulkJob.cancelled) {
                console.log('⏳ Attente connexion WhatsApp...');
                await sleep(15000);
            }
            
            if (todayStr() !== bulkJob.currentDay) {
                bulkJob.currentDay = todayStr();
                bulkJob.sentToday = 0;
            }

            if (bulkJob.sentToday >= (bulkJob.config?.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT)) {
                console.log('⏸️ Limite quotidienne atteinte. Pause...');
                bulkJob.status = 'paused_daily_limit';
                await sleep(15 * 60 * 1000);
                if (!bulkJob.cancelled) {
                    bulkJob.sentToday = 0;
                    bulkJob.status = 'running';
                }
            }

            const item = bulkJob.items[i];
            bulkJob.currentIndex = i;

            try {
                const jid = formatNumber(item.number);
                
                // Simulation humaine
                try {
                    await sock.sendPresenceUpdate('composing', jid);
                    await sleep(1500 + Math.floor(Math.random() * 2500));
                    await sock.sendPresenceUpdate('paused', jid);
                } catch(e) {}

                console.log(`📤 [${i+1}/${bulkJob.items.length}] → ${item.number}`);
                const result = await sock.sendMessage(jid, { text: item.message });
                
                bulkJob.results.push({ number: item.number, jid, status: 'success', id_message: result?.key?.id, timestamp: new Date() });
                bulkJob.sentCount++;
                bulkJob.sentToday++;

            } catch (error) {
                bulkJob.results.push({ number: item.number, status: 'error', error: error.message, timestamp: new Date() });
                bulkJob.failedCount++;
            }

            // Délai entre messages
            if (i < bulkJob.items.length - 1) {
                const msgsSinceStart = i - bulkJob.startIndex + 1;
                let delayMs;
                
                if (msgsSinceStart > 0 && msgsSinceStart % RATE_CONFIG.LONG_BREAK_EVERY === 0) {
                    delayMs = (RATE_CONFIG.LONG_BREAK_MIN_MINUTES + Math.random() * (RATE_CONFIG.LONG_BREAK_MAX_MINUTES - RATE_CONFIG.LONG_BREAK_MIN_MINUTES)) * 60000;
                } else if (bulkJob.config?.batchSize && msgsSinceStart % bulkJob.config.batchSize === 0) {
                    delayMs = (bulkJob.config.batchPauseMinutes || RATE_CONFIG.BATCH_PAUSE_MINUTES) * 60000;
                } else {
                    delayMs = randomDelayMs(RATE_CONFIG.MIN_DELAY_SEC, RATE_CONFIG.MAX_DELAY_SEC);
                }
                
                await sleep(delayMs);
            }
        }

        bulkJob.status = 'completed';
        bulkJob.finishedAt = new Date();
        console.log('\n✅ ENVOI TERMINÉ !');

    } catch (error) {
        console.error('💥 Erreur processing:', error);
        bulkJob.status = 'failed';
    } finally {
        isProcessing = false;
    }
}

// ==================== ROUTES API ====================

// Health check
app.get('/ping', (req, res) => res.status(200).send('pong'));

app.get('/health', (req, res) => {
    res.status(isReady ? 200 : 503).json({
        status: isReady ? 'healthy' : 'degraded',
        whatsapp: { connected: isReady, connections: connectionOpenCount },
        system: { uptime: process.uptime(), memory: process.memoryUsage(), nodeVersion: process.version },
        job: bulkJob ? { id: bulkJob.jobId, status: bulkJob.status, sent: bulkJob.sentCount } : null,
        timestamp: new Date().toISOString()
    });
});

app.get('/', (req, res) => {
    res.json({
        service: 'WhatsApp Bot v3.1',
        status: isReady ? 'connected' : 'waiting_qr',
        version: '3.1.0 (Anti-Boucle)',
        endpoints: {
            health: '/health',
            send: 'POST /send-message',
            sendBulk: 'POST /send-bulk-messages',
            status: 'GET /bulk-status',
            cancel: 'POST /bulk-cancel',
            resetAuth: 'GET /reset-auth'
        }
    });
});

// Envoi simple
app.post('/send-message', async (req, res) => {
    if (!isReady) return res.status(503).json({ error: 'Bot non connecté' });
    
    const rawNumber = req.body.number || req.body.phone;
    const message = req.body.message || req.body.text;
    
    if (!rawNumber || !message) return res.status(400).json({ error: 'Champs requis: number + message' });
    
    try {
        const jid = formatNumber(rawNumber);
        const result = await sock.sendMessage(jid, { text: message });
        res.json({ success: true, jid, id: result?.key?.id });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Envoi bulk
app.post('/send-bulk-messages', async (req, res) => {
    if (!isReady) return res.status(503).json({ error: 'Bot non connecté' });
    if (bulkJob && ['running', 'pending'].includes(bulkJob.status)) return res.status(409).json({ error: 'Job déjà en cours' });
    
    const messages = req.body.messages;
    if (!Array.isArray(messages) || messages.length === 0) return res.status(400).json({ error: 'Messages requis' });
    
    bulkJob = {
        jobId: Date.now().toString(),
        items: messages.map(m => ({ number: m.number || m.phone, message: m.message || m.text })),
        currentIndex: 0, startIndex: 0, sentCount: 0, failedCount: 0,
        results: [], status: 'pending', cancelled: false,
        config: {
            dailyLimit: req.body.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT,
            batchSize: req.body.batchSize || RATE_CONFIG.BATCH_SIZE,
            batchPauseMinutes: Math.max(req.body.batchPauseMinutes || RATE_CONFIG.BATCH_PAUSE_MINUTES, 3),
            minDelaySec: Math.max(req.body.minDelaySeconds || RATE_CONFIG.MIN_DELAY_SEC, 8),
            maxDelaySec: req.body.maxDelaySeconds || RATE_CONFIG.MAX_DELAY_SEC
        },
        sentToday: 0, currentDay: todayStr(),
        startedAt: new Date()
    };
    
    // Sauvegarder dans MongoDB
    try { await new BulkJobModel(bulkJob).save(); } catch(e) {}
    
    setImmediate(processBulkJob);
    
    res.status(202).json({
        success: true, jobId: bulkJob.jobId,
        totalMessages: bulkJob.items.length,
        estimatedTime: `${Math.round(bulkJob.items.length * 15 / 60)} minutes`
    });
});

app.get('/bulk-status', async (req, res) => {
    if (!bulkJob) return res.json({ status: 'idle' });
    res.json({
        ...bulkJob,
        progress: { current: bulkJob.currentIndex+1, total: bulkJob.items.length, percent: ((bulkJob.currentIndex+1)/bulkJob.items.length*100).toFixed(1) }
    });
});

app.post('/bulk-cancel', (req, res) => {
    if (!bulkJob || !['running', 'pending', 'paused_daily_limit'].includes(bulkJob.status)) return res.status(400).json({ error: 'Pas de job actif' });
    bulkJob.cancelled = true;
    res.json({ success: true, message: 'Annulation demandée' });
});

// Reset auth
app.get('/reset-auth', async (req, res) => {
    try {
        console.log('\n🗑️ RESET AUTH DEMANDÉ\n');
        
        if (sock) { try { sock.end(); } catch(e) {} sock = null; isReady = false; }
        
        await AuthModel.deleteMany({});
        isBotStarting = false;
        connectionOpenCount = 0;
        
        res.json({
            success: true,
            message: '✅ Auth supprimée ! Reconnexion dans 5 secondes...',
            instructions: [
                '1. Attendez 10-15 secondes',
                '2. Allez sur /health',
                '3. Regardez les LOGS RENDER pour le QR code',
                '4. Scannez avec WHATSAPP MESSENGER (VERT)'
            ]
        });
        
        setTimeout(() => connectWhatsApp(), 5000);
        
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// Check auth
app.get('/check-auth', async (req, res) => {
    try {
        const count = await AuthModel.countDocuments();
        res.json({ mongodb_connected: mongoose.connection.readyState === 1, docs_count: count, healthy: count >= 3 });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ==================== DÉMARRAGE SERVEUR ====================
app.listen(PORT, async () => {
    console.log(`
╔═══════════════════════════════════════════╗
║   🤖 WHATSAPP BOT v3.1 - ANTI-BOUCLE      ║
║   Serveur: http://localhost:${PORT}              ║
║   Mode: Render Free Optimized                 ║
╚═══════════════════════════════════════════╝
    `);

    // Connexion MongoDB
    try {
        await mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 10000 });
        console.log('✅ MongoDB connecté au démarrage');
    } catch (e) {
        console.error('❌ Erreur MongoDB:', e.message);
    }

    // ⭐ Démarrage DU BOT avec délai de 10 secondes
    // (laisser le temps à Render de stabiliser)
    console.log('⏳ Démarrage du bot dans 10 secondes...');
    setTimeout(() => {
        connectWhatsApp();
    }, 10000); // 10 secondes de délai !
});

// ==================== GRACEFUL SHUTDOWN ====================
process.on('SIGTERM', async () => {
    console.log('\n🛑 Arrêt graceful...');
    if (reconnectTimeout) clearTimeout(reconnectTimeout);
    if (sock) { try { sock.end(); } catch(e) {} }
    await mongoose.connection.close();
    process.exit(0);
});
