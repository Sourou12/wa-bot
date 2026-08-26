/**
 * WHATSAPP BOT v3.2 - RENDER OPTIMIZED ULTIMATE
 * Version ANTI-TIMEOUT + ANTI-BOUCLE INFINIE
 * 
 * Corrections v3.2 :
 * - ✅ Gestion Timeout 408 (fetchProps/init queries)
 * - ✅ Timeouts progressifs adaptatifs
 * - ✅ Retry intelligent avec backoff exponentiel
 * - ✅ Protection anti-double connexion renforcée
 * - ✅ Gestion propre des redémarrages Render
 * - ✅ Rate limiting intégré optimisé
 * - ✅ Health check amélioré
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
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(200);
    next();
});

// ==================== CONFIGURATION ====================
const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI;

// ⭐ CONFIGURATION TIMEOUTS POUR RENDER
const TIMEOUT_CONFIG = {
    BASE_CONNECT_TIMEOUT: 120000,      // 120 secondes de base
    BASE_QUERY_TIMEOUT: 120000,        // 120 secondes pour les queries
    MAX_TIMEOUT: 180000,               // 3 minutes max
    KEEP_ALIVE_INTERVAL: 25000,        // 25 secondes
    RETRY_DELAY_BASE: 10000,           // 10 secondes base pour retry
    RETRY_DELAY_MAX: 60000,            // 60 secondes max
    MAX_RETRIES: 5                     // Max tentatives consécutives
};

// ==================== VARIABLES GLOBALES ====================
let sock = null;
let isReady = false;
let connectionOpenCount = 0;
let isBotStarting = false;
let reconnectTimeout = null;
let retryCount = 0; // ⭐ Compteur de retries pour backoff

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
            const doc = await AuthModel.findById(id).lean().maxTimeMS(10000); // ⭐ Augmenté à 10s
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

// ⭐ CALCUL DELAI PROGRESSIF (Backoff Exponentiel)
function getProgressiveDelay() {
    const delay = Math.min(
        TIMEOUT_CONFIG.RETRY_DELAY_BASE * Math.pow(1.5, retryCount),
        TIMEOUT_CONFIG.RETRY_DELAY_MAX
    );
    return Math.round(delay);
}

// ⭐ CALCUL TIMEOUT ADAPTATIF
function getAdaptiveTimeout() {
    const timeout = Math.min(
        TIMEOUT_CONFIG.BASE_CONNECT_TIMEOUT + (retryCount * 15000),
        TIMEOUT_CONFIG.MAX_TIMEOUT
    );
    return Math.round(timeout);
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

// ==================== CONNEXION WHATSAPP (AVEC PROTECTION COMPLÈTE) ====================
async function connectWhatsApp() {
    // ⭐ PROTECTION ANTI-BOUCLE : Ne pas lancer si déjà en cours
    if (isBotStarting) {
        console.log('⚠️ Connexion déjà en cours, skip...');
        return null;
    }
    
    // ⭐ VÉRIFICATION MAX RETRIES
    if (retryCount > TIMEOUT_CONFIG.MAX_RETRIES) {
        console.error(`💥 Max retries atteint (${TIMEOUT_CONFIG.MAX_RETRIES}) - Attente manuelle ou reset`);
        isBotStarting = false;
        
        // Attendre plus longtemps avant de réessayer (5 minutes)
        setTimeout(() => {
            retryCount = 0; // Reset après attente prolongée
            console.log('🔄 Reset retry count - Nouvelle tentative autorisée');
        }, 300000); // 5 minutes
        
        return null;
    }
    
    isBotStarting = true;
    
    try {
        console.log('\n' + '='.repeat(50));
        console.log(`🔐 CONNEXION WHATSAPP...`);
        console.log(`📊 Tentative #${retryCount + 1}/${TIMEOUT_CONFIG.MAX_RETRIES + 1}`);
        console.log(`⏱️ Timeout configuré: ${getAdaptiveTimeout() / 1000}s`);
        console.log('='.repeat(50) + '\n');

        // Connexion MongoDB avec timeout augmenté
        console.log('🗄️ Connexion MongoDB Atlas...');
        await mongoose.connect(MONGO_URI, {
            serverSelectionTimeoutMS: 15000, // ⭐ Augmenté à 15s
            socketTimeoutMS: 60000,
            maxPoolSize: 10,
            bufferCommands: false // ⭐ Éviter le buffering en cas de déconnexion
        });
        console.log('✅ MongoDB connecté !\n');

        // Préparer auth
        const { state, saveCreds } = await useMongoDBAuthState();

        // ⭐ Fermer l'ancienne connexion si elle existe
        if (sock) {
            console.log('🔄 Fermeture ancienne connexion...');
            try { 
                sock.ev.removeAllListeners(); // ⭐ Nettoyer tous les listeners
                sock.end(); 
            } catch(e) { 
                console.log('⚠️ Erreur fermeture socket:', e.message);
            }
            sock = null;
            isReady = false;
            await sleep(3000); // ⭐ Attendre fermeture complète (augmenté)
        }

        console.log('📱 Création socket WhatsApp...');
        
        // ⭐ CONFIGURATION AVEC TIMEOUTS ADAPTATIFS
        const currentTimeout = getAdaptiveTimeout();
        
        sock = makeWASocket({
            auth: state,
            printQRInTerminal: true,
            syncFullHistory: false,
            shouldSyncHistoryMessage: () => false,
            browser: ["Ecole Marie Auxiliatrice", "Chrome", "6.0"],
            
            // ⭐ TIMEOUTS OPTIMISÉS POUR RENDER
            connectTimeoutMs: currentTimeout,
            keepAliveIntervalMs: TIMEOUT_CONFIG.KEEP_ALIVE_INTERVAL,
            queryTimeoutMs: currentTimeout, // ⭐ CRITIQUE : Résout le 408
            
            logger: pino({ level: 'warn' }),
            markOnlineOnConnect: false,
            
            // ⭐ OPTIONS DE RÉSILIENCE
            retryRequestDelayMs: 5000,
            maxMsgRetryCount: 3
        });

        // Gestion événements
        sock.ev.on('creds.update', saveCreds);

        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const errorMessage = lastDisconnect?.error?.message || '';

            if (qr) {
                isReady = false;
                console.log('\n📸 QR CODE GÉNÉRÉ - Scannez avec WhatsApp MESSENGER (VERT) !\n');
                qrcode.generate(qr, { small: true });
            }

            if (connection === 'close') {
                isReady = false;
                
                console.log(`\n❌ Connexion fermée`);
                console.log(`   Code: ${statusCode}`);
                console.log(`   Message: ${errorMessage.substring(0, 100)}\n`);
                
                // ⭐ GESTION SPÉCIFIQUE DU TIMEOUT 408 (fetchProps)
                if (statusCode === 408) {
                    console.log('⚠️ TIMEOUT 408 détecté (init queries/fetchProps)');
                    console.log('   → Normal sur Render (latence réseau élevée)');
                    
                    retryCount++;
                    isBotStarting = false;
                    
                    // Annuler tout timeout précédent
                    if (reconnectTimeout) clearTimeout(reconnectTimeout);
                    
                    const delay = getProgressiveDelay();
                    console.log(`   → Retry #${retryCount} dans ${delay / 1000}s...\n`);
                    
                    reconnectTimeout = setTimeout(async () => {
                        console.log('🔄 Tentative de reconnexion post-timeout...');
                        await connectWhatsApp();
                    }, delay);
                    
                }
                // ⭐ GESTION CONFLIT/RESTART
                else if (statusCode === 440 || statusCode === DisconnectReason.restartRequired) {
                    console.log('⏳ Restart/Conflit détecté');
                    
                    retryCount++;
                    isBotStarting = false;
                    
                    if (reconnectTimeout) clearTimeout(reconnectTimeout);
                    
                    const delay = Math.max(getProgressiveDelay(), 30000); // Min 30s pour conflit
                    console.log(`   → Attente ${delay / 1000}s avant reconnexion...\n`);
                    
                    reconnectTimeout = setTimeout(async () => {
                        console.log('🔄 Tentative de reconnexion post-conflit...');
                        await connectWhatsApp();
                    }, delay);
                    
                }
                else if (statusCode !== DisconnectReason.loggedOut) {
                    // Autres erreurs (network, etc.)
                    console.log('⚠️ Erreur de connexion générique');
                    
                    retryCount = Math.min(retryCount + 1, 2); // Retry modéré
                    isBotStarting = false;
                    
                    if (reconnectTimeout) clearTimeout(reconnectTimeout);
                    
                    const delay = 8000; // Fixe 8s pour erreurs génériques
                    reconnectTimeout = setTimeout(() => connectWhatsApp(), delay);
                    
                } else {
                    // Logout explicite
                    console.log('🔒 Session expirée (logged out). Faites /reset-auth');
                    isBotStarting = false;
                    retryCount = 0; // Reset complet
                }
            }

            if (connection === 'open') {
                isReady = true;
                connectionOpenCount++;
                isBotStarting = false;
                retryCount = 0; // ⭐ RESET SUCCÈS
                
                console.log('\n' + '✅'.repeat(25));
                console.log(`✅ CONNECTÉ ! Connexion #${connectionOpenCount}`);
                console.log(`✅ Socket ID: ${sock.user?.id || 'N/A'}`);
                console.log('✅'.repeat(25) + '\n');

                // Reprendre job si en attente
                if (bulkJob && ['pending', 'paused_daily_limit'].includes(bulkJob.status)) {
                    console.log('📤 Reprise automatique du job...');
                    setTimeout(() => processBulkJob(), 3000);
                }
            }
        });

        // ⭐ GESTION ERREURS AMÉLIORÉE
        sock.ev.on('error', (error) => {
            const msg = error?.message || '';
            const stack = error?.stack || '';
            
            // ⭐ DÉTECTION SPÉCIFIQUE TIMEOUT
            if (msg.includes('Timed Out') || stack.includes('Timed Out')) {
                console.warn('⚠️ [TIMEOUT] Erreur timeout socket (normal sur Render)');
                // Ne pas relancer ici - laisser connection.update gérer la reconnexion
            }
            else if (msg.includes('stream') || msg.includes('conflict')) {
                console.warn('⚠️ [STREAM] Erreur stream (normale sur Render)');
            }
            else if (msg.includes('init queries') || stack.includes('chats.js')) {
                console.warn('⚠️ [INIT] Erreur initialisation queries (timeout 408 probable)');
            }
            else {
                console.error('❌ [ERROR] Erreur socket:', msg.substring(0, 150));
            }
        });

        // ⭐ NETTOYAGE SI ERREUR SYNCHRONE À LA CRÉATION
        sock.ev.on('connection.update').catch((err) => {
            console.error('💥 Erreur critique connection.update:', err.message);
            isBotStarting = false;
        });

        return sock;

    } catch (err) {
        console.error('\n💥 ERREUR CONNEXION:');
        console.error('   Type:', err.constructor.name);
        console.error('   Message:', err.message);
        console.error('');
        
        isBotStarting = false;
        retryCount++;
        
        // ⭐ DÉLAIS PROGRESSIFS EN CAS D'ERREUR
        const delay = getProgressiveDelay();
        console.log(`🔄 Nouvelle tentative dans ${delay / 1000}s (retry #${retryCount})\n`);
        
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
            // ⭐ VÉRIFICATION ANNULATION
            if (bulkJob.cancelled) {
                bulkJob.status = 'cancelled';
                console.log('❌ Job annulé par utilisateur');
                break;
            }

            // ⭐ ATTENTE CONNEXION ACTIVE
            while (!isReady && !bulkJob.cancelled) {
                console.log('⏳ Attente connexion WhatsApp...');
                await sleep(15000);
                
                // Sécurité : vérifier que le socket existe toujours
                if (!sock) {
                    console.log('⚠️ Socket détruit - Attente reconnexion...');
                    continue;
                }
            }
            
            if (bulkJob.cancelled) break;

            // ⭐ GESTION LIMITE QUOTIDIENNE
            if (todayStr() !== bulkJob.currentDay) {
                bulkJob.currentDay = todayStr();
                bulkJob.sentToday = 0;
            }

            if (bulkJob.sentToday >= (bulkJob.config?.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT)) {
                console.log('⏸️ Limite quotidienne atteinte. Pause jusqu\'à demain...');
                bulkJob.status = 'paused_daily_limit';
                
                // Sauvegarder état
                try { 
                    await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
                } catch(e) {}
                
                await sleep(15 * 60 * 1000); // 15 minutes
                
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
                } catch(e) {
                    // Ignorer erreurs presence update
                }

                console.log(`📤 [${i+1}/${bulkJob.items.length}] → ${item.number}`);
                
                // ⭐ ENVOI AVEC TIMEOUT PERSONNALISÉ
                const sendPromise = sock.sendMessage(jid, { text: item.message });
                const timeoutPromise = new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Send timeout')), 30000)
                );
                
                const result = await Promise.race([sendPromise, timeoutPromise]);
                
                bulkJob.results.push({ 
                    number: item.number, 
                    jid, 
                    status: 'success', 
                    id_message: result?.key?.id, 
                    timestamp: new Date() 
                });
                bulkJob.sentCount++;
                bulkJob.sentToday++;

                // ⭐ SAUVEGARDE PériODIQUE (tous les 10 messages)
                if (bulkJob.sentCount % 10 === 0) {
                    try { 
                        await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
                    } catch(e) {}
                }

            } catch (error) {
                const errorMsg = error?.message || 'Unknown error';
                console.error(`   ❌ Échec ${item.number}: ${errorMsg}`);
                
                bulkJob.results.push({ 
                    number: item.number, 
                    status: 'error', 
                    error: errorMsg, 
                    timestamp: new Date() 
                });
                bulkJob.failedCount++;
                
                // Si erreur de connexion, attendre un peu
                if (errorMsg.includes('timeout') || errorMsg.includes('disconnect')) {
                    console.log('   ⏳ Pause 10s suite à erreur connexion...');
                    await sleep(10000);
                }
            }

            // ⭐ DÉLAI ENTRE MESSAGES (Rate Limiting Intelligent)
            if (i < bulkJob.items.length - 1) {
                const msgsSinceStart = i - (bulkJob.startIndex || 0) + 1;
                let delayMs;
                
                // Longue pause tous les X messages
                if (msgsSinceStart > 0 && msgsSinceStart % RATE_CONFIG.LONG_BREAK_EVERY === 0) {
                    const longBreakDuration = (
                        RATE_CONFIG.LONG_BREAK_MIN_MINUTES + 
                        Math.random() * (RATE_CONFIG.LONG_BREAK_MAX_MINUTES - RATE_CONFIG.LONG_BREAK_MIN_MINUTES)
                    ) * 60000;
                    delayMs = longBreakDuration;
                    console.log(`   ☕ Longue pause ${(longBreakDuration/60000).toFixed(1)}min (${msgsSinceStart} messages envoyés)`);
                }
                // Pause batch
                else if (bulkJob.config?.batchSize && msgsSinceStart % bulkJob.config.batchSize === 0) {
                    delayMs = (bulkJob.config.batchPauseMinutes || RATE_CONFIG.BATCH_PAUSE_MINUTES) * 60000;
                    console.log(`   📦 Pause batch ${(delayMs/60000).toFixed(1)}min`);
                }
                // Délai normal aléatoire
                else {
                    delayMs = randomDelayMs(
                        bulkJob.config?.minDelaySec || RATE_CONFIG.MIN_DELAY_SEC,
                        bulkJob.config?.maxDelaySec || RATE_CONFIG.MAX_DELAY_SEC
                    );
                }
                
                await sleep(delayMs);
            }
        }

        // ⭐ FINALISATION
        bulkJob.status = 'completed';
        bulkJob.finishedAt = new Date();
        
        // Sauvegarde finale
        try { 
            await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
        } catch(e) {}
        
        console.log('\n' + '✅'.repeat(25));
        console.log(` ENVOI TERMINÉ !`);
        console.log(` Succès: ${bulkJob.sentCount} | Échecs: ${bulkJob.failedCount}`);
        console.log('✅'.repeat(25) + '\n');

    } catch (error) {
        console.error('\n💥 ERREUR CRITIQUE PROCESSING:', error);
        bulkJob.status = 'failed';
        
        try { 
            await BulkJobModel.findByIdAndUpdate(bulkJob.jobId, bulkJob); 
        } catch(e) {}
        
    } finally {
        isProcessing = false;
    }
}

// ==================== ROUTES API ====================

// Health check simple
app.get('/ping', (req, res) => res.status(200).send('pong'));

// Health check détaillé
app.get('/health', (req, res) => {
    const healthStatus = {
        status: isReady ? 'healthy' : 'degraded',
        whatsapp: { 
            connected: isReady, 
            connections: connectionOpenCount,
            retryCount: retryCount,
            socketExists: !!sock
        },
        system: { 
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            nodeVersion: process.version,
            platform: process.platform
        },
        mongodb: {
            connected: mongoose.connection.readyState === 1,
            state: ['disconnected', 'connected', 'connecting', 'disconnecting'][mongoose.connection.readyState] || 'unknown'
        },
        job: bulkJob ? { 
            id: bulkJob.jobId, 
            status: bulkJob.status, 
            sent: bulkJob.sentCount,
            total: bulkJob.items?.length || 0,
            progress: ((bulkJob.currentIndex + 1) / (bulkJob.items?.length || 1) * 100).toFixed(1) + '%'
        } : null,
        config: {
            currentTimeout: getAdaptiveTimeout(),
            nextRetryDelay: getProgressiveDelay()
        },
        timestamp: new Date().toISOString()
    };
    
    res.status(isReady ? 200 : 503).json(healthStatus);
});

// Route racine - Info API
app.get('/', (req, res) => {
    res.json({
        service: 'WhatsApp Bot v3.2',
        status: isReady ? 'connected' : 'waiting_qr',
        version: '3.2.0 (Render Optimized Ultimate)',
        features: [
            'Anti-timeout 408',
            'Anti-boucle infinie',
            'Retry progressif',
            'Bulk messaging',
            'Rate limiting intelligent',
            'Persistance MongoDB'
        ],
        endpoints: {
            health: 'GET /health',
            ping: 'GET /ping',
            sendMessage: 'POST /send-message',
            sendBulk: 'POST /send-bulk-messages',
            status: 'GET /bulk-status',
            cancel: 'POST /bulk-cancel',
            resetAuth: 'GET /reset-auth',
            checkAuth: 'GET /check-auth'
        },
        documentation: 'Voir /health pour le statut détaillé'
    });
});

// Envoi message simple
app.post('/send-message', async (req, res) => {
    if (!isReady || !sock) {
        return res.status(503).json({ 
            error: 'Bot non connecté', 
            hint: 'Attendez la connexion ou scannez le QR code',
            status: isReady ? 'degraded' : 'offline'
        });
    }
    
    const rawNumber = req.body.number || req.body.phone;
    const message = req.body.message || req.body.text;
    
    if (!rawNumber || !message) {
        return res.status(400).json({ 
            error: 'Champs requis: number + message',
            example: { number: '01XXXXXXXX', message: 'Votre texte ici' }
        });
    }
    
    try {
        const jid = formatNumber(rawNumber);
        console.log(`📨 Envoi simple vers ${rawNumber}`);
        
        const result = await sock.sendMessage(jid, { text: message });
        
        res.json({ 
            success: true, 
            jid, 
            id: result?.key?.id,
            timestamp: new Date().toISOString()
        });
    } catch (error) {
        console.error('❌ Erreur envoi simple:', error.message);
        res.status(500).json({ 
            error: error.message,
            hint: 'Vérifiez le format du numéro'
        });
    }
});

// Envoi bulk messages
app.post('/send-bulk-messages', async (req, res) => {
    if (!isReady || !sock) {
        return res.status(503).json({ 
            error: 'Bot non connecté',
            hint: 'Connectez d\'abord le bot via QR code'
        });
    }
    
    if (bulkJob && ['running', 'pending'].includes(bulkJob.status)) {
        return res.status(409).json({ 
            error: 'Job déjà en cours',
            currentJob: bulkJob.jobId,
            hint: 'Utilisez /bulk-status ou /bulk-cancel d\'abord'
        });
    }
    
    const messages = req.body.messages;
    if (!Array.isArray(messages) || messages.length === 0) {
        return res.status(400).json({ 
            error: 'Messages requis (array)',
            example: { messages: [{ number: '01XX', message: 'Texte' }] }
        });
    }
    
    if (messages.length > 1000) {
        return res.status(400).json({ 
            error: 'Maximum 1000 messages par job',
            received: messages.length
        });
    }
    
    // Création du job
    bulkJob = {
        jobId: Date.now().toString() + '_' + Math.random().toString(36).substr(2, 9),
        items: messages.map(m => ({ 
            number: m.number || m.phone, 
            message: m.message || m.text 
        })).filter(m => m.number && m.message), // Filtrer entrées invalides
        currentIndex: 0, 
        startIndex: 0, 
        sentCount: 0, 
        failedCount: 0,
        results: [], 
        status: 'pending', 
        cancelled: false,
        config: {
            dailyLimit: Math.min(req.body.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT, 1000),
            batchSize: req.body.batchSize || RATE_CONFIG.BATCH_SIZE,
            batchPauseMinutes: Math.max(req.body.batchPauseMinutes || RATE_CONFIG.BATCH_PAUSE_MINUTES, 3),
            minDelaySec: Math.max(req.body.minDelaySeconds || RATE_CONFIG.MIN_DELAY_SEC, 8),
            maxDelaySec: req.body.maxDelaySeconds || RATE_CONFIG.MAX_DELAY_SEC
        },
        sentToday: 0, 
        currentDay: todayStr(),
        startedAt: new Date()
    };
    
    // Validation finale
    if (bulkJob.items.length === 0) {
        return res.status(400).json({ 
            error: 'Aucun message valide après validation',
            hint: 'Chaque message doit avoir number et message'
        });
    }
    
    // Sauvegarder dans MongoDB
    try { 
        await new BulkJobModel(bulkJob).save(); 
        console.log(`💾 Job sauvegardé: ${bulkJob.jobId} (${bulkJob.items.length} messages)`);
    } catch(e) {
        console.error('⚠️ Erreur sauvegarde job:', e.message);
    }
    
    // Démarrage asynchrone
    setImmediate(processBulkJob);
    
    // Estimation temps
    const estimatedMinutes = Math.round(
        bulkJob.items.length * (
            (RATE_CONFIG.MIN_DELAY_SEC + RATE_CONFIG.MAX_DELAY_SEC) / 2 / 60 +
            (RATE_CONFIG.BATCH_PAUSE_MINUTES / RATE_CONFIG.BATCH_SIZE)
        )
    );
    
    res.status(202).json({
        success: true, 
        jobId: bulkJob.jobId,
        totalMessages: bulkJob.items.length,
        estimatedTime: `${estimatedMinutes} minutes`,
        config: bulkJob.config,
        endpoints: {
            status: `/bulk-status?jobId=${bulkJob.jobId}`,
            cancel: '/bulk-cancel'
        },
        warnings: [
            'Respectez les limites WhatsApp (≈500 messages/jour)',
            'Les délais sont aléatoires pour simuler un comportement humain'
        ]
    });
});

// Statut bulk job
app.get('/bulk-status', async (req, res) => {
    if (!bulkJob) {
        return res.json({ 
            status: 'idle', 
            message: 'Aucun job en cours',
            hint: 'POST /send-bulk-messages pour démarrer'
        });
    }
    
    const total = bulkJob.items?.length || 1;
    const current = bulkJob.currentIndex + 1;
    
    res.json({
        jobId: bulkJob.jobId,
        status: bulkJob.status,
        progress: {
            current: current,
            total: total,
            percent: ((current / total) * 100).toFixed(1) + '%',
            remaining: total - current
        },
        stats: {
            sent: bulkJob.sentCount,
            failed: bulkJob.failedCount,
            sentToday: bulkJob.sentToday,
            dailyLimit: bulkJob.config?.dailyLimit || RATE_CONFIG.DEFAULT_DAILY_LIMIT
        },
        timing: {
            startedAt: bulkJob.startedAt,
            runningFor: bulkJob.startedAt ? 
                Math.round((Date.now() - new Date(bulkJob.startedAt).getTime()) / 1000) + 's' : null,
            estimatedRemaining: bulkJob.status === 'running' ?
                `${Math.round((total - current) * 15 / 60)} minutes` : null
        },
        cancelled: bulkJob.cancelled,
        canCancel: ['running', 'pending', 'paused_daily_limit'].includes(bulkJob.status)
    });
});

// Annulation job
app.post('/bulk-cancel', (req, res) => {
    if (!bulkJob) {
        return res.status(400).json({ 
            error: 'Pas de job actif',
            hint: 'Créez un job d\'abord avec POST /send-bulk-messages'
        });
    }
    
    if (!['running', 'pending', 'paused_daily_limit'].includes(bulkJob.status)) {
        return res.status(400).json({ 
            error: `Impossible d'annuler un job en statut: ${bulkJob.status}`,
            currentStatus: bulkJob.status
        });
    }
    
    bulkJob.cancelled = true;
    
    console.log(`🛑 Annulation demandée pour job ${bulkJob.jobId}`);
    
    res.json({ 
        success: true, 
        message: 'Annulation demandée',
        jobId: bulkJob.jobId,
        info: 'Le job s\'arrêtera au prochain message',
        stats: {
            sentBeforeCancel: bulkJob.sentCount,
            remaining: (bulkJob.items?.length || 0) - bulkJob.currentIndex
        }
    });
});

// Reset auth (nouvelle session)
app.get('/reset-auth', async (req, res) => {
    try {
        console.log('\n' + '='.repeat(50));
        console.log('🗑️ RESET AUTH DEMANDÉ');
        console.log('='.repeat(50) + '\n');
        
        // Arrêter job en cours
        if (bulkJob) {
            bulkJob.cancelled = true;
            console.log('📤 Job en cours annulé');
        }
        
        // Fermer socket
        if (sock) { 
            try { 
                sock.ev.removeAllListeners();
                sock.end(); 
            } catch(e) {} 
            sock = null; 
            isReady = false; 
        }
        
        // Supprimer auth MongoDB
        const deleteResult = await AuthModel.deleteMany({});
        console.log(`🗑️ ${deleteResult.deletedCount} documents auth supprimés`);
        
        // Reset variables
        isBotStarting = false;
        connectionOpenCount = 0;
        retryCount = 0;
        
        // Annuler reconnexion en cours
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
            reconnectTimeout = null;
        }
        
        res.json({
            success: true,
            message: '✅ Authentification réinitialisée !',
            actions: [
                '✅ Socket fermé',
                `✅ ${deleteResult.deletedCount} credentials supprimées`,
                '✅ Variables reset'
            ],
            nextSteps: [
                '1. Attendez 10-15 secondes',
                '2. Consultez GET /health pour vérifier le statut',
                '3. Regardez les LOGS RENDER pour le QR code',
                '4. Scannez avec WHATSAPP MESSENGER (application VERTE)',
                '⚠️ Ne pas utiliser WhatsApp Web !'
            ],
            autoReconnect: 'Reconnexion automatique dans 5 secondes...'
        });
        
        // Reconnexion automatique
        setTimeout(() => {
            console.log('🔄 Reconnexion post-reset...');
            connectWhatsApp();
        }, 5000);
        
    } catch (error) {
        console.error('❌ Erreur reset auth:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Vérifier état auth
app.get('/check-auth', async (req, res) => {
    try {
        const count = await AuthModel.countDocuments();
        const hasCreds = await AuthModel.exists({ _id: 'creds' });
        
        res.json({ 
            mongodb_connected: mongoose.connection.readyState === 1,
            docs_count: count,
            hasCredentials: !!hasCreds,
            healthy: count >= 3,
            details: {
                creds: !!hasCreds,
                keys_count: Math.max(0, count - 1)
            },
            recommendations: count < 3 ? 
                ['Session incomplète - Réessayez /reset-auth'] : 
                ['Auth OK - Le bot devrait se connecter automatiquement']
        });
    } catch (e) {
        res.status(500).json({ 
            error: e.message,
            mongodb_connected: false
        });
    }
});

// ==================== GESTION ERREURS GLOBALES ====================
app.use((err, req, res, next) => {
    console.error('💥 Erreur non gérée:', err.stack);
    res.status(500).json({ 
        error: 'Erreur interne serveur',
        message: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
});

// Route 404
app.use((req, res) => {
    res.status(404).json({ 
        error: 'Endpoint non trouvé',
        availableEndpoints: {
            root: 'GET /',
            health: 'GET /health, GET /ping',
            messaging: 'POST /send-message, POST /send-bulk-messages',
            jobs: 'GET /bulk-status, POST /bulk-cancel',
            auth: 'GET /reset-auth, GET /check-auth'
        }
    });
});

// ==================== DÉMARRAGE SERVEUR ====================
app.listen(PORT, async () => {
    console.log(`
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║   🤖 WHATSAPP BOT v3.2 - RENDER ULTIMATE             ║
║   ─────────────────────────────────                   ║
║   Serveur: http://localhost:${PORT.toString().padEnd(4)}                      ║
║   Mode: Render Free Optimized                         ║
║   Features: Anti-Timeout | Anti-Boucle | Retry Smart  ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
    `);

    // Connexion MongoDB initiale
    try {
        await mongoose.connect(MONGO_URI, { 
            serverSelectionTimeoutMS: 15000,
            socketTimeoutMS: 60000
        });
        console.log('✅ MongoDB connecté au démarrage');
    } catch (e) {
        console.error('❌ Erreur MongoDB initiale:', e.message);
        console.log('⏳ Retente au démarrage du bot...');
    }

    // ⭐ DÉMARRAGE DU BOT AVEC DÉLAI ADAPTATIF
    // (laisser le temps à Render de stabiliser le conteneur)
    const startupDelay = 10000; // 10 secondes
    
    console.log(`⏳ Démarrage du bot dans ${startupDelay / 1000} secondes...`);
    console.log('   (Attente stabilisation Render)\n');
    
    setTimeout(() => {
        connectWhatsApp();
    }, startupDelay);
});

// ==================== GRACEFUL SHUTDOWN ====================
process.on('SIGTERM', async () => {
    console.log('\n' + '='.repeat(50));
    console.log('🛑 SIGNAL SIGTERM REÇU - Arrêt graceful...');
    console.log('='.repeat(50) + '\n');
    
    // Annuler reconnexion
    if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
        console.log('⏹️ Timeout reconnexion annulé');
    }
    
    // Annuler job
    if (bulkJob) {
        bulkJob.cancelled = true;
        console.log('📤 Job bulk marqué comme annulé');
    }
    
    // Fermer socket proprement
    if (sock) {
        try {
            sock.ev.removeAllListeners();
            sock.end();
            console.log('📱 Socket fermé proprement');
        } catch(e) {
            console.log('⚠️ Erreur fermeture socket:', e.message);
        }
    }
    
    // Fermer MongoDB
    try {
        await mongoose.connection.close();
        console.log('🗄️ MongoDB déconnecté');
    } catch(e) {}
    
    console.log('✅ Arrêt complété\n');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('\n\n🛑 SIGINT reçu (Ctrl+C)');
    process.exit(0);
});

// Gestion exceptions non capturées
process.on('uncaughtException', (error) => {
    console.error('💥 Uncaught Exception:', error.message);
    // Ne pas quitter immédiatement sur Render
    if (error.message.includes('Timed Out')) {
        console.log('⚠️ Timeout ignoré (géré par reconnexion)');
    } else {
        setTimeout(() => process.exit(1), 5000);
    }
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
});
