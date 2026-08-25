const { 
    default: makeWASocket, 
    initAuthCreds, 
    BufferJSON, 
    DisconnectReason, 
    proto,
    useMultiFileAuthState,
    makeCacheableSignalKeyStore
} = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const express = require('express');
const mongoose = require('mongoose');
const pino = require('pino');

const app = express();
app.use(express.json({ limit: '5mb' }));

// ==================== MIDDLEWARES ====================

// CORS
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
    res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') {
        return res.sendStatus(200);
    }
    next();
});

// ==================== CONFIGURATION ====================

const PORT = process.env.PORT || 10000;
const MONGO_URI = process.env.MONGO_URI;

// Variables globales
let sock;
let isReady = false;
let connectionOpenCount = 0;
let reconnectAttempts = 0;
const MAX_RECONNECT_ATTEMPTS = 10;

// ==================== MODÈLE MONGODB ====================

const AuthSchema = new mongoose.Schema({
    _id: { type: String, required: true },
    data: { type: String, required: true }
});
const AuthModel = mongoose.models.AuthState || mongoose.model('AuthState', AuthSchema);

// Modèle pour persister le bulkJob (survit aux redémarrages Render)
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
        error: String
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
});

const BulkJobModel = mongoose.models.BulkJob || mongoose.model('BulkJob', BulkJobSchema);

let bulkJob = null; // Job en mémoire (cache)

// ==================== AUTH MONGODB (AMÉLIORÉE) ====================

async function useMongoDBAuthState() {
    console.log('📦 Initialisation de l\'auth MongoDB...');

    const readData = async (id) => {
        try {
            const doc = await AuthModel.findById(id).lean().maxTimeMS(5000); // Timeout 5s
            if (!doc) return undefined; // null → undefined pour éviter les bugs
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
                await AuthModel.findByIdAndUpdate(
                    id, 
                    { data: value }, 
                    { upsert: true, new: true }
                );
            }
        } catch (err) {
            console.error(`❌ Erreur écriture Mongo (${id}):`, err.message);
        }
    };

    const removeData = async (id) => {
        try {
            await AuthModel.findByIdAndDelete(id).maxTimeMS(3000);
        } catch (err) {
            console.error(`❌ Erreur suppression Mongo (${id}):`, err.message);
        }
    };

    // Charger les credentials
    let creds = await readData('creds');
    
    if (!creds) {
        console.log('🆕 Nouvelle session - Génération des creds initiaux...');
        creds = initAuthCreds();
    } else {
        console.log('✅ Session existante chargée depuis MongoDB');
    }

    const saveCreds = async () => {
        await writeData('creds', creds);
    };

    // Clés avec gestion d'erreur robuste
    const keys = {
        get: async (type, ids) => {
            const data = {};
            const promises = ids.map(async (id) => {
                try {
                    let value = await readData(`${type}-${id}`);
                    
                    // Conversion spéciale pour app-state-sync-key
                    if (type === 'app-state-sync-key' && value && typeof value === 'object') {
                        try {
                            value = proto.Message.AppStateSyncKeyData.fromObject(value);
                        } catch (e) {
                            console.warn(`⚠️ Conversion proto échouée pour ${type}-${id}:`, e.message);
                        }
                    }
                    
                    data[id] = value;
                } catch (error) {
                    console.error(`❌ Erreur lecture clé ${type}-${id}:`, error.message);
                    data[id] = undefined;
                }
            });
            
            await Promise.allSettled(promises); // Utiliser allSettled pour ne pas tout casser
            return data;
        },
        
        set: async (data) => {
            const tasks = [];
            
            for (const category in data) {
                for (const id in data[category]) {
                    const value = data[category][id];
                    const key = `${category}-${id}`;
                    
                    // Écrire ou supprimer selon la valeur
                    if (value === undefined || value === null) {
                        tasks.push(removeData(key));
                    } else {
                        tasks.push(writeData(key, value));
                    }
                }
            }
            
            // Exécuter toutes les écritures en parallèle avec gestion d'erreur
            const results = await Promise.allSettled(tasks);
            const failed = results.filter(r => r.status === 'rejected');
            
            if (failed.length > 0) {
                console.warn(`⚠️ ${failed.length}/${tasks.length} écritures ont échoué`);
            }
        }
    };

    return {
        state: { creds, keys },
        saveCreds
    };
}

// ==================== UTILITAIRES ====================

function formatNumber(rawNumber) {
    let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');
    
    // Logique formatage numéro Bénin (229)
    if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) {
        cleanNumber = '229' + cleanNumber.slice(2);
    } else if (cleanNumber.length === 8) {
        cleanNumber = '229' + cleanNumber;
    }
    
    // Retirer le préfixe + si présent
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

// ==================== CONFIGURATION RATE LIMITING (ANTI-BAN) ====================

const RATE_LIMITING_CONFIG = {
    DEFAULT_MIN_DELAY_SEC: 60,      // Délai minimum entre messages
    DEFAULT_MAX_DELAY_SEC: 120,     // Délai maximum entre messages
    MIN_ALLOWED_DELAY_SEC: 45,      // Minimum absolu autorisé
    DEFAULT_BATCH_SIZE: 10,         // Taille d'un batch
    DEFAULT_BATCH_PAUSE_MINUTES: 3, // Pause après chaque batch
    MIN_ALLOWED_BATCH_PAUSE_MINUTES: 3,
    WITHIN_BATCH_MIN_DELAY_SEC: 8,  // Délai min intra-batch
    WITHIN_BATCH_MAX_DELAY_SEC: 20, // Délai max intra-batch
    LONG_BREAK_EVERY: 40,           // Pause longue tous les X messages
    LONG_BREAK_MIN_MINUTES: 8,      // Durée min pause longue
    LONG_BREAK_MAX_MINUTES: 15,     // Durée max pause longue
    DEFAULT_DAILY_LIMIT: 500,       // Limite quotidienne par défaut
    PRESENCE_COMPOSING_MIN_MS: 1500,// Min temps "écriture..."
    PRESENCE_COMPOSING_MAX_MS: 4000,// Max temps "écriture..."
    MAX_RETRIES_PER_MESSAGE: 3      // Tentatives max par message
};

// ==================== CONNEXION WHATSAPP (ROBUSTE) ====================

async function startBot() {
    try {
        reconnectAttempts++;
        console.log(`\n${'='.repeat(60)}`);
        console.log(`🔐 Tentative de connexion #${reconnectAttempts}`);
        console.log(`${'='.repeat(60)}\n`);

        // Connexion MongoDB
        console.log('🗄️  Connexion à MongoDB Atlas...');
        await mongoose.connect(MONGO_URI, {
            serverSelectionTimeoutMS: 10000,
            socketTimeoutMS: 45000,
            maxPoolSize: 10,
            retryWrites: true
        });
        console.log('✅ MongoDB connecté !');

        // Préparer l'auth
        const { state, saveCreds } = await useMongoDBAuthState();

        console.log('📱 Création de la socket WhatsApp...');
        
        sock = makeWASocket({
            auth: state,
            printQRInTerminal: true,
            syncFullHistory: false,           // ⚡ IMPORTANT: Éviter erreurs sync
            shouldSyncHistoryMessage: () => false, // ⚡ Double protection
            browser: ["Ecole Marie Auxiliatrice", "Chrome", "5.0.0"],
            keepAliveIntervalMs: 25000,       // Keep alive agressif (25s)
            connectTimeoutMs: 60000,
            defaultQueryTimeoutMs: undefined,
            logger: pino({ level: 'warn' }),  // Réduire logs bruit
            // Options anti-erreur
            retryRequestDelayMs: 5000,
            maxMsgRetryCount: 3,
            // Ne pas marquer en ligne immédiatement (moins suspect)
            markOnlineOnConnect: false,
            // Gestion des agents
            agent: undefined
        });

        // Sauvegarder les credentials quand ils changent
        sock.ev.on('creds.update', async (credsUpdate) => {
            try {
                Object.assign(state.creds, credsUpdate);
                await saveCreds();
                console.log('💾 Credentials mis à jour dans MongoDB');
            } catch (err) {
                console.error('❌ Erreur sauvegarde creds:', err.message);
            }
        });

        // ==================== GESTION DES ÉVÉNEMENTS DE CONNEXION ====================
        
        sock.ev.on('connection.update', async (update) => {
            const { connection, lastDisconnect, qr } = update;
            
            // Afficher QR code
            if (qr) {
                isReady = false;
                console.log('\n' + '📸'.repeat(30));
                console.log('QR CODE GÉNÉRÉ - Scannez avec WhatsApp !');
                console.log('📸'.repeat(30) + '\n');
                
                try {
                    qrcode.generate(qr, { small: true });
                } catch (e) {
                    console.log('QR Code (copiez-le):', qr);
                }
            }

            // Gérer fermeture de connexion
            if (connection === 'close') {
                isReady = false;
                
                const statusCode = lastDisconnect?.error?.output?.statusCode;
                const errorMessage = lastDisconnect?.error?.output?.payload?.message || '';
                const reason = lastDisconnect?.error?.message || '';

                console.log(`\n❌ Connexion fermée`);
                console.log(`   Code: ${statusCode || 'Inconnu'}`);
                console.log(`   Message: ${errorMessage || reason || 'Pas de message'}`);

                // Analyser la cause et décider de l'action
                if (statusCode === DisconnectReason.loggedOut) {
                    console.log('\n🔄 Session expirée (logged out). Nettoyage...');
                    await clearMongoDBAuth();
                    reconnectAttempts = 0;
                    setTimeout(startBot, 10000); // Attendre 10s avant reconnexion
                    
                } else if (
                    statusCode === 428 || 
                    errorMessage.includes('Placeholder') ||
                    errorMessage.includes('failed to find key') ||
                    reason.includes('decode patch')
                ) {
                    // Erreur de synchronisation / état corrompu
                    console.log('\n⚠️ Erreur de sync détectée.');
                    console.log('   Tentative de reconnexion avec nettoyage partiel...');
                    
                    reconnectAttempts = 0;
                    setTimeout(startBot, 15000); // Attendre 15s
                    
                } else if (statusCode === DisconnectReason.restartRequired) {
                    console.log('\n🔄 Redémarrage requis...');
                    reconnectAttempts = 0;
                    setTimeout(startBot, 5000);
                    
                } else {
                    // Autre erreur -> reconnecter avec backoff
                    const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
                    
                    if (shouldReconnect && reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
                        const delay = Math.min(reconnectAttempts * 5000, 30000); // Max 30s
                        console.log(`   Reconnexion dans ${delay/1000}s (tentative ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS})...`);
                        
                        setTimeout(startBot, delay);
                    } else if (reconnectAttempts >= MAX_RECONNECT_ATTEMPTS) {
                        console.error(`\n🚨 Trop de tentatives (${MAX_RECONNECT_ATTEMPTS}). Arrêt temporaire.`);
                        console.log('   Le bot essaiera de nouveau dans 5 minutes...\n');
                        setTimeout(() => {
                            reconnectAttempts = 0;
                            startBot();
                        }, 300000); // 5 minutes
                    } else {
                        console.log('\n⛔ Reconnexion non autorisée pour cette erreur.');
                    }
                }
            }

            // Connexion réussie
            if (connection === 'open') {
                isReady = true;
                connectionOpenCount++;
                reconnectAttempts = 0; // Reset compteur succès
                
                console.log('\n' + '✅'.repeat(30));
                console.log(`✅ CONNECTÉ À WHATSAPP AVEC SUCCÈS !`);
                console.log(`   Connexion #${connectionOpenCount}`);
                console.log('✅'.repeat(30) + '\n');

                // Reprendre un job en cours s'il y en a un
                if (bulkJob && (bulkJob.status === 'pending' || bulkJob.status === 'paused_daily_limit')) {
                    console.log(`📤 Reprise automatique du job ${bulkJob.jobId}...`);
                    setTimeout(() => processBulkJob(), 3000);
                }
            }

            // Autres états de connexion
            if (connection === 'connecting') {
                console.log('🔄 Connexion en cours...');
            }
            
            if (connection === 'disconnecting') {
                console.log('👋 Déconnexion en cours...');
            }
        });

        // ==================== GESTION DES ERREURS DE SOCKET ====================
        
        sock.ev.on('error', (error) => {
            const errorMsg = error?.message || String(error);
            
            console.error('\n🚨 ERREUR SOCKET:', errorMsg.substring(0, 200));

            // Erreurs critiques à surveiller
            if (
                errorMsg.includes('failed to find key') ||
                errorMsg.includes('decode patch') ||
                errorMsg.includes('sync') ||
                errorMsg.includes('Stream closed')
            ) {
                console.error('💡 Cette erreur peut nécessiter une reconnexion');
                // La reconnexion sera gérée par connection.update
            }
        });

        // Messages entrants (optionnel - pour log)
        sock.ev.on('messages.upsert', async ({ messages }) => {
            for (const msg of messages) {
                if (!msg.message) continue;
                
                const from = msg.key.remoteJid;
                const messageType = Object.keys(msg.message)[0];
                
                // Ignorer les notifications système
                if (messageType ===protocolMessage || messageType ===senderKeyDistributionMessage) {
                    continue;
                }

                console.log(`💬 Message reçu de ${from} [${messageType}]`);
            }
        });

        return sock;

    } catch (err) {
        console.error('💥 Erreur critique initialisation bot:', err.message);
        console.error('Stack:', err.stack);
        
        // Planifier une nouvelle tentative
        if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
            const delay = Math.min(reconnectAttempts * 10000, 60000);
            console.log(`🔄 Nouvelle tentative dans ${delay/1000}s...`);
            setTimeout(startBot, delay);
        } else {
            console.error('🚨 Trop d\'échecs consécutifs. Attente 5 minutes...');
            setTimeout(() => {
                reconnectAttempts = 0;
                startBot();
            }, 300000);
        }
        
        return null;
    }
}

// ==================== NETTOYAGE AUTH MONGODB ====================

async function clearMongoDBAuth() {
    try {
        console.log('🗑️  Suppression de l\'auth MongoDB...');
        
        // Supprimer tous les documents auth
        const result = await AuthModel.deleteMany({});
        console.log(`   ✅ ${result.deletedCount} documents supprimés`);
        
        return true;
    } catch (err) {
        console.error('❌ Erreur nettoyage auth:', err.message);
        return false;
    }
}

// ==================== PROCESSING BULK JOB (AMÉLIORÉ) ====================

async function processBulkJob() {
    if (!bulkJob) return;

    try {
        bulkJob.status = 'running';
        
        // Sauvegarder le status dans MongoDB
        await BulkJobModel.findOneAndUpdate(
            { jobId: bulkJob.jobId },
            { status: 'running' }
        ).catch(() => {});

        console.log(`\n${'🚀'.repeat(25)}`);
        console.log(` DÉMARRAGE ENVOI EN MASSE`);
        console.log(` Job ID: ${bulkJob.jobId}`);
        console.log(` Messages: ${bulkJob.items.length}`);
        console.log(`${'🚀'.repeat(25)}\n`);

        for (let i = bulkJob.currentIndex; i < bulkJob.items.length; i++) {
            // Vérifier annulation
            if (bulkJob.cancelled) {
                bulkJob.status = 'cancelled';
                console.log('\n⏹️  Job annulé par l\'utilisateur');
                break;
            }

            // Attendre que WhatsApp soit prêt
            while (!isReady && !bulkJob.cancelled) {
                console.log('⏳ Attente reconnexion WhatsApp...');
                await sleep(15000); // 15 secondes
            }
            
            if (bulkJob.cancelled) {
                bulkJob.status = 'cancelled';
                break;
            }

            // Vérifier reset quotidien
            if (todayStr() !== bulkJob.currentDay) {
                bulkJob.currentDay = todayStr();
                bulkJob.sentToday = 0;
            }

            // Vérifier limite quotidienne
            if (bulkJob.sentToday >= bulkJob.config.dailyLimit) {
                console.log(`\n⏸️ LIMITE QUOTIDIENNE ATTEINTE (${bulkJob.config.dailyLimit})`);
                console.log(`   Messages envoyés aujourd\'hui: ${bulkJob.sentToday}`);
                console.log(`   Pause jusqu'à demain 00h00...\n`);
                
                bulkJob.status = 'paused_daily_limit';
                
                // Sauvegarder dans MongoDB
                await BulkJobModel.findOneAndUpdate(
                    { jobId: bulkJob.jobId },
                    { $set: bulkJob.toObject ? bulkJob.toObject() : bulkJob }
                ).catch(() => {});
                
                // Attendre le jour suivant
                while (todayStr() === bulkJob.currentDay && !bulkJob.cancelled) {
                    await sleep(15 * 60 * 1000); // Vérifier toutes les 15 min
                }
                
                if (bulkJob.cancelled) {
                    bulkJob.status = 'cancelled';
                    break;
                }
                
                // Reset pour le nouveau jour
                bulkJob.currentDay = todayStr();
                bulkJob.sentToday = 0;
                bulkJob.status = 'running';
                
                console.log(`\n🌅 Nouveau jour ! Reprise de l'envoi...\n`);
            }

            // Traiter le message courant
            const item = bulkJob.items[i];
            bulkJob.currentIndex = i;

            let success = false;
            let lastError = null;

            // Tentatives avec retries
            for (let attempt = 1; attempt <= RATE_LIMITING_CONFIG.MAX_RETRIES_PER_MESSAGE; attempt++) {
                try {
                    const recipientJid = formatNumber(item.number);

                    // Simuler comportement humain (typing indicator)
                    try {
                        await sock.sendPresenceUpdate('composing', recipientJid);
                        const composingTime = RATE_LIMITING_CONFIG.PRESENCE_COMPOSING_MIN_MS + 
                            Math.floor(Math.random() * (RATE_LIMITING_CONFIG.PRESENCE_COMPOSING_MAX_MS - RATE_LIMITING_CONFIG.PRESENCE_COMPOSING_MIN_MS));
                        await sleep(composingTime);
                        await sock.sendPresenceUpdate('paused', recipientJid);
                    } catch (presenceError) {
                        // Ignorer les erreurs de présence (non critiques)
                        console.warn(`   ⚠️ Presence update échoué: ${presenceError.message.substring(0, 50)}`);
                    }

                    // Envoyer le message
                    console.log(`📤 [${i + 1}/${bulkJob.items.length}] → ${item.number}`);
                    console.log(`   Tentative ${attempt}/${RATE_LIMITING_CONFIG.MAX_RETRIES_PER_MESSAGE}`);
                    
                    const result = await sock.sendMessage(recipientJid, { text: item.message });

                    // Succès !
                    bulkJob.results.push({
                        number: item.number,
                        jid: recipientJid,
                        status: 'success',
                        id_message: result?.key?.id,
                        timestamp: new Date().toISOString()
                    });
                    
                    bulkJob.sentCount++;
                    bulkJob.sentToday++;
                    success = true;

                    console.log(`   ✅ SUCCÈS (ID: ${result?.key?.id})`);
                    break; // Sortir de la boucle de retry

                } catch (error) {
                    lastError = error;
                    console.error(`   ❌ Erreur tentative ${attempt}: ${error.message.substring(0, 100)}`);
                    
                    if (attempt < RATE_LIMITING_CONFIG.MAX_RETRIES_PER_MESSAGE) {
                        const retryDelay = attempt * 5000; // 5s, 10s, 15s...
                        console.log(`   🔄 Nouvelle tentative dans ${retryDelay/1000}s...`);
                        await sleep(retryDelay);
                    }
                }
            }

            // Si échec après toutes les tentatives
            if (!success) {
                bulkJob.results.push({
                    number: item.number,
                    status: 'error',
                    error: lastError?.message || 'Erreur inconnue',
                    timestamp: new Date().toISOString()
                });
                bulkJob.failedCount++;
                console.error(`   💥 ÉCHEC FINAL pour ${item.number}`);
            }

            // Calculer le délai avant le prochain message
            const isLastMessage = i === bulkJob.items.length - 1;
            
            if (!isLastMessage) {
                const messagesSentSinceStart = i - bulkJob.startIndex + 1;
                
                // Déterminer le type de pause nécessaire
                const dueForLongBreak = messagesSentSinceStart > 0 && 
                    messagesSentSinceStart % RATE_LIMITING_CONFIG.LONG_BREAK_EVERY === 0;
                    
                const dueForBatchPause = !dueForLongBreak && 
                    bulkJob.config.batchSize > 0 && 
                    messagesSentSinceStart % bulkJob.config.batchSize === 0;

                let delayMs;
                let pauseType;

                if (dueForLongBreak) {
                    // Pause longue
                    const minutes = RATE_LIMITING_CONFIG.LONG_BREAK_MIN_MINUTES + 
                        Math.random() * (RATE_LIMITING_CONFIG.LONG_BREAK_MAX_MINUTES - RATE_LIMITING_CONFIG.LONG_BREAK_MIN_MINUTES);
                    delayMs = Math.round(minutes * 60 * 1000);
                    pauseType = `LONGUE PAUSE (~${Math.round(minutes)} min)`;
                    
                } else if (dueForBatchPause) {
                    // Pause batch
                    delayMs = Math.round(bulkJob.config.batchPauseMinutes * 60 * 1000);
                    pauseType = `PAUSE BATCH (~${bulkJob.config.batchPauseMinutes} min)`;
                    
                } else {
                    // Délai normal
                    delayMs = randomDelayMs(
                        RATE_LIMITING_CONFIG.WITHIN_BATCH_MIN_DELAY_SEC,
                        RATE_LIMITING_CONFIG.WITHIN_BATCH_MAX_DELAY_SEC
                    );
                    pauseType = `DÉLAI NORMAL (~${Math.round(delayMs/1000)}s)`;
                }

                bulkJob.nextSendAt = new Date(Date.now() + delayMs);

                console.log(`\n⏳ ${pauseType}`);
                console.log(`   Prochain message: ${new Date(bulkJob.nextSendAt).toLocaleTimeString()}\n`);

                // Sauvegarder progression périodiquement (tous les 10 messages)
                if (i % 10 === 0) {
                    await BulkJobModel.findOneAndUpdate(
                        { jobId: bulkJob.jobId },
                        { 
                            $set: {
                                currentIndex: i,
                                sentCount: bulkJob.sentCount,
                                failedCount: bulkJob.failedCount,
                                sentToday: bulkJob.sentToday,
                                nextSendAt: bulkJob.nextSendAt,
                                results: bulkJob.results.slice(-50) // Garder les 50 derniers résultats
                            }
                        }
                    ).catch(err => console.warn('⚠️ Erreur sauvegarde progression:', err.message));
                }

                await sleep(delayMs);
            }
        }

        // Terminé !
        bulkJob.status = 'completed';
        bulkJob.finishedAt = new Date();

        console.log(`\n${'✅'.repeat(30)}`);
        console.log(` ENVOI TERMINÉ !`);
        console.log(`${'✅'.repeat(30)}`);
        console.log(`\n📊 RÉSUMÉ:`);
        console.log(`   ✅ Succès: ${bulkJob.sentCount}`);
        console.log(`   ❌ Échecs: ${bulkJob.failedCount}`);
        console.log(`   📝 Total: ${bulkJob.items.length}`);
        console.log(`   ⏱️  Durée: Depuis ${new Date(bulkJob.startedAt).toLocaleTimeString()} jusqu'à maintenant\n`);

        // Sauvegarder final dans MongoDB
        try {
            await BulkJobModel.findOneAndUpdate(
                { jobId: bulkJob.jobId },
                { 
                    $set: {
                        status: 'completed',
                        finishedAt: bulkJob.finishedAt,
                        currentIndex: bulkJob.items.length - 1,
                        sentCount: bulkJob.sentCount,
                        failedCount: bulkJob.failedCount,
                        results: bulkJob.results
                    }
                }
            );
            console.log('💾 Résultats sauvegardés dans MongoDB');
        } catch (err) {
            console.error('❌ Erreur sauvegarde finale:', err.message);
        }

    } catch (error) {
        console.error('💥 ERREUR CRITIQUE dans processBulkJob:', error);
        bulkJob.status = 'failed';
        bulkJob.finishedAt = new Date();
    }
}

// ==================== ROUTES API ====================

/**
 * Health check pour UptimeRobot (keep-alive Render)
 */
app.get('/ping', (req, res) => {
    res.status(200).json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        whatsapp: isReady ? 'connected' : 'disconnected'
    });
});

app.get('/health', (req, res) => {
    res.status(isReady ? 200 : 503).json({
        status: isReady ? 'healthy' : 'degraded',
        whatsapp: {
            connected: isReady,
            connectionCount: connectionOpenCount,
            reconnectAttempts
        },
        system: {
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            nodeVersion: process.version,
            platform: process.platform
        },
        job: bulkJob ? {
            id: bulkJob.jobId,
            status: bulkJob.status,
            progress: `${bulkJob.currentIndex + 1}/${bulkJob.items.length}`,
            sentCount: bulkJob.sentCount,
            failedCount: bulkJob.failedCount
        } : null,
        timestamp: new Date().toISOString()
    });
});

/**
 * Page d'accueil / Dashboard
 */
app.get('/', (req, res) => {
    res.json({
        service: 'WhatsApp Bot - Ecole Marie Auxiliatrice',
        version: '3.0.0',
        status: isReady ? 'connected' : 'waiting_qr',
        whatsapp: {
            ready: isReady,
            connections: connectionOpenCount
        },
        endpoints: {
            health: '/health',
            sendSingle: 'POST /send-message',
            sendBulk: 'POST /send-bulk-messages',
            bulkStatus: 'GET /bulk-status',
            bulkCancel: 'POST /bulk-cancel',
            bulkResults: 'GET /bulk-results',
            resetAuth: 'GET /reset-auth (urgence)'
        },
        message: isReady 
            ? '✅ Le bot WhatsApp est prêt à envoyer des messages !'
            : '⏳ En attente du scan du QR Code... (Voir les logs Render)',
        timestamp: new Date().toISOString()
    });
});

/**
 * Envoi d'un message unique
 */
app.post('/send-message', async (req, res) => {
    if (!isReady) {
        return res.status(503).json({ 
            status: 'error', 
            error: 'Le client WhatsApp n\'est pas encore connecté.' 
        });
    }

    const rawNumber = req.body.number || req.body.phone;
    const message = req.body.message || req.body.text;

    if (!rawNumber || !message) {
        return res.status(400).json({ 
            status: 'error', 
            error: 'Les champs "number" et "message" sont requis.',
            example: { number: 'XXXXXXXXX', message: 'Votre texte ici' }
        });
    }

    try {
        const recipientJid = formatNumber(rawNumber);
        
        // Simuler typing
        try {
            await sock.sendPresenceUpdate('composing', recipientJid);
            await sleep(1000 + Math.floor(Math.random() * 2000));
        } catch(e) {}

        const result = await sock.sendMessage(recipientJid, { text: message });
        
        // Arrêter typing
        try {
            await sock.sendPresenceUpdate('paused', recipientJid);
        } catch(e) {}

        return res.json({
            status: 'success',
            message: 'Message envoyé avec succès !',
            jid_destinataire: recipientJid,
            id_message: result?.key?.id,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('Erreur envoi message:', error);
        return res.status(500).json({ 
            status: 'error', 
            error: error.message,
            details: 'Voir les logs Render pour plus d\'informations'
        });
    }
});

/**
 * Lancement d'un envoi en masse
 */
app.post('/send-bulk-messages', async (req, res) => {
    if (!isReady) {
        return res.status(503).json({ 
            status: 'error', 
            error: 'Le client WhatsApp n\'est pas encore connecté.' 
        });
    }

    if (bulkJob && (bulkJob.status === 'running' || bulkJob.status === 'pending')) {
        return res.status(409).json({ 
            status: 'error', 
            error: 'Un envoi en masse est déjà en cours.',
            currentJobId: bulkJob.jobId,
            hint: 'Utilisez GET /bulk-status pour voir la progression ou POST /bulk-cancel pour annuler.'
        });
    }

    const messages = req.body.messages;
    
    // Configuration avec valeurs par défaut sécurisées
    const config = {
        minDelaySec: Math.max(
            Number(req.body.minDelaySeconds) || RATE_LIMITING_CONFIG.DEFAULT_MIN_DELAY_SEC,
            RATE_LIMITING_CONFIG.MIN_ALLOWED_DELAY_SEC
        ),
        maxDelaySec: Number(req.body.maxDelaySeconds) || RATE_LIMITING_CONFIG.DEFAULT_MAX_DELAY_SEC,
        dailyLimit: Number(req.body.dailyLimit) || RATE_LIMITING_CONFIG.DEFAULT_DAILY_LIMIT,
        batchSize: Number(req.body.batchSize) || RATE_LIMITING_CONFIG.DEFAULT_BATCH_SIZE,
        batchPauseMinutes: Math.max(
            Number(req.body.batchPauseMinutes) || RATE_LIMITING_CONFIG.DEFAULT_BATCH_PAUSE_MINUTES,
            RATE_LIMITING_CONFIG.MIN_ALLOWED_BATCH_PAUSE_MINUTES
        )
    };

    // Validation
    if (!Array.isArray(messages) || messages.length === 0) {
        return res.status(400).json({ 
            status: 'error', 
            error: 'Le champ "messages" doit être un tableau non vide.',
            example: {
                messages: [
                    { number: 'XXXXXXXXX', message: 'Texte 1' },
                    { number: 'YYYYYYYYY', message: 'Texte 2' }
                ]
            }
        });
    }

    if (config.minDelaySec < RATE_LIMITING_CONFIG.MIN_ALLOWED_DELAY_SEC) {
        return res.status(400).json({ 
            status: 'error', 
            error: `Le délai minimum doit être d'au moins ${RATE_LIMITING_CONFIG.MIN_ALLOWED_DELAY_SEC} secondes.` 
        });
    }

    // Créer le job
    const jobId = Date.now().toString();
    
    bulkJob = {
        id: jobId,
        jobId: jobId,
        items: messages.map(m => ({
            number: m.number || m.phone,
            message: m.message || m.text
        })),
        currentIndex: 0,
        startIndex: 0,
        sentCount: 0,
        failedCount: 0,
        results: [],
        status: 'pending',
        cancelled: false,
        config: config,
        sentToday: 0,
        currentDay: todayStr(),
        startedAt: new Date(),
        finishedAt: null,
        nextSendAt: null
    };

    // Sauvegarder dans MongoDB pour persistance
    try {
        const bulkJobDoc = new BulkJobModel(bulkJob);
        await bulkJobDoc.save();
        console.log(`💾 Job ${jobId} sauvegardé dans MongoDB`);
    } catch (err) {
        console.warn('⚠️ Erreur sauvegarde job MongoDB:', err.message);
        // Continuer même si la sauvegarde échoue (le job tourne en mémoire aussi)
    }

    // Démarrer le processing de manière asynchrone
    setImmediate(processBulkJob);

    // Calcul estimé du temps total
    const estimatedTotalMinutes = (messages.length * ((config.minDelaySec + config.maxDelaySec) / 2)) / 60;
    const estimatedWithPauses = estimatedTotalMinutes * 1.5; // +50% pour les pauses

    return res.status(202).json({
        status: 'accepted',
        message: `Envoi en masse lancé pour ${bulkJob.items.length} messages.`,
        job_id: jobId,
        configuration: {
            totalMessages: bulkJob.items.length,
            delayRange: `${config.minDelaySec}-${config.maxDelaySec} secondes`,
            batchSize: config.batchSize,
            batchPause: `${config.batchPauseMinutes} minutes`,
            dailyLimit: config.dailyLimit
        },
        estimates: {
            estimatedTimeMinutes: Math.round(estimatedWithPauses),
            longBreakEvery: RATE_LIMITING_CONFIG.LONG_BREAK_EVERY,
            longBreakDuration: `${RATE_LIMITING_CONFIG.LONG_BREAK_MIN_MINUTES}-${RATE_LIMITING_CONFIG.LONG_BREAK_MAX_MINUTES} min`
        },
        endpoints: {
            status: `/bulk-status?job_id=${jobId}`,
            cancel: `/bulk-cancel`,
            results: `/bulk-results?job_id=${jobId}`
        },
        warnings: [
            'Ne fermez pas cet onglet pendant l\'envoi',
            'Le job survit aux redémarrages Render (sauvegardé dans MongoDB)',
            'Check /bulk-status régulièrement pour la progression'
        ],
        timestamp: new Date().toISOString()
    });
});

/**
 * Statut du job en cours
 */
app.get('/bulk-status', async (req, res) => {
    if (!bulkJob) {
        // Vérifier dans MongoDB s'il y a un job récent
        try {
            const recentJob = await BulkJobModel.findOne().sort({ createdAt: -1 }).limit(1);
            
            if (recentJob && recentJob.status !== 'completed' && recentJob.status !== 'cancelled') {
                // Restaurer le job depuis MongoDB
                bulkJob = recentJob.toObject();
                return res.json(bulkJob);
            }
        } catch (e) {}
        
        return res.json({ 
            status: 'idle', 
            message: 'Aucun envoi en masse lancé ou terminé.',
            hint: 'Utilisez POST /send-bulk-messages pour démarrer un nouvel envoi.'
        });
    }

    // Mettre à jour depuis MongoDB périodiquement
    try {
        const mongoJob = await BulkJobModel.findOne({ jobId: bulkJob.jobId });
        if (mongoJob && mongoJob.status !== bulkJob.status) {
            bulkJob.status = mongoJob.status;
        }
    } catch (e) {}

    return res.json({
        ...bulkJob,
        progress: {
            current: bulkJob.currentIndex + 1,
            total: bulkJob.items.length,
            percent: ((bulkJob.currentIndex + 1) / bulkJob.items.length * 100).toFixed(1)
        },
        stats: {
            sent: bulkJob.sentCount,
            failed: bulkJob.failedCount,
            remaining: bulkJob.items.length - bulkJob.currentIndex - 1,
            sentToday: bulkJob.sentToday,
            dailyLimit: bulkJob.config?.dailyLimit || RATE_LIMITING_CONFIG.DEFAULT_DAILY_LIMIT
        },
        timestamps: {
            started: bulkJob.startedAt,
            nextSend: bulkJob.nextSendAt,
            finished: bulkJob.finishedAt
        }
    });
});

/**
 * Annuler un job en cours
 */
app.post('/bulk-cancel', async (req, res) => {
    if (!bulkJob || (bulkJob.status !== 'running' && bulkJob.status !== 'pending' && bulkJob.status !== 'paused_daily_limit')) {
        return res.status(400).json({ 
            status: 'error', 
            error: 'Aucun envoi actif à annuler.',
            hint: 'Vérifiez /bulk-status pour voir s\'il y a un job en cours.'
        });
    }

    bulkJob.cancelled = true;

    // Mettre à jour MongoDB
    try {
        await BulkJobModel.findOneAndUpdate(
            { jobId: bulkJob.jobId },
            { cancelled: true, status: 'cancelled' }
        );
    } catch (e) {}

    return res.json({ 
        status: 'success', 
        message: 'Annulation demandée. Le job s\'arrêtera au prochain message.',
        job_id: bulkJob.jobId,
        progression: {
            completed: bulkJob.sentCount,
            remaining: bulkJob.items.length - bulkJob.currentIndex - 1
        },
        note: 'Les résultats déjà envoyés sont disponibles via /bulk-results'
    });
});

/**
 * Résultats d'un job
 */
app.get('/bulk-results', async (req, res) => {
    const requestedJobId = req.query.job_id;
    
    try {
        let job;
        
        if (requestedJobId) {
            job = await BulkJobModel.findOne({ jobId: requestedJobId });
        } else if (bulkJob) {
            job = bulkJob;
        } else {
            // Dernier job terminé
            job = await BulkJobModel.findOne({ status: 'completed' }).sort({ finishedAt: -1 }).limit(1);
        }

        if (!job) {
            return res.json({ 
                status: 'not_found', 
                message: 'Aucun résultat trouvé.',
                hint: 'Spécifiez ?job_id=XXX ou lancez un premier envoi.'
            });
        }

        return res.json({
            job_id: job.jobId || job.id,
            status: job.status,
            summary: {
                total: job.items?.length || 0,
                success: job.sentCount || 0,
                failed: job.failedCount || 0,
                duration: job.startedAt && job.finishedAt 
                    ? `${Math.round((new Date(job.finishedAt) - new Date(job.startedAt)) / 60000)} minutes`
                    : 'En cours...'
            },
            results_count: job.results?.length || 0,
            // N'afficher que les 100 derniers résultats pour éviter de surcharger la réponse
            results_preview: (job.results || []).slice(-100),
            full_results_available: `/bulk-results?job_id=${job.jobId}&full=true`,
            timestamps: {
                started: job.startedAt,
                finished: job.finishedAt
            }
        });

    } catch (error) {
        console.error('Erreur récupération résultats:', error);
        return res.status(500).json({ 
            status: 'error', 
            error: error.message 
        });
    }
});

/**
 * RESET AUTH (Urgence - si erreurs de sync)
 */
app.get('/reset-auth', async (req, res) => {
    try {
        console.log('\n🗑️ DEMANDE DE RESET AUTH REÇUE\n');

        // Déconnecter proprement
        if (sock) {
            try {
                sock.end(); // Fermer la socket
            } catch (e) {}
            sock = null;
            isReady = false;
        }

        // Supprimer l'auth MongoDB
        const cleared = await clearMongoDBAuth();

        // Reset compteurs
        reconnectAttempts = 0;
        connectionOpenCount = 0;

        res.json({
            success: true,
            message: cleared 
                ? '✅ Auth MongoDB supprimée avec succès !' 
                : '⚠️ Auth supprimée (ou inexistante)',
            instructions: [
                '1. Attendez 10-15 secondes',
                '2. Allez sur /health pour vérifier le statut',
                '3. Regardez les LOGS RENDER (onglet Logs)',
                '4. Scannez le QR code qui va apparaître',
                '5. Le QR est valide ~20 secondes seulement !'
            ],
            nextSteps: {
                checkStatus: '/health',
                viewLogs: 'Dashboard Render → Onglet "Logs"',
                warning: 'Vous devrez rescanner votre QR code WhatsApp'
            },
            timestamp: new Date().toISOString()
        });

        // Reconnexion automatique après 5 secondes
        console.log('🔄 Reconnexion automatique dans 5 secondes...');
        setTimeout(async () => {
            try {
                await startBot();
            } catch (e) {
                console.error('❌ Erreur reconnexion:', e);
            }
        }, 5000);

    } catch (error) {
        console.error('❌ Erreur reset auth:', error);
        res.status(500).json({
            success: false,
            error: error.message,
            hint: 'Vérifiez les logs Render pour plus de détails'
        });
    }
});

/**
 * Vérifier l'état de l'auth MongoDB
 */
app.get('/check-auth', async (req, res) => {
    try {
        const count = await AuthModel.countDocuments();
        const samples = await AuthModel.find().limit(5).select('_id').lean();
        
        res.json({
            mongodb_connected: mongoose.connection.readyState === 1,
            auth_documents_count: count,
            sample_keys: samples.map(s => s._id),
            healthy: count >= 3, // Au moins 3 docs = probablement OK
            recommendation: count === 0 
                ? 'AUTH VIDE - Doit reconnecter (scan QR)' 
                : count >= 3 
                    ? 'Auth semble présente et complète' 
                    : 'Auth partielle - Possible corruption',
            actions: count === 0 
                ? { reset: 'Non nécessaire', connect: 'Attendez génération QR auto' }
                : { reset: 'GET /reset-auth si erreurs', connect: 'Déjà connecté' }
        });
        
    } catch (error) {
        res.status(500).json({
            error: error.message,
            mongodb_connected: false
        });
    }
});

// ==================== DÉMARRAGE SERVEUR ====================

app.listen(PORT, async () => {
    console.log(`
╔═══════════════════════════════════════════════════════╗
║                                                       ║
║   🤖 WHATSAPP BOT v3.0 - ECOLE MARIE AUXILIATRICE     ║
║                                                       ║
║   Serveur: http://localhost:${PORT}                      ║
║   Mode: Render Free Optimized                          ║
║   Auth: MongoDB Atlas (Persistant)                     ║
║   Anti-Ban: ACTIVÉ                                     ║
║                                                       ║
╚═══════════════════════════════════════════════════════╝
    `);

    // Démarrer le bot
    await startBot();
});

// ==================== GESTION GRACEFUL SHUTDOWN ====================

process.on('SIGTERM', async () => {
    console.log('\n🛑 SIGTERM reçu. Fermeture graceful...');
    
    if (bulkJob && bulkJob.status === 'running') {
        console.log(`💾 Sauvegarde du job ${bulkJob.jobId} avant arrêt...`);
        try {
            await BulkJobModel.findOneAndUpdate(
                { jobId: bulkJob.jobId },
                { $set: { status: 'paused_daily_limit', /* sera repris */ } }
            );
        } catch (e) {}
    }
    
    if (sock) {
        try { sock.end(); } catch(e) {}
    }
    
    await mongoose.connection.close();
    console.log('✅ Fermeture terminée');
    process.exit(0);
});

process.on('uncaughtException', (error) => {
    console.error('💥 Uncaught Exception:', error.message);
    // Ne pas crasher, juste logger
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('💥 Unhandled Rejection at:', promise, 'reason:', reason);
});
