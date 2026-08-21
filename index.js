const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const express = require('express');

const app = express();
app.use(express.json({ limit: '5mb' }));

let sock;
let isReady = false;

async function connectToWhatsApp() {
    try {
        const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');

        sock = makeWASocket({
            auth: state,
            printQRInTerminal: false,
            browser: ["Ecole Marie Auxiliatrice", "Chrome", "1.0.0"],
            keepAliveIntervalMs: 30000, // Envoie des pings réseau à WhatsApp toutes les 30s
            connectTimeoutMs: 60000
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
                    setTimeout(connectToWhatsApp, 5000);
                }
            } else if (connection === 'open') {
                isReady = true;
                console.log('✅ Connecté à WhatsApp avec succès via Baileys !');
            }
        });
    } catch (err) {
        console.error('Erreur d\'initialisation Baileys :', err);
    }
}

connectToWhatsApp();

// Utilitaire : formatage du numéro au format Bénin (+229)
function formatNumber(rawNumber) {
    let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');

    if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) {
        cleanNumber = '229' + cleanNumber;
    } else if (cleanNumber.length === 8) {
        cleanNumber = '229' + cleanNumber;
    }

    return `${cleanNumber}@s.whatsapp.net`;
}

// Utilitaire : pause de x millisecondes
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Utilitaire : délai aléatoire entier entre min et max (secondes) -> ms
function randomDelayMs(minSeconds, maxSeconds) {
    const min = Math.ceil(minSeconds);
    const max = Math.floor(maxSeconds);
    const seconds = Math.floor(Math.random() * (max - min + 1)) + min;
    return seconds * 1000;
}

// Route 1 : Ping dédié pour Cron-Job / UptimeRobot
app.get('/ping', (req, res) => {
    res.status(200).send('pong');
});

// Route 2 : Vérification du statut global du bot
app.get('/', (req, res) => {
    res.json({
        status: isReady ? 'connected' : 'waiting_qr',
        message: isReady ? 'Le bot WhatsApp est prêt !' : 'En attente du scan du QR Code dans les logs Render...'
    });
});

// Route 3 : Envoi de message WhatsApp (unitaire)
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
            error: 'Les champs "number" (ou "phone") et "message" sont requis.'
        });
    }

    try {
        const recipientJid = formatNumber(rawNumber);

        let targetJid = recipientJid;
        try {
            const [exists] = await sock.onWhatsApp(recipientJid);
            if (exists && exists.jid) {
                targetJid = exists.jid;
            }
        } catch (e) {
            console.log('Vérification JID ignorée, envoi direct à :', recipientJid);
        }

        const result = await sock.sendMessage(targetJid, { text: message });

        console.log(`[OK] Message envoyé à ${targetJid}`);
        return res.json({
            status: 'success',
            message: 'Message envoyé avec succès !',
            jid_destinataire: targetJid,
            id_message: result?.key?.id
        });

    } catch (error) {
        console.error('Erreur lors de l\'envoi :', error);
        return res.status(500).json({ status: 'error', error: error.message });
    }
});

// ============================================================
//  ENVOI EN MASSE (jusqu'à 1500 messages) AVEC DÉLAI ANTI-BAN
// ============================================================
//
// Principe :
// - On met les messages en file d'attente (queue) en mémoire.
// - Un seul job d'envoi en masse tourne à la fois.
// - Entre CHAQUE message, on attend un délai aléatoire (par défaut
//   30 à 45 secondes) pour limiter le risque de bannissement par
//   WhatsApp (envoi trop rapide/robotique = signal de spam).
// - Le traitement se fait en arrière-plan (asynchrone) : la requête
//   HTTP répond immédiatement, et vous suivez la progression via
//   GET /bulk-status.
//
// ATTENTION : pour 1500 messages avec ~37s de moyenne entre chaque,
// comptez environ 15h de traitement total. C'est voulu : c'est ce
// qui protège le compte WhatsApp.

const MAX_BULK_MESSAGES = 1500;
const DEFAULT_MIN_DELAY_SEC = 30;
const DEFAULT_MAX_DELAY_SEC = 45;

let bulkJob = null; // état du job en cours (un seul à la fois)

async function processBulkJob() {
    if (!bulkJob) return;

    bulkJob.status = 'running';

    for (let i = bulkJob.currentIndex; i < bulkJob.items.length; i++) {
        // Si le job a été annulé entre-temps
        if (bulkJob.cancelled) {
            bulkJob.status = 'cancelled';
            console.log('[BULK] Job annulé par l\'utilisateur.');
            return;
        }

        // Si la connexion WhatsApp tombe en cours de route, on met en pause
        // et on réessaie régulièrement au lieu d'échouer tous les messages restants.
        while (!isReady && !bulkJob.cancelled) {
            console.log('[BULK] WhatsApp non connecté, pause de 10s avant nouvelle tentative...');
            await sleep(10000);
        }
        if (bulkJob.cancelled) {
            bulkJob.status = 'cancelled';
            return;
        }

        const item = bulkJob.items[i];
        bulkJob.currentIndex = i;

        try {
            const recipientJid = formatNumber(item.number);
            let targetJid = recipientJid;

            try {
                const [exists] = await sock.onWhatsApp(recipientJid);
                if (exists && exists.jid) {
                    targetJid = exists.jid;
                }
            } catch (e) {
                // on ignore l'échec de vérification et on tente l'envoi direct
            }

            const result = await sock.sendMessage(targetJid, { text: item.message });

            bulkJob.results.push({
                number: item.number,
                jid: targetJid,
                status: 'success',
                id_message: result?.key?.id
            });
            bulkJob.sentCount++;
            console.log(`[BULK ${i + 1}/${bulkJob.items.length}] ✅ Envoyé à ${targetJid}`);

        } catch (error) {
            bulkJob.results.push({
                number: item.number,
                status: 'error',
                error: error.message
            });
            bulkJob.failedCount++;
            console.error(`[BULK ${i + 1}/${bulkJob.items.length}] ❌ Échec pour ${item.number} :`, error.message);
        }

        const isLastMessage = i === bulkJob.items.length - 1;
        if (!isLastMessage) {
            const delayMs = randomDelayMs(bulkJob.minDelaySec, bulkJob.maxDelaySec);
            bulkJob.nextSendAt = Date.now() + delayMs;
            console.log(`[BULK] Prochain envoi dans ${Math.round(delayMs / 1000)}s...`);
            await sleep(delayMs);
        }
    }

    bulkJob.status = 'completed';
    bulkJob.finishedAt = new Date().toISOString();
    console.log(`[BULK] Job terminé : ${bulkJob.sentCount} envoyés, ${bulkJob.failedCount} échecs.`);
}

// Route 4 : Lancer un envoi en masse
// Body attendu :
// {
//   "messages": [
//     { "number": "0197xxxxxx", "message": "Bonjour ..." },
//     { "number": "0198xxxxxx", "message": "Bonjour ..." }
//   ],
//   "minDelaySeconds": 30,   // optionnel, défaut 30
//   "maxDelaySeconds": 45    // optionnel, défaut 45
// }
app.post('/send-bulk-messages', (req, res) => {
    if (!isReady) {
        return res.status(503).json({
            status: 'error',
            error: 'Le client WhatsApp n\'est pas encore connecté.'
        });
    }

    if (bulkJob && (bulkJob.status === 'running' || bulkJob.status === 'pending')) {
        return res.status(409).json({
            status: 'error',
            error: 'Un envoi en masse est déjà en cours. Attendez sa fin ou annulez-le (POST /bulk-cancel) avant d\'en lancer un nouveau.'
        });
    }

    const messages = req.body.messages;
    const minDelaySeconds = Number(req.body.minDelaySeconds) || DEFAULT_MIN_DELAY_SEC;
    const maxDelaySeconds = Number(req.body.maxDelaySeconds) || DEFAULT_MAX_DELAY_SEC;

    if (!Array.isArray(messages) || messages.length === 0) {
        return res.status(400).json({
            status: 'error',
            error: 'Le champ "messages" doit être un tableau non vide de { number, message }.'
        });
    }

    if (messages.length > MAX_BULK_MESSAGES) {
        return res.status(400).json({
            status: 'error',
            error: `Maximum ${MAX_BULK_MESSAGES} messages par envoi en masse (reçu : ${messages.length}).`
        });
    }

    const invalidIndex = messages.findIndex(m => !m || !(m.number || m.phone) || !(m.message || m.text));
    if (invalidIndex !== -1) {
        return res.status(400).json({
            status: 'error',
            error: `Message invalide à l'index ${invalidIndex} : "number" et "message" sont requis pour chaque entrée.`
        });
    }

    if (minDelaySeconds < 15) {
        return res.status(400).json({
            status: 'error',
            error: 'minDelaySeconds trop bas : un minimum de 15s est imposé pour limiter le risque de bannissement.'
        });
    }

    bulkJob = {
        id: Date.now().toString(),
        items: messages.map(m => ({ number: m.number || m.phone, message: m.message || m.text })),
        currentIndex: 0,
        sentCount: 0,
        failedCount: 0,
        results: [],
        status: 'pending',
        cancelled: false,
        minDelaySec: minDelaySeconds,
        maxDelaySec: maxDelaySeconds,
        startedAt: new Date().toISOString(),
        finishedAt: null,
        nextSendAt: null
    };

    // Estimation grossière du temps total (délai moyen * nb de pauses)
    const avgDelaySec = (minDelaySeconds + maxDelaySeconds) / 2;
    const estimatedSeconds = Math.round((bulkJob.items.length - 1) * avgDelaySec);

    // Démarrage en arrière-plan, sans bloquer la réponse HTTP
    processBulkJob();

    return res.status(202).json({
        status: 'accepted',
        message: `Envoi en masse lancé pour ${bulkJob.items.length} messages.`,
        job_id: bulkJob.id,
        delai_entre_messages: `${minDelaySeconds}-${maxDelaySeconds}s`,
        duree_estimee_minutes: Math.round(estimatedSeconds / 60),
        suivi: 'GET /bulk-status'
    });
});

// Route 5 : Suivre la progression de l'envoi en masse en cours
app.get('/bulk-status', (req, res) => {
    if (!bulkJob) {
        return res.json({ status: 'idle', message: 'Aucun envoi en masse lancé.' });
    }

    return res.json({
        job_id: bulkJob.id,
        status: bulkJob.status,
        total: bulkJob.items.length,
        envoyes: bulkJob.sentCount,
        echecs: bulkJob.failedCount,
        restants: bulkJob.items.length - (bulkJob.sentCount + bulkJob.failedCount),
        index_courant: bulkJob.currentIndex,
        prochain_envoi_a: bulkJob.nextSendAt ? new Date(bulkJob.nextSendAt).toISOString() : null,
        demarre_a: bulkJob.startedAt,
        termine_a: bulkJob.finishedAt
    });
});

// Route 6 : Annuler l'envoi en masse en cours (arrête avant le prochain message)
app.post('/bulk-cancel', (req, res) => {
    if (!bulkJob || bulkJob.status !== 'running' && bulkJob.status !== 'pending') {
        return res.status(400).json({ status: 'error', error: 'Aucun envoi en masse actif à annuler.' });
    }
    bulkJob.cancelled = true;
    return res.json({ status: 'success', message: 'Annulation demandée. Le job s\'arrêtera après le message en cours.' });
});

// Route 7 : Récupérer le détail complet des résultats du dernier job
app.get('/bulk-results', (req, res) => {
    if (!bulkJob) {
        return res.json({ status: 'idle', results: [] });
    }
    return res.json({
        job_id: bulkJob.id,
        status: bulkJob.status,
        results: bulkJob.results
    });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
