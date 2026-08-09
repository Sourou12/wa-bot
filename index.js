const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const express = require('express');

const app = express();
app.use(express.json());

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

// Route 3 : Envoi de message WhatsApp
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
        // Nettoyage et formatage Bénin (+229)
        let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');

        if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) {
            cleanNumber = '229' + cleanNumber;
        } else if (cleanNumber.length === 8) {
            cleanNumber = '229' + cleanNumber;
        }

        const recipientJid = `${cleanNumber}@s.whatsapp.net`;

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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
