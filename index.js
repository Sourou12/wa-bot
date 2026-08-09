const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const qrcode = require('qrcode-terminal');
const express = require('express');

const app = express();
app.use(express.json());

let sock;
let isReady = false;

async function connectToWhatsApp() {
    // Sauvegarde la session dans le dossier auth_info_baileys
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');

    sock = makeWASocket({
        auth: state,
        printQRInTerminal: false
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            isReady = false;
            console.log('--- SCANNEZ CE QR CODE AVEC WHATSAPP ---');
            qrcode.generate(qr, { small: true });
        }

        if (connection === 'close') {
            isReady = false;
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
            console.log('Connexion fermée. Reconnexion automatique...', shouldReconnect);
            if (shouldReconnect) {
                connectToWhatsApp();
            }
        } else if (connection === 'open') {
            isReady = true;
            console.log('Connecté à WhatsApp avec succès via Baileys !');
        }
    });
}

connectToWhatsApp();

// Vérification de l'état
app.get('/', (req, res) => {
    res.json({
        status: isReady ? 'connected' : 'waiting_qr',
        message: isReady ? 'Le bot WhatsApp est prêt !' : 'En attente du scan du QR Code...'
    });
});

// Route d'envoi de message
app.post('/send-message', async (req, res) => {
    if (!isReady) {
        return res.status(503).json({
            status: 'error',
            error: 'Le client WhatsApp n\'est pas encore connecté.'
        });
    }

    const { number, message } = req.body;

    if (!number || !message) {
        return res.status(400).json({
            status: 'error',
            error: 'Les champs "number" et "message" sont requis.'
        });
    }

    try {
        // Formatage du numéro pour Baileys
        const cleanNumber = number.replace('+', '').replace(/\s+/g, '');
        const formattedNumber = `${cleanNumber}@s.whatsapp.net`;
        
        await sock.sendMessage(formattedNumber, { text: message });
        res.json({ status: 'success', message: 'Message envoyé !' });
    } catch (error) {
        console.error('Erreur d\'envoi :', error);
        res.status(500).json({ status: 'error', error: error.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
