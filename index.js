const { default: makeWASocket, useMultiFileAuthState, DisconnectReason } = require('@whiskeysockets/baileys');
const qrcodeTerminal = require('qrcode-terminal');
const QRCode = require('qrcode');
const express = require('express');

const app = express();
app.use(express.json());

let sock;
let isReady = false;
let currentQR = null;

async function connectToWhatsApp() {
    // Sauvegarde la session dans le dossier auth_info_baileys
    const { state, saveCreds } = await useMultiFileAuthState('auth_info_baileys');

    sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        browser: ["Ecole Marie Auxiliatrice", "Chrome", "1.0.0"]
    });

    sock.ev.on('creds.update', saveCreds);

    sock.ev.on('connection.update', (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
            isReady = false;
            currentQR = qr;
            console.log('--- SCANNEZ CE QR CODE AVEC WHATSAPP ---');
            qrcodeTerminal.generate(qr, { small: true });
        }

        if (connection === 'close') {
            isReady = false;
            currentQR = null;
            const statusCode = lastDisconnect?.error?.output?.statusCode;
            const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
            console.log('Connexion fermée. Reconnexion automatique...', shouldReconnect);
            if (shouldReconnect) {
                setTimeout(connectToWhatsApp, 3000);
            }
        } else if (connection === 'open') {
            isReady = true;
            currentQR = null;
            console.log('Connecté à WhatsApp avec succès via Baileys !');
        }
    });
}

connectToWhatsApp();

// Route d'accueil : Vérification de l'état
app.get('/', (req, res) => {
    res.json({
        status: isReady ? 'connected' : 'waiting_qr',
        message: isReady ? 'Le bot WhatsApp est prêt !' : 'En attente du scan du QR Code...'
    });
});

// Route visuelle pour afficher et scanner le QR Code dans le navigateur
app.get('/qr', async (req, res) => {
    if (isReady) {
        return res.send('<h2>✅ WhatsApp est déjà connecté !</h2>');
    }
    if (!currentQR) {
        return res.send('<h2>⏳ Génération du QR Code en cours... Veuillez rafraîchir dans 5 secondes.</h2>');
    }
    try {
        const qrImage = await QRCode.toDataURL(currentQR);
        res.send(`
            <html>
                <body style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100vh;font-family:sans-serif;">
                    <h2>Scannez ce QR Code avec WhatsApp</h2>
                    <img src="${qrImage}" style="width:300px;height:300px;"/>
                    <p>Ouvrez WhatsApp > Appareils connectés > Lier un appareil</p>
                </body>
            </html>
        `);
    } catch (err) {
        res.status(500).send('Erreur lors de la génération du QR Code');
    }
});

// Route d'envoi de message
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
        // 1. Nettoyage strict du numéro (seuls les chiffres sont conservés)
        let cleanNumber = String(rawNumber).replace(/[^0-9]/g, '');

        // 2. Traitement spécifique du format Bénin (+229)
        // Si le numéro fait 10 chiffres et commence par 01 (ex: 0161666899) -> 2290161666899
        if (cleanNumber.length === 10 && cleanNumber.startsWith('01')) {
            cleanNumber = '229' + cleanNumber;
        } 
        // Si le numéro fait 8 chiffres (ex: 61666899) -> 22961666899
        else if (cleanNumber.length === 8) {
            cleanNumber = '229' + cleanNumber;
        }

        // 3. Obtenir l'identifiant WhatsApp JID exact (sur le réseau WhatsApp)
        const recipientJid = `${cleanNumber}@s.whatsapp.net`;
        
        // Vérification si le numéro existe bien sur WhatsApp
        const [exists] = await sock.onWhatsApp(recipientJid);
        
        const targetJid = exists ? exists.jid : recipientJid;

        // 4. Envoi du message texte
        const result = await sock.sendMessage(targetJid, { text: message });

        console.log(`Message envoyé avec succès à ${targetJid}`);
        return res.json({ 
            status: 'success', 
            message: 'Message envoyé !',
            jid_destinataire: targetJid,
            id_message: result.key.id
        });

    } catch (error) {
        console.error('Erreur d\'envoi :', error);
        return res.status(500).json({ status: 'error', error: error.message });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
