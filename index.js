const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');
const puppeteer = require('puppeteer');

const app = express();
app.use(express.json());

let isReady = false;
let currentQr = null;

const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
        executablePath: puppeteer.executablePath(),
        headless: true,
        args: [
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-dev-shm-usage',
            '--disable-accelerated-2d-canvas',
            '--no-first-run',
            '--no-zygote',
            '--disable-gpu',
            '--unhandled-rejections=strict'
        ]
    }
});

client.on('qr', (qr) => {
    isReady = false;
    currentQr = qr;
    console.log('--- NOUVEAU QR CODE GENERE ---');
    qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
    isReady = true;
    currentQr = null;
    console.log('Connecté à WhatsApp avec succès !');
});

client.on('authenticated', () => {
    console.log('Authentification réussie !');
});

client.on('auth_failure', (msg) => {
    isReady = false;
    console.error('Échec de l\'authentification :', msg);
});

client.on('disconnected', (reason) => {
    isReady = false;
    console.log('Client déconnecté :', reason);
    // Relance l'initialisation si déconnecté
    client.initialize();
});

// Route pour vérifier le statut de santé du serveur
app.get('/', (req, res) => {
    res.json({
        status: isReady ? 'connected' : 'connecting_or_waiting_qr',
        message: isReady ? 'Le bot WhatsApp est prêt !' : 'En attente de connexion WhatsApp...'
    });
});

// Route POST pour envoyer le message
app.post('/send-message', async (req, res) => {
    if (!isReady) {
        return res.status(503).json({ 
            status: 'error', 
            error: 'Le client WhatsApp n\'est pas encore prêt. Allez sur https://wa-bot-rlrx.onrender.com/ pour vérifier le statut.' 
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
        const formattedNumber = `${number}@c.us`;
        await client.sendMessage(formattedNumber, message);
        res.json({ status: 'success', message: 'Message envoyé !' });
    } catch (error) {
        console.error('Erreur lors de l\'envoi :', error);
        res.status(500).json({ status: 'error', error: error.message });
    }
});

client.initialize();

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
