const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');
const puppeteer = require('puppeteer');

const app = express();
app.use(express.json());

let isReady = false;

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
            '--disable-software-rasterizer',
            '--disable-extensions',
            '--mute-audio',
            '--no-default-browser-check',
            '--disable-background-networking',
            '--disable-background-timer-throttling',
            '--disable-backgrounding-occluded-windows',
            '--disable-breakpad',
            '--disable-component-extensions-with-background-pages',
            '--disable-features=Translate,BackForwardCache,AcceptCHFrame,MediaRouter,OptimizationHints',
            '--disable-ipc-flooding-protection',
            '--disable-renderer-backgrounding'
        ]
    }
});

client.on('qr', (qr) => {
    isReady = false;
    console.log('--- SCANNEZ CE QR CODE AVEC WHATSAPP ---');
    qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
    isReady = true;
    console.log('Connecté à WhatsApp avec succès !');
});

client.on('authenticated', () => {
    console.log('Authentification réussie !');
});

client.on('disconnected', (reason) => {
    isReady = false;
    console.log('Client déconnecté :', reason);
});

// Route de statut de santé
app.get('/', (req, res) => {
    res.json({
        status: isReady ? 'connected' : 'waiting_qr_or_loading',
        message: isReady ? 'Le bot WhatsApp est prêt !' : 'En attente de connexion WhatsApp...'
    });
});

// Route d'envoi de message
app.post('/send-message', async (req, res) => {
    if (!isReady) {
        return res.status(503).json({ 
            status: 'error', 
            error: 'Le client WhatsApp n\'est pas encore prêt. Vérifiez https://wa-bot-rlrx.onrender.com/' 
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
