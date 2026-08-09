const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');
const puppeteer = require('puppeteer');

const app = express();
app.use(express.json());

let isReady = false;

// Client WhatsApp avec Chromium ultra-optimisé pour les conteneurs à mémoire restreinte
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
            '--single-process', // Réduit fortement l'usage de la mémoire RAM
            '--disable-gpu'
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

client.on('disconnected', (reason) => {
    isReady = false;
    console.log('Client déconnecté :', reason);
});

app.post('/send-message', async (req, res) => {
    if (!isReady) {
        return res.status(503).json({ 
            status: 'error', 
            error: 'Le client WhatsApp est en cours d\'initialisation. Patientez quelques secondes.' 
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
        console.error('Erreur d\'envoi :', error);
        res.status(500).json({ status: 'error', error: error.message });
    }
});

client.initialize();

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
