const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');
const puppeteer = require('puppeteer');

const app = express();
app.use(express.json());

// Variable globale pour suivre l'état de la connexion WhatsApp
let isReady = false;

// Configuration du client WhatsApp avec Puppeteer adapté au Cloud
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
            '--disable-gpu'
        ]
    }
});

// Affichage du QR Code dans la console / logs Render
client.on('qr', (qr) => {
    isReady = false;
    console.log('--- SCANNEZ CE QR CODE AVEC WHATSAPP ---');
    qrcode.generate(qr, { small: true });
});

// Déclenché lorsque le client est prêt à envoyer des messages
client.on('ready', () => {
    isReady = true;
    console.log('Connecté à WhatsApp avec succès !');
});

// En cas de déconnexion de l'appareil
client.on('disconnected', (reason) => {
    isReady = false;
    console.log('Client déconnecté :', reason);
});

// Route POST pour envoyer un message
app.post('/send-message', async (req, res) => {
    // Vérification que le client est bien connecté avant de traiter la requête
    if (!isReady) {
        return res.status(503).json({ 
            status: 'error', 
            error: 'Le client WhatsApp est en cours d\'initialisation ou déconnecté. Vérifiez les logs Render ou réessayez dans quelques secondes.' 
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
        console.error('Erreur lors de l\'envoi du message :', error);
        res.status(500).json({ status: 'error', error: error.message });
    }
});

// Initialisation du client WhatsApp
client.initialize();

// Port dynamique pour Render (fallback sur 3000 en local)
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));
