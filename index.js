const { Client, LocalAuth } = require('whatsapp-web.js');
const qrcode = require('qrcode-terminal');
const express = require('express');

const app = express();
app.use(express.json());

// Configuration du client WhatsApp avec arguments headless pour le Cloud
const client = new Client({
    authStrategy: new LocalAuth(),
    puppeteer: {
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

// Affichage du QR code dans le terminal/logs
client.on('qr', (qr) => {
    console.log('--- SCANNEZ CE QR CODE AVEC WHATSAPP ---');
    qrcode.generate(qr, { small: true });
});

// Confirmation de connexion
client.on('ready', () => {
    console.log('Connecté à WhatsApp avec succès !');
});

// Route POST pour envoyer un message
app.post('/send-message', async (req, res) => {
    const { number, message } = req.body;
    try {
        const formattedNumber = `${number}@c.us`;
        await client.sendMessage(formattedNumber, message);
        res.json({ status: 'success', message: 'Message envoyé !' });
    } catch (error) {
        res.status(500).json({ status: 'error', error: error.message });
    }
});

client.initialize();

// Gestion dynamique du port pour Render / Heroku / Local
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Serveur démarré sur le port ${PORT}`));