// back/Main.js
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, "../../.env") });
const express = require('express');
const cors = require('cors');
const { poolPromise } = require('./config/db');
const varlikRoutes = require('./routes/varlikRoutes');
const gelirRoutes = require('./routes/gelirRoutes');
const borcRoutes = require('./routes/borcRoutes');
const giderRoutes = require('./routes/giderRoutes');
const kullaniciRoutes = require('./routes/kullaniciRoutes');

const app = express();

// CORS ayarları
app.use(cors({
    origin: 'http://localhost:8080',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));

// Middleware
app.use(express.json());

// Veritabanı bağlantısını kontrol et
poolPromise.then(() => {
    console.log('Veritabanı bağlantısı başarılı ve hazır');
}).catch(err => {
    console.error('Veritabanı bağlantı hatası:', err);
    process.exit(1);
});

// Routes
app.use('/api/kullanicilar', kullaniciRoutes);
app.use('/api/varlik', varlikRoutes);
app.use('/api/gelir', gelirRoutes);
app.use('/api/borc', borcRoutes);
app.use('/api/gider', giderRoutes);

// 404 handler
app.use((req, res) => {
    console.log('404 - Endpoint bulunamadı:', req.method, req.url);
    res.status(404).json({
        success: false,
        error: `Endpoint bulunamadı: ${req.method} ${req.url}`
    });
});

// Error handler
app.use((err, req, res, next) => {
    console.error('Hata:', err);
    res.status(500).json({
        success: false,
        error: 'Sunucu hatası'
    });
});

// Sunucuyu başlat
const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || '0.0.0.0'; // Tüm IP'lerden erişime izin ver

app.listen(PORT, HOST, () => {
    console.log(`\nSunucu http://${HOST}:${PORT} adresinde çalışıyor\n`);
    console.log('Kullanılabilir endpoint\'ler:');
    console.log('- POST /api/kullanicilar');
    console.log('- POST /api/kullanicilar/validate');
    console.log('- POST /api/kullanicilar/verify');
    console.log('- POST /api/kullanicilar/forgot-password');
    console.log('- POST /api/kullanicilar/reset-password');
    console.log('- GET  /api/varlik');
    console.log('- POST /api/varlik');
    console.log('- GET  /api/gelir');
    console.log('- POST /api/gelir');
    console.log('- GET  /api/borc');
    console.log('- POST /api/borc');
    console.log('- GET  /api/gider');
    console.log('- POST /api/gider');
});
