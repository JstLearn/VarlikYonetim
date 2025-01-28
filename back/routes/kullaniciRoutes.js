const express = require('express');
const router = express.Router();
const { addKullanici, validateKullanici, verifyEmail, forgotPassword, resetPassword } = require('../controllers/kullaniciController');

// POST /api/kullanicilar (Kayıt)
router.post('/', addKullanici);

// POST /api/kullanicilar/validate (Login)
router.post('/validate', validateKullanici);

// POST /api/kullanicilar/verify (E-posta doğrulama)
router.post('/verify', verifyEmail);

// POST /api/kullanicilar/forgot-password (Parola sıfırlama)
router.post('/forgot-password', forgotPassword);

// POST /api/kullanicilar/reset-password (Parola sıfırlama)
router.post('/reset-password', resetPassword);

module.exports = router; 