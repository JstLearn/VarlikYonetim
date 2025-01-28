const express = require('express');
const router = express.Router();
const sql = require('mssql');
const config = require('../config/db');

router.post('/', async (req, res) => {
  try {
    const { kullanici, sifre } = req.body;
    
    const pool = await sql.connect(config);
    
    const result = await pool.request()
      .input('kullanici', sql.VarChar(150), kullanici)
      .input('sifre', sql.VarChar(150), sifre)
      .input('onaylandi', sql.Bit, false)
      .query(`
        INSERT INTO [VARLIK_YONETIM].[dbo].[kullanicilar]
        (kullanici, sifre, onaylandi)
        VALUES
        (@kullanici, @sifre, @onaylandi)
      `);
      
    res.json({ success: true, message: 'Kullanıcı başarıyla eklendi' });
  } catch (error) {
    console.error('Kullanıcı ekleme hatası:', error);
    res.status(500).json({ success: false, message: 'Kullanıcı eklenirken bir hata oluştu' });
  }
});

module.exports = router; 