const { sql, poolPromise } = require('../config/db');

// Gider ekleme
const addGider = async (req, res) => {
    try {
        const { 
            gider, 
            duzenlimi, 
            tutar, 
            para_birimi, 
            kalan_taksit, 
            odeme_tarihi, 
            faiz_binecekmi, 
            odendi_mi, 
            talimat_varmi, 
            bagimli_oldugu_gelir 
        } = req.body;
        
        const kullanici = req.user.username;
        const tarih = new Date();

        const pool = await poolPromise;
        await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .input('borc', sql.VarChar(150), gider)
            .input('duzenlimi', sql.Bit, duzenlimi)
            .input('tutar', sql.Decimal(18,2), tutar)
            .input('para_birimi', sql.VarChar(10), para_birimi)
            .input('kalan_taksit', sql.Int, kalan_taksit)
            .input('odeme_tarihi', sql.DateTime, odeme_tarihi)
            .input('faiz_binecekmi', sql.Bit, faiz_binecekmi)
            .input('odendi_mi', sql.Bit, odendi_mi)
            .input('talimat_varmi', sql.Bit, talimat_varmi)
            .input('bagimli_oldugu_gelir', sql.VarChar(150), bagimli_oldugu_gelir)
            .input('tarih', sql.DateTime, tarih)
            .query(`
                INSERT INTO borclar_giderler (
                    kullanici, borc, duzenlimi, tutar, para_birimi, kalan_taksit,
                    odeme_tarihi, faiz_binecekmi, odendi_mi, talimat_varmi, bagimli_oldugu_gelir, tarih
                )
                VALUES (
                    @kullanici, @borc, @duzenlimi, @tutar, @para_birimi, @kalan_taksit,
                    @odeme_tarihi, @faiz_binecekmi, @odendi_mi, @talimat_varmi, @bagimli_oldugu_gelir, @tarih
                )
            `);

        res.status(201).json({
            success: true,
            message: 'Gider başarıyla eklendi',
            data: {
                gider: gider,
                tutar: tutar,
                odeme_tarihi: odeme_tarihi,
                kullanici
            }
        });
    } catch (error) {
        console.error('SQL Hatası:', error);
        res.status(500).json({
            success: false,
            error: error.message || 'Gider eklenirken bir hata oluştu'
        });
    }
};

// Tüm giderleri getirme
const getAllGider = async (req, res) => {
    try {
        const pool = await poolPromise;
        const result = await pool.request()
            .input('kullanici', sql.VarChar(150), req.user.username)
            .query('SELECT * FROM borclar_giderler WHERE kullanici = @kullanici');
        
        res.status(200).json({
            success: true,
            data: result.recordset
        });
    } catch (error) {
        console.error('SQL Hatası:', error);
        res.status(500).json({
            success: false,
            error: error.message || 'Gider verileri alınırken bir hata oluştu'
        });
    }
};

module.exports = {
    addGider,
    getAllGider
}; 