// controllers/varlikController.js
const { sql, poolPromise } = require("../config/db");

// Varlık Ekleme
const addVarlik = async (req, res) => {
  try {
    const {
      varlik,
      tur,
      nerede,
      alis_tarihi,
      alis_fiyati,
      alis_adedi,
      simdi_fiyati_USD,
      kar_zarar,
      kar_zarar_yuzde,
      min_satis_fiyati_USD
    } = req.body;

    const kullanici = req.user.username;
    const tarih = new Date();

    // Gelen verileri kontrol et ve dönüştür
    const parsedData = {
      varlik: varlik || '',
      tur: tur || '',
      nerede: nerede || '',
      alis_tarihi: alis_tarihi ? new Date(alis_tarihi) : new Date(),
      alis_fiyati: parseFloat(alis_fiyati) || 0,
      alis_adedi: parseFloat(alis_adedi) || 0,
      simdi_fiyati_USD: parseFloat(simdi_fiyati_USD) || 0,
      kar_zarar: parseFloat(kar_zarar) || 0,
      kar_zarar_yuzde: parseFloat(kar_zarar_yuzde) || 0,
      min_satis_fiyati_USD: parseFloat(min_satis_fiyati_USD) || 0
    };

    const pool = await poolPromise;
    await pool
      .request()
      .input("kullanici", sql.VarChar(150), kullanici)
      .input("varlik", sql.VarChar(150), parsedData.varlik)
      .input("tur", sql.VarChar(150), parsedData.tur)
      .input("nerede", sql.VarChar(150), parsedData.nerede)
      .input("alis_tarihi", sql.DateTime, parsedData.alis_tarihi)
      .input("alis_fiyati", sql.Numeric(18, 8), parsedData.alis_fiyati)
      .input("alis_adedi", sql.Numeric(18, 8), parsedData.alis_adedi)
      .input("simdi_fiyati_USD", sql.Numeric(18, 8), parsedData.simdi_fiyati_USD)
      .input("kar_zarar", sql.Numeric(18, 8), parsedData.kar_zarar)
      .input("kar_zarar_yuzde", sql.Numeric(18, 8), parsedData.kar_zarar_yuzde)
      .input("min_satis_fiyati_USD", sql.Numeric(18, 8), parsedData.min_satis_fiyati_USD)
      .input("tarih", sql.DateTime, tarih)
      .query(`
        INSERT INTO [dbo].[varliklar]
          (kullanici, varlik, tur, nerede, alis_tarihi, alis_fiyati, alis_adedi,
           simdi_fiyati_USD, kar_zarar, kar_zarar_yuzde, min_satis_fiyati_USD, tarih)
        VALUES
          (@kullanici, @varlik, @tur, @nerede, @alis_tarihi, @alis_fiyati, @alis_adedi,
           @simdi_fiyati_USD, @kar_zarar, @kar_zarar_yuzde, @min_satis_fiyati_USD, @tarih)
      `);

    res.status(201).json({ 
      success: true,
      message: "Varlık başarıyla eklendi!",
      data: {
        varlik: parsedData.varlik,
        alis_fiyati: parsedData.alis_fiyati,
        alis_tarihi: parsedData.alis_tarihi,
        kullanici
      }
    });
  } catch (err) {
    console.error("Hata oluştu (varlık):", err);
    res.status(500).json({
      success: false,
      error: err.message || "Varlık eklenirken bir hata oluştu"
    });
  }
};

// Varlıkları Getirme
const getAllVarlik = async (req, res) => {
  try {
    const pool = await poolPromise;
    const result = await pool
      .request()
      .input('kullanici', sql.VarChar(150), req.user.username)
      .query("SELECT * FROM [dbo].[varliklar] WHERE kullanici = @kullanici");

    res.status(200).json({
      success: true,
      data: result.recordset
    });
  } catch (err) {
    console.error("Hata oluştu (get-all-varlik):", err);
    res.status(500).json({
      success: false,
      error: err.message || "Varlık verileri alınırken bir hata oluştu"
    });
  }
};

module.exports = {
  addVarlik,
  getAllVarlik,
};
