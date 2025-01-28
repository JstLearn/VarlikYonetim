// controllers/gelirController.js
const { sql, poolPromise } = require("../config/db");

// Gelir Ekleme
const addGelir = async (req, res) => {
  try {
    const {
      gelir,
      duzenlimi,
      tutar,
      para_birimi,
      kalan_taksit,
      tahsilat_tarihi,
      faiz_binecekmi,
      alindi_mi,
      talimat_varmi,
      bagimli_oldugu_gider
    } = req.body;

    const kullanici = req.user.username;
    const tarih = new Date();

    const pool = await poolPromise;

    // Veriyi ekle
    await pool
      .request()
      .input("kullanici", sql.VarChar(150), kullanici)
      .input("gelir", sql.VarChar(150), gelir)
      .input("duzenlimi", sql.Bit, duzenlimi)
      .input("tutar", sql.Numeric(18, 2), tutar)
      .input("para_birimi", sql.VarChar(10), para_birimi)
      .input("kalan_taksit", sql.Int, kalan_taksit)
      .input("tahsilat_tarihi", sql.DateTime, tahsilat_tarihi)
      .input("faiz_binecekmi", sql.Bit, faiz_binecekmi)
      .input("alindi_mi", sql.Bit, alindi_mi)
      .input("talimat_varmi", sql.Bit, talimat_varmi)
      .input("bagimli_oldugu_gider", sql.VarChar(150), bagimli_oldugu_gider)
      .input("tarih", sql.DateTime, tarih)
      .query(`
        INSERT INTO [dbo].[gelirler]
          (kullanici, gelir, duzenlimi, tutar, para_birimi, kalan_taksit, 
           tahsilat_tarihi, faiz_binecekmi, alindi_mi, talimat_varmi, bagimli_oldugu_gider, tarih)
        VALUES
          (@kullanici, @gelir, @duzenlimi, @tutar, @para_birimi, @kalan_taksit,
           @tahsilat_tarihi, @faiz_binecekmi, @alindi_mi, @talimat_varmi, @bagimli_oldugu_gider, @tarih)
      `);

    res.status(201).json({ 
      success: true,
      message: "Gelir başarıyla eklendi!",
      data: {
        gelir: gelir,
        tutar: tutar,
        tahsilat_tarihi: tahsilat_tarihi,
        kullanici
      }
    });
  } catch (err) {
    console.error("SQL Hatası (gelir):", err);
    res.status(500).json({
      success: false,
      error: err.message || "Gelir eklenirken bir hata oluştu"
    });
  }
};

// Gelirleri Getirme
const getAllGelir = async (req, res) => {
  try {
    const pool = await poolPromise;
    const result = await pool
      .request()
      .input('kullanici', sql.VarChar(150), req.user.username)
      .query("SELECT * FROM [dbo].[gelirler] WHERE kullanici = @kullanici");

    res.status(200).json({
      success: true,
      data: result.recordset
    });
  } catch (err) {
    console.error("SQL Hatası (get-all-gelir):", err);
    res.status(500).json({
      success: false,
      error: err.message || "Gelir verileri alınırken bir hata oluştu"
    });
  }
};

module.exports = {
  addGelir,
  getAllGelir
};
