// back/controllers/borcController.js
const { sql, poolPromise } = require("../config/db");

// Borç/Gider Ekleme
const addBorc = async (req, res) => {
  try {
    let {
      borc,
      duzenlimi,
      tutar,
      para_birimi = "TRY",
      kalan_taksit = 1,
      odeme_tarihi,
      faiz_binecekmi,
      odendi_mi,
      talimat_varmi,
      bagimli_oldugu_gelir = null
    } = req.body;

    const kullanici = req.user.username;

    const pool = await poolPromise;

    // Veriyi ekle
    await pool
      .request()
      .input("kullanici", sql.VarChar(150), kullanici)
      .input("borc", sql.VarChar(150), borc)
      .input("duzenlimi", sql.Bit, duzenlimi)
      .input("tutar", sql.Numeric(18, 2), tutar)
      .input("para_birimi", sql.VarChar(10), para_birimi)
      .input("kalan_taksit", sql.Int, kalan_taksit)
      .input("odeme_tarihi", sql.DateTime, odeme_tarihi)
      .input("faiz_binecekmi", sql.Bit, faiz_binecekmi)
      .input("odendi_mi", sql.Bit, odendi_mi)
      .input("talimat_varmi", sql.Bit, talimat_varmi)
      .input("bagimli_oldugu_gelir", sql.VarChar(150), bagimli_oldugu_gelir)
      .query(`
        INSERT INTO [dbo].[borclar_giderler]
          (kullanici, borc, duzenlimi, tutar, para_birimi, kalan_taksit,
           odeme_tarihi, faiz_binecekmi, odendi_mi, talimat_varmi, bagimli_oldugu_gelir)
        VALUES
          (@kullanici, @borc, @duzenlimi, @tutar, @para_birimi, @kalan_taksit,
           @odeme_tarihi, @faiz_binecekmi, @odendi_mi, @talimat_varmi, @bagimli_oldugu_gelir)
      `);

    res.status(201).json({ 
      success: true,
      message: "Borç/Gider başarıyla eklendi!",
      data: {
        borc: borc,
        tutar: tutar,
        odeme_tarihi: odeme_tarihi,
        kullanici
      }
    });
  } catch (err) {
    console.error("SQL Hatası (borc/gider):", err);
    res.status(500).json({
      success: false,
      error: err.message || "Borç/Gider eklenirken bir hata oluştu"
    });
  }
};

// Borçları/Giderleri Getirme
const getAllBorc = async (req, res) => {
  try {
    const pool = await poolPromise;
    const result = await pool
      .request()
      .input('kullanici', sql.VarChar(150), req.user.username)
      .query('SELECT * FROM [dbo].[borclar_giderler] WHERE kullanici = @kullanici');

    res.status(200).json({
      success: true,
      data: result.recordset
    });
  } catch (err) {
    console.error("SQL Hatası (get-all-borc):", err);
    res.status(500).json({
      success: false,
      error: err.message || "Borç/Gider verileri alınırken bir hata oluştu"
    });
  }
};

module.exports = {
  addBorc,
  getAllBorc,
};
