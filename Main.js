// Main.js
const express = require("express");
const sql = require("mssql");
const cors = require("cors");

const app = express();
const port = 3000;

// CORS'u etkinleştir
app.use(cors());

// JSON verisini okuyabilmek için
app.use(express.json());

// MSSQL bağlantı ayarları
const config = {
  user: "durak",
  password: "74108520963Xx..",
  server: "DENIZ",            // SQL Server instance adı veya IP'si
  database: "VARLıK_YONETIM", // Veritabanı adı
  options: {
    trustServerCertificate: true,
    enableArithAbort: true,
  },
};

// Basit GET testi
app.get("/", (req, res) => {
  res.send("Hoş geldiniz! Express.js sunucunuz çalışıyor.");
});

/* 
  1) BORÇ EKLE 
  Tablo: [dbo].[borçlar_giderler]
  Sütunlar:
    borç (varchar(150)), düzenlimi (bit), tutar (numeric(18,8)), 
    para_birimi (varchar(150)), kalan_taksit (int), ödeme_tarihi (datetimeoffset(2)), 
    faiz_binecekmi (bit), ödendi_mi (bit), talimat_varmı (bit), bağımlı_olduğu_gelir (varchar(150))
*/
app.post("/add-borc", async (req, res) => {
  const {
    borc,
    düzenlimi,
    tutar,
    para_birimi,
    kalan_taksit,
    ödeme_tarihi,
    faiz_binecekmi,
    ödendi_mi,
    talimat_varmı,
    bağımlı_olduğu_gelir,
  } = req.body;

  try {
    const pool = await sql.connect(config);

    await pool
      .request()
      .input("borc", sql.NVarChar, borc)
      .input("düzenlimi", sql.Bit, düzenlimi)
      .input("tutar", sql.Numeric(18, 8), tutar)
      .input("para_birimi", sql.NVarChar, para_birimi)
      .input("kalan_taksit", sql.Int, kalan_taksit)
      .input("ödeme_tarihi", sql.DateTimeOffset, ödeme_tarihi)
      .input("faiz_binecekmi", sql.Bit, faiz_binecekmi)
      .input("ödendi_mi", sql.Bit, ödendi_mi)
      .input("talimat_varmı", sql.Bit, talimat_varmı)
      .input("bağımlı_olduğu_gelir", sql.NVarChar, bağımlı_olduğu_gelir)
      .query(`
        INSERT INTO [dbo].[borçlar_giderler] 
          (borç, düzenlimi, tutar, para_birimi, kalan_taksit, ödeme_tarihi, 
           faiz_binecekmi, ödendi_mi, talimat_varmı, bağımlı_olduğu_gelir)
        VALUES 
          (@borc, @düzenlimi, @tutar, @para_birimi, @kalan_taksit, @ödeme_tarihi, 
           @faiz_binecekmi, @ödendi_mi, @talimat_varmı, @bağımlı_olduğu_gelir)
      `);

    pool.close();
    res.status(201).send({ message: "Borç başarıyla eklendi!" });
  } catch (err) {
    console.error("Hata oluştu (borç):", err);
    res.status(500).send({ error: "Borç eklenirken bir hata oluştu." });
  }
});

/*
  2) GİDER EKLE 
  Aynı tablo (borçlar_giderler)
  "Borç/Gider" alanını 'borc' kolonu gibi kullanıyoruz.
*/
app.post("/add-gider", async (req, res) => {
  const {
    borc,
    düzenlimi,
    tutar,
    para_birimi,
    kalan_taksit,
    ödeme_tarihi,
    faiz_binecekmi,
    ödendi_mi,
    talimat_varmı,
    bağımlı_olduğu_gelir,
  } = req.body;

  try {
    const pool = await sql.connect(config);

    await pool
      .request()
      .input("borc", sql.NVarChar, borc)
      .input("düzenlimi", sql.Bit, düzenlimi)
      .input("tutar", sql.Numeric(18, 8), tutar)
      .input("para_birimi", sql.NVarChar, para_birimi)
      .input("kalan_taksit", sql.Int, kalan_taksit)
      .input("ödeme_tarihi", sql.DateTimeOffset, ödeme_tarihi)
      .input("faiz_binecekmi", sql.Bit, faiz_binecekmi)
      .input("ödendi_mi", sql.Bit, ödendi_mi)
      .input("talimat_varmı", sql.Bit, talimat_varmı)
      .input("bağımlı_olduğu_gelir", sql.NVarChar, bağımlı_olduğu_gelir)
      .query(`
        INSERT INTO [dbo].[borçlar_giderler] 
          (borç, düzenlimi, tutar, para_birimi, kalan_taksit, ödeme_tarihi, 
           faiz_binecekmi, ödendi_mi, talimat_varmı, bağımlı_olduğu_gelir)
        VALUES 
          (@borc, @düzenlimi, @tutar, @para_birimi, @kalan_taksit, @ödeme_tarihi, 
           @faiz_binecekmi, @ödendi_mi, @talimat_varmı, @bağımlı_olduğu_gelir)
      `);

    pool.close();
    res.status(201).send({ message: "Gider başarıyla eklendi!" });
  } catch (err) {
    console.error("Hata oluştu (gider):", err);
    res.status(500).send({ error: "Gider eklenirken bir hata oluştu." });
  }
});

/*
  3) GELİR EKLE 
  Tablo: [dbo].[gelirler]
  Sütunlar:
    gelir (varchar(150)), düzenlimi (bit), tutar (numeric(18,8)), 
    para_birimi (varchar(150)), kalan_taksit (int), tahsilat_tarihi (datetimeoffset(2)),
    faiz_binecekmi (bit), alındı_mi (bit), talimat_varmı (bit), bağımlı_olduğu_gider (varchar(150))
*/
app.post("/add-gelir", async (req, res) => {
  const {
    gelir,
    düzenlimi,
    tutar,
    para_birimi,
    kalan_taksit,
    tahsilat_tarihi,
    faiz_binecekmi,
    alindi_mi,
    talimat_varmı,
    bağımlı_olduğu_gider,
  } = req.body;

  try {
    const pool = await sql.connect(config);

    await pool
      .request()
      .input("gelir", sql.NVarChar, gelir)
      .input("düzenlimi", sql.Bit, düzenlimi)
      .input("tutar", sql.Numeric(18, 8), tutar)
      .input("para_birimi", sql.NVarChar, para_birimi)
      .input("kalan_taksit", sql.Int, kalan_taksit)
      .input("tahsilat_tarihi", sql.DateTimeOffset, tahsilat_tarihi)
      .input("faiz_binecekmi", sql.Bit, faiz_binecekmi)
      .input("alindi_mi", sql.Bit, alindi_mi)
      .input("talimat_varmı", sql.Bit, talimat_varmı)
      .input("bağımlı_olduğu_gider", sql.NVarChar, bağımlı_olduğu_gider)
      .query(`
        INSERT INTO [dbo].[gelirler]
          (gelir, düzenlimi, tutar, para_birimi, kalan_taksit, tahsilat_tarihi,
           faiz_binecekmi, alındı_mi, talimat_varmı, bağımlı_olduğu_gider)
        VALUES
          (@gelir, @düzenlimi, @tutar, @para_birimi, @kalan_taksit, @tahsilat_tarihi,
           @faiz_binecekmi, @alindi_mi, @talimat_varmı, @bağımlı_olduğu_gider)
      `);

    pool.close();
    res.status(201).send({ message: "Gelir başarıyla eklendi!" });
  } catch (err) {
    console.error("Hata oluştu (gelir):", err);
    res.status(500).send({ error: "Gelir eklenirken bir hata oluştu." });
  }
});

/*
  4) VARLIK EKLE
  Tablo: [dbo].[varlıklar]
  Sütunlar:
    varlık (varchar(150)), tür (varchar(150)), nerede (varchar(150)), 
    alış_tarihi (datetimeoffset(2)), alış_fiyatı (numeric(18,8)), alış_adedi (numeric(18,8))
*/
app.post("/add-varlik", async (req, res) => {
  const {
    varlik,
    tur,
    nerede,
    alis_tarihi,
    alis_fiyati,
    alis_adedi,
  } = req.body;

  try {
    const pool = await sql.connect(config);

    await pool
      .request()
      .input("varlik", sql.NVarChar, varlik)
      .input("tur", sql.NVarChar, tur)
      .input("nerede", sql.NVarChar, nerede)
      .input("alis_tarihi", sql.DateTimeOffset, alis_tarihi)
      .input("alis_fiyati", sql.Numeric(18, 8), alis_fiyati)
      .input("alis_adedi", sql.Numeric(18, 8), alis_adedi)
      .query(`
        INSERT INTO [dbo].[varlıklar]
          (varlık, tür, nerede, alış_tarihi, alış_fiyatı, alış_adedi)
        VALUES
          (@varlik, @tur, @nerede, @alis_tarihi, @alis_fiyati, @alis_adedi)
      `);

    pool.close();
    res.status(201).send({ message: "Varlık başarıyla eklendi!" });
  } catch (err) {
    console.error("Hata oluştu (varlık):", err);
    res.status(500).send({ error: "Varlık eklenirken bir hata oluştu." });
  }
});

/* 
  5) VARLIK GETİR
*/
app.get("/get-all-varlik", async (req, res) => {
  try {
    const pool = await sql.connect(config);
    const result = await pool
      .request()
      .query("SELECT * FROM [dbo].[varlıklar]");

    pool.close();
    res.status(200).send(result.recordset);
  } catch (err) {
    console.error("Hata oluştu (get-all-varlik):", err);
    res.status(500).send({ error: "Varlıklar çekilirken bir hata oluştu." });
  }
});

/* 
  6) BORÇ GETİR
*/
app.get("/get-all-borc", async (req, res) => {
  try {
    const pool = await sql.connect(config);
    const result = await pool
      .request()
      .query("SELECT * FROM [dbo].[borçlar_giderler]");
    pool.close();

    res.status(200).send(result.recordset);
  } catch (err) {
    console.error("Hata oluştu (get-all-borc):", err);
    res.status(500).send({ error: "Borç verileri çekilirken bir hata oluştu." });
  }
});

/* 
  7) GELİR GETİR
*/
app.get("/get-all-gelir", async (req, res) => {
  try {
    const pool = await sql.connect(config);
    const result = await pool
      .request()
      .query("SELECT * FROM [dbo].[gelirler]");

    pool.close();
    res.status(200).send(result.recordset);
  } catch (err) {
    console.error("Hata oluştu (get-all-gelir):", err);
    res.status(500).send({ error: "Gelir verileri çekilirken bir hata oluştu." });
  }
});

/* 
  8) GİDER GETİR
*/
app.get("/get-all-gider", async (req, res) => {
  try {
    const pool = await sql.connect(config);
    const result = await pool
      .request()
      .query("SELECT * FROM [dbo].[borçlar_giderler]");

    pool.close();
    res.status(200).send(result.recordset);
  } catch (err) {
    console.error("Hata oluştu (get-all-gider):", err);
    res.status(500).send({ error: "Gider verileri çekilirken bir hata oluştu." });
  }
});

// Sunucuyu başlat
app.listen(port, () => {
  console.log(`Sunucu çalışıyor: http://localhost:${port}`);
});
