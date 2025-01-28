const sql = require("mssql");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../../.env") });

// Config değerlerini kontrol et
console.log("DB Config:", {
  user: process.env.DB_USER,
  server: process.env.DB_SERVER,
  database: "VARLIK_YONETIM" // Türkçe karakter düzeltildi
});

const config = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: "VARLIK_YONETIM", // Türkçe karakter düzeltildi
  options: {
    trustServerCertificate: true,
    enableArithAbort: true,
  },
};

const poolPromise = new sql.ConnectionPool(config)
  .connect()
  .then((pool) => {
    return pool;
  })
  .catch((err) => {
    console.error("Veritabanı bağlantı hatası:", err);
    process.exit(1);
  });

module.exports = {
  sql,
  poolPromise,
};
