{
  "info": "Bu dosya, projenin SQL Server veritabanındaki tabloların güncel şemasını içermektedir. Son güncelleme tarihi: 2024-04-12",
  "tables": [
    {
      "tableName": "pariteler",
      "purpose": "Borsa ve diğer piyasa paritelerini, özellikleri ile saklar.",
      "columns": [
        {
          "columnName": "ID",
          "dataType": "int",
          "allowNull": false,
          "description": "Parite kaydı için benzersiz kimlik (Primary Key)."
        },
        {
          "columnName": "parite",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "Parite adı (örn: BTC/USDT)."
        },
        {
          "columnName": "borsa",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "Paritenin işlem gördüğü borsa (örn: BINANCE, COMMODITY)."
        },
        {
          "columnName": "tip",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "Paritenin tipi (örn: SPOT, FUTURES, COMMODITY)."
        },
        {
          "columnName": "ulke",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "İlgili ülke bilgisi."
        },
        {
          "columnName": "aciklama",
          "dataType": "nvarchar(500)",
          "allowNull": true,
          "description": "Parite hakkında açıklayıcı bilgi."
        },
        {
          "columnName": "aktif",
          "dataType": "bit",
          "allowNull": true,
          "description": "Paritenin aktif olup olmadığını belirtir."
        },
        {
          "columnName": "veri_var",
          "dataType": "bit",
          "allowNull": true,
          "description": "Parite için veri var mı yok mu."
        },
        {
          "columnName": "veriler_guncel",
          "dataType": "bit",
          "allowNull": true,
          "description": "Parite verilerinin güncellik durumu."
        },
        {
          "columnName": "kayit_tarihi",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kaydın oluşturulma tarihi (varsayılan GETDATE())."
        }
      ]
    },
    {
      "tableName": "kurlar",
      "purpose": "Döviz ve diğer kur çiftlerini saklar.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Kur kaydı için benzersiz kimlik (Primary Key)."
        },
        {
          "columnName": "parite",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "Kur çiftinin adı (örn: USD/TRY)."
        },
        {
          "columnName": "interval",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "Veri aralığı (örn: günlük, saatlik)."
        },
        {
          "columnName": "borsa",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "Kurun işlem gördüğü borsa."
        },
        {
          "columnName": "tip",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "Kur türü (örn: Döviz, Kripto)."
        },
        {
          "columnName": "ulke",
          "dataType": "nvarchar(50)",
          "allowNull": true,
          "description": "İlgili ülke."
        },
        {
          "columnName": "fiyat",
          "dataType": "decimal(18,8)",
          "allowNull": true,
          "description": "Kapanış fiyatı."
        },
        {
          "columnName": "dolar_karsiligi",
          "dataType": "decimal(18,8)",
          "allowNull": true,
          "description": "1 birimin USD cinsinden karşılığı."
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Verinin ait olduğu tarih."
        },
        {
          "columnName": "kayit_tarihi",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kaydın sisteme işlendiği tarih (varsayılan GETDATE())."
        }
      ]
    },
    {
      "tableName": "varliklar",
      "purpose": "Kullanıcıların varlık işlemlerini ayrıntılı olarak takip eder.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Varlık için benzersiz kimlik (Primary Key)."
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar(150)",
          "allowNull": false,
          "description": "Kullanıcıya ait ID veya ad."
        },
        {
          "columnName": "varlik",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Varlığın adı."
        },
        {
          "columnName": "tur",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Varlık tipi (örn: dijital, gayrimenkul)."
        },
        {
          "columnName": "nerede",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Varlığın bulunduğu yer (örn: borsa, cüzdan)."
        },
        {
          "columnName": "alis_tarihi",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Varlığın alım tarihi."
        },
        {
          "columnName": "alis_fiyati",
          "dataType": "numeric(18,8)",
          "allowNull": true,
          "description": "Alım fiyatı."
        },
        {
          "columnName": "alis_adedi",
          "dataType": "numeric(18,8)",
          "allowNull": true,
          "description": "Alınan miktar."
        },
        {
          "columnName": "simdi_fiyati_USD",
          "dataType": "numeric(18,8)",
          "allowNull": true,
          "description": "Güncel fiyat (USD cinsinden)."
        },
        {
          "columnName": "kar_zarar",
          "dataType": "numeric(18,8)",
          "allowNull": true,
          "description": "Toplam kâr/zarar (USD bazında)."
        },
        {
          "columnName": "kar_zarar_yuzde",
          "dataType": "numeric(18,8)",
          "allowNull": true,
          "description": "Yüzdelik kâr/zarar oranı."
        },
        {
          "columnName": "min_satis_fiyati_USD",
          "dataType": "numeric(18,8)",
          "allowNull": true,
          "description": "Minimum satış fiyatı (USD)."
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kayıt tarihi (varsayılan GETDATE())."
        }
      ]
    },
    {
      "tableName": "borclar_giderler",
      "purpose": "Kullanıcıların borç ve gider bilgilerini tutar.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Borcun benzersiz ID'si (Primary Key)."
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar(150)",
          "allowNull": false,
          "description": "Kullanıcıya ait ID veya ad."
        },
        {
          "columnName": "borc",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Borç tanımı (örn: kredi, fatura)."
        },
        {
          "columnName": "duzenlimi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Borç düzenli mi."
        },
        {
          "columnName": "tutar",
          "dataType": "numeric(18,2)",
          "allowNull": true,
          "description": "Toplam borç tutarı."
        },
        {
          "columnName": "para_birimi",
          "dataType": "varchar(10)",
          "allowNull": true,
          "description": "Borç para birimi (örn: TRY, USD)."
        },
        {
          "columnName": "kalan_taksit",
          "dataType": "int",
          "allowNull": true,
          "description": "Kalan taksit sayısı."
        },
        {
          "columnName": "odeme_tarihi",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Son ödeme tarihi."
        },
        {
          "columnName": "faiz_binecekmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Faizin uygulanıp uygulanmayacağı."
        },
        {
          "columnName": "odendi_mi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Borç ödenmiş mi."
        },
        {
          "columnName": "talimat_varmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Otomatik ödeme talimatı var mı."
        },
        {
          "columnName": "bagimli_oldugu_gelir",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Borçla ilişkili gelir kaydı."
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kayıt tarihi (varsayılan GETDATE())."
        }
      ]
    },
    {
      "tableName": "gelirler",
      "purpose": "Kullanıcıların gelir kayıtlarını ayrıntılı olarak saklar.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Gelir için benzersiz ID (Primary Key)."
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar(150)",
          "allowNull": false,
          "description": "Kullanıcıya ait ID veya ad."
        },
        {
          "columnName": "gelir",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Gelir tanımı (örn: maaş, kira)."
        },
        {
          "columnName": "duzenlimi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Gelirin düzenli ödeme olup olmadığı."
        },
        {
          "columnName": "tutar",
          "dataType": "numeric(18,2)",
          "allowNull": true,
          "description": "Gelir tutarı."
        },
        {
          "columnName": "para_birimi",
          "dataType": "varchar(10)",
          "allowNull": true,
          "description": "Gelirin para birimi (örn: TRY, USD)."
        },
        {
          "columnName": "kalan_taksit",
          "dataType": "int",
          "allowNull": true,
          "description": "Taksitli ödemelerde kalan taksit sayısı."
        },
        {
          "columnName": "tahsilat_tarihi",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Gelirin tahsil edildiği tarih."
        },
        {
          "columnName": "faiz_binecekmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Faiz getirisi olup olmayacağı."
        },
        {
          "columnName": "alindi_mi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Gelirin alınmış olup olmadığı."
        },
        {
          "columnName": "talimat_varmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Otomatik tahsilat talimatı var mı."
        },
        {
          "columnName": "bagimli_oldugu_gider",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Gelirle ilişkili gider kaydı."
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kayıt tarihi (varsayılan GETDATE())."
        }
      ]
    },
    {
      "tableName": "kullanicilar",
      "purpose": "Kullanıcı hesap bilgilerini ve doğrulama durumlarını tutar.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Kullanıcı için benzersiz ID (Primary Key)."
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Kullanıcının adı."
        },
        {
          "columnName": "sifre",
          "dataType": "varchar(150)",
          "allowNull": true,
          "description": "Şifrelenmiş kullanıcı parolası."
        },
        {
          "columnName": "tarih",
          "dataType": "date",
          "allowNull": false,
          "description": "Hesabın oluşturulma tarihi (varsayılan GETDATE())."
        },
        {
          "columnName": "onaylandi",
          "dataType": "bit",
          "allowNull": false,
          "description": "Hesabın onay durumu."
        },
        {
          "columnName": "verification_token",
          "dataType": "varchar(50)",
          "allowNull": true,
          "description": "Hesap doğrulama token'ı."
        }
      ]
    },
    {
      "tableName": "istekler",
      "purpose": "Kullanıcı istek ve önerilerini takip eder.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "int",
          "allowNull": true,
          "description": "İstek kaydı için benzersiz ID."
        },
        {
          "columnName": "kullanici",
          "dataType": "nchar(50)",
          "allowNull": true,
          "description": "İsteği yapan kullanıcı."
        },
        {
          "columnName": "istekler",
          "dataType": "nchar(500)",
          "allowNull": true,
          "description": "İstek veya öneri içeriği."
        },
        {
          "columnName": "oncelik",
          "dataType": "nchar(50)",
          "allowNull": true,
          "description": "İsteğin öncelik seviyesi."
        },
        {
          "columnName": "date",
          "dataType": "datetime",
          "allowNull": true,
          "description": "İsteğin oluşturulma tarihi (varsayılan GETDATE())."
        }
      ]
    }
  ],
  "note": "Bu dosya veritabanı şemasının en güncel halini içermektedir. .env dosyasındaki veritabanı bağlantı ayarları (DB_SERVER, DB_DATABASE, DB_USER, DB_PASSWORD) kullanılarak sorgulanmıştır."
}













VERİTABANINI BAŞTAN OLUŞTURMAK


-- Veritabanını oluştur
CREATE DATABASE VARLIK_YONETIM;
GO

USE VARLIK_YONETIM;
GO

-- Pariteler tablosu
CREATE TABLE pariteler (
    ID int IDENTITY(1,1) PRIMARY KEY,
    parite nvarchar(50),
    borsa nvarchar(50),
    tip nvarchar(50),
    ulke nvarchar(50),
    aciklama nvarchar(500),
    aktif bit,
    veri_var bit,
    veriler_guncel bit,
    kayit_tarihi datetime NOT NULL DEFAULT GETDATE()
);

-- Kurlar tablosu
CREATE TABLE kurlar (
    id bigint IDENTITY(1,1) PRIMARY KEY,
    parite nvarchar(50),
    [interval] nvarchar(50),
    borsa nvarchar(50),
    tip nvarchar(50),
    ulke nvarchar(50),
    fiyat decimal(18,8),
    dolar_karsiligi decimal(18,8),
    tarih datetime,
    kayit_tarihi datetime NOT NULL DEFAULT GETDATE()
);

-- Varlıklar tablosu
CREATE TABLE varliklar (
    id bigint IDENTITY(1,1) PRIMARY KEY,
    kullanici varchar(150) NOT NULL,
    varlik varchar(150),
    tur varchar(150),
    nerede varchar(150),
    alis_tarihi datetime,
    alis_fiyati numeric(18,8),
    alis_adedi numeric(18,8),
    simdi_fiyati_USD numeric(18,8),
    kar_zarar numeric(18,8),
    kar_zarar_yuzde numeric(18,8),
    min_satis_fiyati_USD numeric(18,8),
    tarih datetime NOT NULL DEFAULT GETDATE()
);

-- Borçlar ve Giderler tablosu
CREATE TABLE borclar_giderler (
    id bigint IDENTITY(1,1) PRIMARY KEY,
    kullanici varchar(150) NOT NULL,
    borc varchar(150),
    duzenlimi bit,
    tutar numeric(18,2),
    para_birimi varchar(10),
    kalan_taksit int,
    odeme_tarihi datetime,
    faiz_binecekmi bit,
    odendi_mi bit,
    talimat_varmi bit,
    bagimli_oldugu_gelir varchar(150),
    tarih datetime NOT NULL DEFAULT GETDATE()
);

-- Gelirler tablosu
CREATE TABLE gelirler (
    id bigint IDENTITY(1,1) PRIMARY KEY,
    kullanici varchar(150) NOT NULL,
    gelir varchar(150),
    duzenlimi bit,
    tutar numeric(18,2),
    para_birimi varchar(10),
    kalan_taksit int,
    tahsilat_tarihi datetime,
    faiz_binecekmi bit,
    alindi_mi bit,
    talimat_varmi bit,
    bagimli_oldugu_gider varchar(150),
    tarih datetime NOT NULL DEFAULT GETDATE()
);

-- Kullanıcılar tablosu
CREATE TABLE kullanicilar (
    id bigint IDENTITY(1,1) PRIMARY KEY,
    kullanici varchar(150),
    sifre varchar(150),
    tarih date NOT NULL DEFAULT GETDATE(),
    onaylandi bit NOT NULL DEFAULT 0,
    verification_token varchar(50)
);

-- İstekler tablosu
CREATE TABLE istekler (
    id int IDENTITY(1,1) PRIMARY KEY,
    kullanici nchar(50),
    istekler nchar(500),
    oncelik nchar(50),
    [date] datetime DEFAULT GETDATE()
);

-- İndeksler
CREATE INDEX IX_varliklar_kullanici ON varliklar(kullanici);
CREATE INDEX IX_borclar_giderler_kullanici ON borclar_giderler(kullanici);
CREATE INDEX IX_gelirler_kullanici ON gelirler(kullanici);
CREATE INDEX IX_kullanicilar_kullanici ON kullanicilar(kullanici);
CREATE INDEX IX_pariteler_parite ON pariteler(parite);
CREATE INDEX IX_kurlar_parite ON kurlar(parite);
GO
