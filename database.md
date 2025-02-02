{
  "info": "Bu dosya, projenin veritabanı tablolarına ait temel bilgileri içermektedir. Son güncelleme tarihi: 2024-03-19",
  "tables": [
    {
      "tableName": "pariteler",
      "purpose": "Borsa paritelerini ve özelliklerini saklar.",
      "columns": [
        {
          "columnName": "ID",
          "dataType": "int",
          "allowNull": false,
          "description": "Parite kaydı için benzersiz kimlik (Primary Key)."
        },
        {
          "columnName": "parite",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Parite adı (örn: BTC/USDT)."
        },
        {
          "columnName": "borsa",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Paritenin bulunduğu borsa (örn: BINANCE)."
        },
        {
          "columnName": "tip",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Paritenin tipi (örn: SPOT, FUTURES)."
        },
        {
          "columnName": "ulke",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "İlgili ülke."
        },
        {
          "columnName": "aciklama",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Parite hakkında açıklayıcı bilgi."
        },
        {
          "columnName": "aktif",
          "dataType": "bit",
          "allowNull": true,
          "description": "Paritenin aktif olup olmadığı."
        },
        {
          "columnName": "kayit_tarihi",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kaydın oluşturulma tarihi."
        }
      ]
    },
    {
      "tableName": "kurlar",
      "purpose": "Döviz veya diğer parite verilerini saklar.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Kur kaydı için benzersiz kimlik (Primary Key)."
        },
        {
          "columnName": "parite",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Kur çiftinin adı (ör: USD/TRY, EUR/USD vb.)."
        },
        {
          "columnName": "interval",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Kur verisi aralığı (ör: günlük, saatlik vb.)."
        },
        {
          "columnName": "borsa",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Kurun işlem gördüğü borsa."
        },
        {
          "columnName": "tip",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "Kurun türü (ör: Döviz, Kripto vb.)."
        },
        {
          "columnName": "ulke",
          "dataType": "nvarchar",
          "allowNull": true,
          "description": "İlgili ülke ismi (ör: Türkiye, ABD vb.)."
        },
        {
          "columnName": "fiyat",
          "dataType": "decimal",
          "allowNull": true,
          "description": "Kurun anlık fiyatı."
        },
        {
          "columnName": "dolar_karsiligi",
          "dataType": "decimal",
          "allowNull": true,
          "description": "1 birimin USD cinsinden karşılığı."
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Kur verisinin ait olduğu tarih."
        },
        {
          "columnName": "kayit_tarihi",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Verinin sisteme kaydedildiği tarih."
        }
      ]
    },
    {
      "tableName": "varliklar",
      "purpose": "Kullanıcıların sahip olduğu varlıkları (alış, satış, kâr/zarar vb.) takip eder.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Varlık kaydı için benzersiz kimlik (Primary Key).",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar",
          "allowNull": false,
          "description": "Kullanıcı adı veya ID bilgisi.",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "varlik",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Varlık adı (ör: hisse, coin, ev vb.).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "tur",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Varlığın türü (ör: dijital, gayrimenkul vb.).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "nerede",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Varlığın bulunduğu yer (ör: borsa, cüzdan vb.).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "alis_tarihi",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Varlığın alındığı tarih.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "alis_fiyati",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Alış fiyatı (genelde USD veya başka para birimi).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "alis_adedi",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Alınan miktar/adet.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "simdi_fiyati_USD",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Mevcut fiyatı (USD cinsinden).",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "kar_zarar",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Toplam kâr/zarar miktarı (USD bazlı).",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "kar_zarar_yuzde",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Kâr/zarar yüzdesi.",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "min_satis_fiyati_USD",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Satış için minimum hedef fiyatı (USD).",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kaydın oluşturulma veya güncellenme tarihi.",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        }
      ]
    },
    {
      "tableName": "borclar_giderler",
      "purpose": "Kullanıcıların mevcut veya planlanmış borç bilgilerini tutar.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Borç kaydı için benzersiz kimlik (Primary Key).",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar",
          "allowNull": false,
          "description": "Kullanıcı adı veya ID bilgisi.",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "borc",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Borcun adı/tanımı (ör: Kredi kartı, İhtiyaç kredisi vb.).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "duzenlimi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Borcun düzenli (her ay vb.) ödenip ödenmediğini belirtir.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "tutar",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Borcun toplam tutarı.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "para_birimi",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Borcun para birimi (ör: TRY, USD).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "kalan_taksit",
          "dataType": "int",
          "allowNull": true,
          "description": "Kalan taksit sayısı.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "odeme_tarihi",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Ödeme günü/tarihi.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "faiz_binecekmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Faizin işleyip işlemeyeceğini belirtir.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "odendi_mi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Borcun ödenip ödenmediğini belirtir.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "talimat_varmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Otomatik ödeme talimatının olup olmadığı.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "bagimli_oldugu_gelir",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Bu borcun dayandığı bir gelir kaydına referans.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kaydın oluşturulma veya güncellenme tarihi.",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        }
      ]
    },
    {
      "tableName": "gelirler",
      "purpose": "Kullanıcıların düzenli/düzensiz gelir kayıtlarını tutar.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "bigint",
          "allowNull": false,
          "description": "Gelir kaydı için benzersiz kimlik (Primary Key).",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar",
          "allowNull": false,
          "description": "Kullanıcı adı veya ID bilgisi.",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
        },
        {
          "columnName": "gelir",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Gelirin adı/tanımı (ör: Maaş, ek iş, kira vb.).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "duzenlimi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Gelirin düzenli (her ay vb.) gelip gelmediğini belirtir.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "tutar",
          "dataType": "numeric",
          "allowNull": true,
          "description": "Gelir tutarı.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "para_birimi",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Gelirin para birimi (ör: TRY, USD).",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "kalan_taksit",
          "dataType": "int",
          "allowNull": true,
          "description": "Gelirin taksitle alınması durumunda kalan taksit sayısı.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "tahsilat_tarihi",
          "dataType": "datetime",
          "allowNull": true,
          "description": "Gelirin tahsilat günü/tarihi.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "faiz_binecekmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Faiz getirisinin olup olmayacağını belirtir.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "alindi_mi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Gelirin tahsil edilip edilmediğini gösterir.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "talimat_varmi",
          "dataType": "bit",
          "allowNull": true,
          "description": "Otomatik tahsilat talimatı olup olmadığını belirtir.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "bagimli_oldugu_gider",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Bu gelirin dayandığı bir gider kaydına referans.",
          "frontend": "kullanıcıdan istiyoruz frontendde olmalı"
        },
        {
          "columnName": "tarih",
          "dataType": "datetime",
          "allowNull": false,
          "description": "Kaydın oluşturulma veya güncellenme tarihi.",
          "frontend": "kullanıcıdan istemiyoruz frontendde olmamalı"
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
          "description": "Kullanıcı için benzersiz kimlik (Primary Key)."
        },
        {
          "columnName": "kullanici",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Kullanıcı adı."
        },
        {
          "columnName": "sifre",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Şifrelenmiş kullanıcı parolası."
        },
        {
          "columnName": "tarih",
          "dataType": "date",
          "allowNull": false,
          "description": "Hesabın oluşturulma tarihi."
        },
        {
          "columnName": "onaylandi",
          "dataType": "bit",
          "allowNull": false,
          "description": "Kullanıcı hesabının onaylanıp onaylanmadığı."
        },
        {
          "columnName": "verification_token",
          "dataType": "varchar",
          "allowNull": true,
          "description": "Hesap doğrulama için kullanılan token."
        }
      ]
    },
    {
      "tableName": "istekler",
      "purpose": "Kullanıcı isteklerini ve önerilerini takip eder.",
      "columns": [
        {
          "columnName": "id",
          "dataType": "int",
          "allowNull": true,
          "description": "İstek kaydı için benzersiz kimlik."
        },
        {
          "columnName": "kullanici",
          "dataType": "nchar",
          "allowNull": true,
          "description": "İsteği oluşturan kullanıcı."
        },
        {
          "columnName": "istekler",
          "dataType": "nchar",
          "allowNull": true,
          "description": "İstek veya öneri içeriği."
        },
        {
          "columnName": "oncelik",
          "dataType": "nchar",
          "allowNull": true,
          "description": "İsteğin öncelik seviyesi."
        },
        {
          "columnName": "date",
          "dataType": "datetime",
          "allowNull": true,
          "description": "İsteğin oluşturulma tarihi."
        }
      ]
    }
  ],
  "note": "Bu dosya veritabanı şemasının güncel halini içermektedir. Sistem tablosu olan 'sysdiagrams' tablosu dokümantasyona dahil edilmemiştir."
}
