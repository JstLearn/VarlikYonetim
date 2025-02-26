# VarlikYonetim Veritabanı Şeması

Bu belge, projenin SQL Server veritabanındaki tabloların güncel şemasını içermektedir.
Son güncelleme tarihi: 2024-02-24

## Veritabanı Bağlantı Bilgileri
- Sunucu: ADMINISTRATOR
- Veritabanı: VARLIK_YONETIM
- Kullanıcı: durak
- Sürücü: ODBC Driver 17 for SQL Server
- Bağlantı Güvenliği: Şifreleme kapalı (DB_ENCRYPT=no)

## Tablolar

### 1. pariteler
Borsa ve diğer piyasa paritelerini, özellikleri ile saklar.

| Kolon           | Tip           | Null  | Varsayılan | Açıklama                                    |
|-----------------|---------------|-------|------------|---------------------------------------------|
| ID              | int          | Hayır | IDENTITY   | Parite kaydı için benzersiz kimlik (PK)     |
| parite          | nvarchar(50) | Evet  | -          | Parite adı (örn: BTC/USDT)                  |
| borsa           | nvarchar(50) | Evet  | -          | İşlem gördüğü borsa (BINANCE, COMMODITY)    |
| tip             | nvarchar(50) | Evet  | -          | Paritenin tipi (SPOT, FUTURES, COMMODITY)   |
| ulke            | nvarchar(50) | Evet  | -          | İlgili ülke bilgisi                         |
| aciklama        | nvarchar(500)| Evet  | -          | Parite hakkında açıklayıcı bilgi            |
| aktif           | bit          | Evet  | -          | Paritenin aktif olup olmadığı               |
| veri_var        | bit          | Evet  | -          | Parite için veri var mı                     |
| veriler_guncel  | bit          | Evet  | -          | Parite verilerinin güncellik durumu         |
| kayit_tarihi    | datetime     | Hayır | GETDATE()  | Kaydın oluşturulma tarihi                   |

### 2. kurlar
Döviz ve diğer kur çiftlerini saklar.

| Kolon           | Tip           | Null  | Varsayılan | Açıklama                                    |
|-----------------|---------------|-------|------------|---------------------------------------------|
| id              | bigint(19)   | Hayır | IDENTITY   | Kur kaydı için benzersiz kimlik (PK)        |
| parite          | nvarchar(50) | Evet  | -          | Kur çiftinin adı (örn: USD/TRY)             |
| interval        | nvarchar(50) | Evet  | -          | Veri aralığı (günlük, saatlik)              |
| borsa           | nvarchar(50) | Evet  | -          | Kurun işlem gördüğü borsa                   |
| tip             | nvarchar(50) | Evet  | -          | Kur türü (Döviz, Kripto)                    |
| ulke            | nvarchar(50) | Evet  | -          | İlgili ülke                                 |
| fiyat           | decimal(18,8)| Evet  | -          | Kapanış fiyatı                              |
| dolar_karsiligi | decimal(18,8)| Evet  | -          | 1 birimin USD cinsinden karşılığı           |
| tarih           | datetime     | Evet  | -          | Verinin ait olduğu tarih                    |
| kayit_tarihi    | datetime     | Hayır | GETDATE()  | Kaydın sisteme işlendiği tarih              |

### 3. varliklar
Kullanıcıların varlık işlemlerini ayrıntılı olarak takip eder.

| Kolon               | Tip           | Null  | Varsayılan | Açıklama                                    |
|--------------------|---------------|-------|------------|---------------------------------------------|
| id                 | bigint(19)   | Hayır | IDENTITY   | Varlık için benzersiz kimlik (PK)           |
| kullanici          | varchar(150) | Hayır | -          | Kullanıcıya ait ID veya ad                  |
| varlik             | varchar(150) | Evet  | -          | Varlığın adı                                |
| tur                | varchar(150) | Evet  | -          | Varlık tipi (dijital, gayrimenkul)          |
| nerede             | varchar(150) | Evet  | -          | Varlığın bulunduğu yer (borsa, cüzdan)      |
| alis_tarihi        | datetime     | Evet  | -          | Varlığın alım tarihi                        |
| alis_adedi         | numeric(18,8)| Evet  | -          | Alınan miktar                               |
| simdi_fiyati_USD   | numeric(18,8)| Evet  | -          | Güncel fiyat (USD)                          |
| kar_zarar          | numeric(18,8)| Evet  | -          | Toplam kâr/zarar (USD)                      |
| kar_zarar_yuzde    | numeric(18,8)| Evet  | -          | Yüzdelik kâr/zarar oranı                    |
| min_satis_fiyati_USD| numeric(18,8)| Evet  | -         | Minimum satış fiyatı (USD)                  |
| tarih              | datetime     | Hayır | GETDATE()  | Kayıt tarihi                                |

### 4. borclar_giderler
Kullanıcıların borç ve gider bilgilerini tutar.

| Kolon                | Tip           | Null  | Varsayılan | Açıklama                                    |
|---------------------|---------------|-------|------------|---------------------------------------------|
| id                  | bigint(19)   | Hayır | IDENTITY   | Borcun benzersiz ID'si (PK)                 |
| kullanici           | varchar(150) | Hayır | -          | Kullanıcıya ait ID veya ad                  |
| borc                | varchar(150) | Evet  | -          | Borç tanımı (kredi, fatura)                 |
| duzenlimi           | bit          | Evet  | -          | Borç düzenli mi                             |
| tutar               | numeric(18,2)| Evet  | -          | Toplam borç tutarı                          |
| para_birimi         | varchar(10)  | Evet  | -          | Borç para birimi (TRY, USD)                 |
| kalan_taksit        | int(10)     | Evet  | -          | Kalan taksit sayısı                         |
| odeme_tarihi        | datetime     | Evet  | -          | Son ödeme tarihi                            |
| faiz_binecekmi      | bit          | Evet  | -          | Faiz uygulanacak mı                         |
| odendi_mi           | bit          | Evet  | -          | Borç ödenmiş mi                             |
| talimat_varmi       | bit          | Evet  | -          | Otomatik ödeme talimatı var mı              |
| bagimli_oldugu_gelir| varchar(150) | Evet  | -          | Borçla ilişkili gelir kaydı                 |
| tarih               | datetime     | Hayır | GETDATE()  | Kayıt tarihi                                |

### 5. gelirler
Kullanıcıların gelir kayıtlarını ayrıntılı olarak saklar.

| Kolon                | Tip           | Null  | Varsayılan | Açıklama                                    |
|---------------------|---------------|-------|------------|---------------------------------------------|
| id                  | bigint(19)   | Hayır | IDENTITY   | Gelir için benzersiz ID (PK)                |
| kullanici           | varchar(150) | Hayır | -          | Kullanıcıya ait ID veya ad                  |
| gelir               | varchar(150) | Evet  | -          | Gelir tanımı (maaş, kira)                   |
| duzenlimi           | bit          | Evet  | -          | Gelir düzenli mi                            |
| tutar               | numeric(18,2)| Evet  | -          | Gelir tutarı                                |
| para_birimi         | varchar(10)  | Evet  | -          | Gelirin para birimi (TRY, USD)              |
| kalan_taksit        | int(10)     | Evet  | -          | Taksitli ödemelerde kalan sayı              |
| tahsilat_tarihi     | datetime     | Evet  | -          | Gelirin tahsil edildiği tarih               |
| faiz_binecekmi      | bit          | Evet  | -          | Faiz getirisi olacak mı                     |
| alindi_mi           | bit          | Evet  | -          | Gelir alındı mı                             |
| talimat_varmi       | bit          | Evet  | -          | Otomatik tahsilat talimatı var mı           |
| bagimli_oldugu_gider| varchar(150) | Evet  | -          | Gelirle ilişkili gider kaydı                |
| tarih               | datetime     | Hayır | GETDATE()  | Kayıt tarihi                                |

### 6. kullanicilar
Kullanıcı hesap bilgilerini ve doğrulama durumlarını tutar.

| Kolon             | Tip           | Null  | Varsayılan | Açıklama                                    |
|------------------|---------------|-------|------------|---------------------------------------------|
| id               | bigint(19)   | Hayır | IDENTITY   | Kullanıcı için benzersiz ID (PK)            |
| kullanici        | varchar(150) | Evet  | -          | Kullanıcının adı                            |
| sifre            | varchar(150) | Evet  | -          | Şifrelenmiş kullanıcı parolası              |
| tarih            | date         | Hayır | GETDATE()  | Hesabın oluşturulma tarihi                  |
| onaylandi        | bit          | Hayır | 0          | Hesabın onay durumu                         |
| verification_token| varchar(50)  | Evet  | -          | Hesap doğrulama token'ı                     |

### 7. istekler
Kullanıcı istek ve önerilerini takip eder.

| Kolon      | Tip           | Null  | Varsayılan | Açıklama                                    |
|-----------|---------------|-------|------------|---------------------------------------------|
| id        | int(10)      | Hayır | IDENTITY   | İstek kaydı için benzersiz ID (PK)          |
| kullanici | nchar(50)    | Evet  | -          | İsteği yapan kullanıcı                      |
| istekler  | nchar(500)   | Evet  | -          | İstek veya öneri içeriği                    |
| oncelik   | nchar(50)    | Evet  | -          | İsteğin öncelik seviyesi                    |
| date      | datetime     | Evet  | GETDATE()  | İsteğin oluşturulma tarihi                  |

## İndeksler

- IX_varliklar_kullanici: varliklar(kullanici)
- IX_borclar_giderler_kullanici: borclar_giderler(kullanici)
- IX_gelirler_kullanici: gelirler(kullanici)
- IX_kullanicilar_kullanici: kullanicilar(kullanici)
- IX_pariteler_parite: pariteler(parite)
- IX_kurlar_parite: kurlar(parite)

## İlişkiler

1. borclar_giderler -> gelirler:
   - bagimli_oldugu_gelir -> gelir (Soft Reference)

2. gelirler -> borclar_giderler:
   - bagimli_oldugu_gider -> borc (Soft Reference)

3. varliklar -> kullanicilar:
   - kullanici -> kullanici (Soft Reference)

4. borclar_giderler -> kullanicilar:
   - kullanici -> kullanici (Soft Reference)

5. gelirler -> kullanicilar:
   - kullanici -> kullanici (Soft Reference)

## Not
- Tüm tarih alanları için varsayılan değer GETDATE() kullanılmaktadır
- Para birimleri için varchar(10) standardı kullanılmaktadır
- Soft Reference: Fiziksel foreign key kısıtlaması olmayan mantıksal ilişki
- Tüm ID alanları IDENTITY özelliği ile otomatik artmaktadır
- Tüm para birimi alanları için TRY ve USD desteklenmektedir
- Tüm datetime alanları için UTC zaman dilimi kullanılmaktadır
- Veritabanı karakter seti: Turkish_CI_AS (Case Insensitive, Accent Sensitive)
- Veritabanı yedekleme sıklığı: Günlük (00:00 UTC)
- Parasal tutarlar için numeric(18,2) hassasiyet kullanılmaktadır
- Kur ve varlık hesaplamalarında decimal(18,8) hassasiyet kullanılmaktadır
- Tamsayı alanlar için int ve bigint hassasiyet kullanılmaktadır
- Tüm primary key'ler CLUSTERED index olarak tanımlanmıştır
- Veritabanı son güncelleme tarihi: 2024-02-24