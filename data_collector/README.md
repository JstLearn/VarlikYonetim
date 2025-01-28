# Varlık Yönetimi - Veri Toplama Uygulaması

Bu uygulama, çeşitli finansal varlıkların (hisse senetleri, kripto paralar, döviz kurları ve altın) fiyat verilerini otomatik olarak toplar ve bir SQL Server veritabanında saklar.

## Özellikler

- Borsa İstanbul hisse senetleri için fiyat ve hacim verileri
- Popüler kripto paraların USD cinsinden fiyat ve hacim verileri
- Önemli döviz kurları için güncel veriler
- Altın türleri için güncel alış-satış fiyatları
- Saatlik otomatik veri güncelleme
- Hata yönetimi ve loglama

## Gereksinimler

- Python 3.8 veya üzeri
- SQL Server veritabanı
- SQL Server ODBC sürücüsü
- collectapi.com API anahtarı (altın verileri için)

## Kurulum

1. Sanal ortam oluşturun ve aktifleştirin:
```bash
python -m venv venv
venv\Scripts\activate
```

2. Gerekli paketleri yükleyin:
```bash
pip install -r requirements.txt
```

3. `config.py` dosyasında gerekli ayarlamaları yapın:
   - Veritabanı bağlantı bilgilerini güncelleyin
   - CollectAPI API anahtarınızı ekleyin
   - İstenirse sembol listesini ve güncelleme aralığını değiştirin

4. SQL Server'da 'VarlikDB' adında bir veritabanı oluşturun

## Kullanım

Uygulamayı başlatmak için:
```bash
python main.py
```

Uygulama başlatıldığında:
1. Gerekli veritabanı tabloları otomatik olarak oluşturulur
2. İlk veri toplama işlemi gerçekleştirilir
3. Belirtilen aralıklarla (varsayılan: 1 saat) veriler güncellenir

## Veritabanı Yapısı

### Hisseler Tablosu
- ID (PK)
- Sembol
- Tarih
- Acilis
- Yuksek
- Dusuk
- Kapanis
- Hacim

### Kriptolar Tablosu
- ID (PK)
- Sembol
- Tarih
- Acilis
- Yuksek
- Dusuk
- Kapanis
- Hacim

### Dovizler Tablosu
- ID (PK)
- Sembol
- Tarih
- Acilis
- Yuksek
- Dusuk
- Kapanis

### Altin Tablosu
- ID (PK)
- Tur
- Tarih
- Alis
- Satis

## Hata Ayıklama

Uygulama çalışırken karşılaşılan hatalar konsola yazdırılır. Hata mesajları tarih ve saat bilgisi içerir.

## Lisans

Bu proje MIT lisansı altında lisanslanmıştır. 