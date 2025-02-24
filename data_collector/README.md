# Varlık Yönetimi Veri Toplama Servisi

Bu servis, çeşitli finansal varlıkların verilerini toplar ve veritabanına kaydeder.

## Desteklenen Veri Kaynakları

- **Binance**: Kripto para çiftleri
- **Forex**: Döviz çiftleri
- **Hisse Senetleri**: Dünya borsalarındaki hisseler
- **Borsa Endeksleri**: Önemli borsa endeksleri
- **Emtialar**: Altın, gümüş, petrol vb.

## Kurulum

1. Python 3.8 veya üzeri sürümü yükleyin
2. Sanal ortam oluşturun ve aktif edin:
```bash
python -m venv venv
venv\Scripts\activate
```

3. Gerekli paketleri yükleyin:
```bash
pip install -r requirements.txt
```

4. MsSQL veritabanı bağlantısını yapılandırın:
- SQL Server yüklü olmalı
- Windows kimlik doğrulaması aktif olmalı
- `VarlikYonetim` veritabanı oluşturulmuş olmalı

## Kullanım

Servisi başlatmak için:
```bash
python main.py
```

Servis başladığında:
1. Tüm veri kaynaklarından veri toplar
2. Verileri veritabanına kaydeder
3. 5 dakika bekler
4. İşlemi tekrarlar

Durdurmak için `Ctrl+C` tuşlarına basın.

## Klasör Yapısı

```
data_collector/
├── collectors/           # Veri toplayıcılar
│   ├── base_collector.py
│   ├── binance_collector.py
│   ├── forex_collector.py
│   ├── stock_collector.py
│   ├── index_collector.py
│   └── commodity_collector.py
├── utils/               # Yardımcı modüller
│   ├── database.py
│   └── logger.py
├── logs/               # Log dosyaları
├── main.py            # Ana program
├── requirements.txt   # Bağımlılıklar
└── README.md         # Dokümantasyon
```
## Veritabanı Şeması

`pariteler` tablosu:
- `parite`: VARCHAR(50) - Parite kodu (örn: BTC/USDT)
- `aktif`: BIT - Aktiflik durumu
- `borsa`: VARCHAR(50) - İşlem gördüğü borsa
- `tip`: VARCHAR(20) - Varlık tipi (CRYPTO, FOREX, STOCK, INDEX, COMMODITY)
- `ulke`: VARCHAR(50) - Ülke kodu
- `aciklama`: VARCHAR(500) - Açıklama
- `kayit_tarihi`: DATETIME - Son güncelleme tarihi

## Hata Ayıklama

- Log dosyaları `logs` klasöründe günlük olarak tutulur
- Her hata detaylı şekilde loglanır
- Veritabanı bağlantı hataları için Windows kimlik doğrulamasını kontrol edin
- API hatalarında internet bağlantısını kontrol edin

## Güvenlik

- API anahtarları gerektiren işlemler için `.env` dosyası kullanılır
- Windows kimlik doğrulaması ile güvenli veritabanı bağlantısı
- Hassas veriler loglanmaz

## Katkıda Bulunma

1. Bu depoyu fork edin
2. Yeni bir branch oluşturun (`git checkout -b feature/yeniOzellik`)
3. Değişikliklerinizi commit edin (`git commit -am 'Yeni özellik: X'`)
4. Branch'inizi push edin (`git push origin feature/yeniOzellik`)
5. Pull Request oluşturun 
