# VarlikYonetim Proje Klasör Yapısı

Bu belge, projenin klasör ve dosya yapısının güncel halini yansıtmaktadır.

```
VarlikYonetim/
├── .env                           # Ana .env dosyası (veritabanı, API ayarları vb.)
├── .cursorrules                   # Proje dokümantasyon ve kod geliştirme kuralları
├── database.md                    # Veritabanı şemasını tanımlayan dokümantasyon dosyası
├── folderStructure.md             # Bu klasör yapısını tanımlayan dokümantasyon dosyası
├── back/                          # Backend (Node.js) kaynak kodları
│   ├── config/                    # Yapılandırma dosyaları (örn: db.js)
│   ├── controllers/               # API endpoint işlemleri
│   ├── middleware/                # Ara katman işlevleri
│   ├── models/                    # Veritabanı modelleri
│   ├── routes/                    # API rotaları
│   └── utils/                     # Yardımcı fonksiyonlar ve yardımcı modüller
├── front/                         # Frontend (React Native Web) kaynak kodları
│   ├── components/                # UI bileşenleri
│   ├── constants/                 # Sabit değerler
│   ├── context/                   # Durum yönetimi (React Context / Zustand)
│   ├── hooks/                     # Özel React hook'ları
│   ├── services/                  # API istekleri ve servis dosyaları
│   ├── styles/                    # Stil dosyaları (CSS, Tailwind configuration vb.)
│   └── utils/                     # Yardımcı modüller
└── data_collector/                # Veri Toplama Servisi (Python)
    ├── collectors/                # Farklı veri kaynaklarından veri toplayan modüller
    │   ├── binance_futures_collector.py
    │   ├── binance_spot_collector.py
    │   ├── candle_collectors/     # Mum (candle) verilerini toplayan modüller
    │   │   ├── binance_futures_collector.py
    │   │   ├── binance_spot_collector.py
    │   │   ├── forex_collector.py
    │   │   ├── index_collector.py
    │   │   ├── stock_collector.py
    │   │   └── commodity_collector.py
    │   ├── commodity_collector.py # Parite (emtia) verilerini toplayan modül
    │   ├── forex_collector.py
    │   ├── index_collector.py
    │   └── stock_collector.py
    ├── logs/                      # Log dosyalarının bulunduğu klasör
    ├── utils/                     # Yardımcı modüller ve konfigürasyon dosyaları
    │   ├── config.py              # Veri toplama servisinin konfigürasyon dosyası (.env referansı burada)
    │   ├── database.py            # Veritabanı bağlantı ve sorgu modülü
    │   └── logger.py              # Loglama işlemleri (varsa)
    ├── main.py                    # Veri toplama servisinin ana giriş noktası
    └── requirements.txt           # Python bağımlılıkları listesi
```

## Açıklamalar

- **.env**  
  Projenin genel konfigürasyon ayarlarını içerir. (Veritabanı bağlantısı, API anahtarları vs.)

- **.cursorrules**  
  Proje dokümantasyon ve geliştirme kurallarını içeren dosya.  
  **Not:** Bu dosyanın yolunu (C:\Users\durak\OneDrive\Code\VarlikYonetim\.cursorrules) her zaman referans alıyoruz.

- **back/**  
  Node.js tabanlı backend kodları. API endpointleri, veritabanı modelleri, araçlar vb. burada yer alır.

- **front/**  
  React Native Web kullanılarak oluşturulan frontend uygulaması. Bileşenler, stiller, servisler ve durum yönetimi modülleri burada bulunur.

- **data_collector/**  
  Python ile yazılan veri toplama servisi. Farklı veri kaynaklarından (örneğin, Binance, Forex, Emtia vs.) veri toplayarak veritabanına işleyen modülleri içerir.  
  Konfigürasyon ayarları **utils/config.py** üzerinden ana .env dosyasını referans alır.

Bu güncelleme, projenin klasör yapısını en güncel haliyle yansıtmak amacıyla yapılmıştır.


