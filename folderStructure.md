# VarlikYonetim Proje Klasör Yapısı

Bu belge, projenin klasör ve dosya yapısının güncel halini yansıtmaktadır.

## Teknoloji Yığını
- Backend: Node.js
- Frontend: React Native Web
- Veri Toplama: Python
- Veritabanı: MsSQL
- API: REST

## Proje Mimarisi

VarlikYonetim/
├── .env                           # Ortam değişkenleri ve gizli ayarlar
├── .cursorrules                   # Proje geliştirme kuralları
├── .gitignore                     # Git tarafından yok sayılacak dosyalar
├── database.md                    # Veritabanı yapısı dokümantasyonu
├── folderStructure.md             # Bu dosya: Klasör yapısı dokümantasyonu
├── back/                          # Backend (Node.js) kaynak kodları
│   ├── config/                    # Yapılandırma dosyaları
│   │   └── db.js                 # Veritabanı yapılandırması
│   ├── controllers/               # API endpoint işlemleri
│   │   ├── index.js              # Controller indeksi
│   │   ├── kullaniciController.js # Kullanıcı işlemleri
│   │   ├── varlikController.js   # Varlık işlemleri
│   │   ├── borcController.js     # Borç işlemleri
│   │   ├── gelirController.js    # Gelir işlemleri
│   │   └── giderController.js    # Gider işlemleri
│   ├── middleware/                # Ara katman işlevleri
│   │   ├── authMiddleware.js     # Kimlik doğrulama
│   │   └── errorHandler.js       # Hata yakalama
│   ├── models/                    # Veritabanı modelleri
│   │   ├── Gelir.js             # Gelir modeli
│   │   ├── Gider.js             # Gider modeli
│   │   ├── User.js              # Kullanıcı modeli
│   │   ├── Varlik.js            # Varlık modeli
│   │   └── Borc.js              # Borç modeli
│   ├── routes/                    # API rotaları
│   │   ├── kullanicilar.js      # Kullanıcı rotaları
│   │   ├── varlikRoutes.js      # Varlık rotaları
│   │   ├── gelirRoutes.js       # Gelir rotaları
│   │   ├── giderRoutes.js       # Gider rotaları
│   │   ├── kullaniciRoutes.js   # Kullanıcı rotaları
│   │   └── borcRoutes.js        # Borç rotaları
│   ├── utils/                     # Yardımcı fonksiyonlar
│   │   └── logger.js            # Loglama işlemleri
│   ├── package.json               # Node.js bağımlılıkları
│   ├── package-lock.json          # Node.js bağımlılık kilitleri
│   └── Main.js                    # Ana uygulama dosyası
├── front/                         # Frontend (React Native Web) kaynak kodları
│   ├── src/                       # Kaynak kodlar
│   │   └── components/           # Bileşen dosyaları
│   ├── components/                # UI bileşenleri
│   │   ├── Tables/              # Tablo bileşenleri
│   │   │   ├── styles.js       # Tablo stilleri
│   │   │   └── DataTable.js    # Veri tablosu bileşeni
│   │   ├── Modal/               # Modal bileşenleri
│   │   │   └── AlertModal.js   # Uyarı modal bileşeni
│   │   ├── Header/              # Başlık bileşenleri
│   │   │   └── Header.js       # Ana başlık bileşeni
│   │   ├── Forms/               # Form bileşenleri
│   │   │   ├── DynamicForm.js  # Dinamik form oluşturucu
│   │   │   ├── FormField.js    # Form alan bileşeni
│   │   │   ├── GelirForm.js    # Gelir formu
│   │   │   └── BorcForm.js     # Borç formu
│   │   ├── Common/              # Ortak bileşenler
│   │   │   └── Checkbox.js     # Onay kutusu bileşeni
│   │   ├── Buttons/             # Düğme bileşenleri
│   │   │   ├── MainButton.js   # Ana düğme bileşeni
│   │   │   └── SubButton.js    # Alt düğme bileşeni
│   │   ├── UserInfo.js         # Kullanıcı bilgi bileşeni
│   │   ├── LoginModal.js       # Giriş modal bileşeni
│   │   └── Logo.js             # Logo bileşeni
│   ├── assets/                    # Medya dosyaları
│   ├── context/                   # Durum yönetimi
│   ├── public/                    # Statik dosyalar
│   ├── services/                  # API servisleri
│   ├── styles/                    # Stil dosyaları
│   ├── .babelrc                   # Babel yapılandırması
│   ├── App.js                     # Ana uygulama bileşeni
│   ├── app.json                   # Uygulama yapılandırması
│   ├── babel.config.js            # Babel yapılandırması
│   ├── index.html                 # Ana HTML dosyası
│   ├── index.web.js               # Web giriş noktası
│   ├── package.json               # Node.js bağımlılıkları
│   ├── package-lock.json          # Node.js bağımlılık kilitleri
│   └── webpack.config.js          # Webpack yapılandırması
└── data_collector/                # Veri toplama servisi (Python)
    ├── __pycache__/              # Python önbellek klasörü
    ├── candle_collectors/         # Mum verisi toplama modülleri
    │   ├── __pycache__/          # Python önbellek klasörü
    │   ├── binance_futures_collector.py    # Binance vadeli işlem mum verisi
    │   ├── binance_spot_collector.py       # Binance spot işlem mum verisi
    │   ├── commodity_collector.py          # Emtia mum verisi
    │   ├── currency_page.html             # Para birimi bilgi sayfası
    │   ├── forex_collector.py             # Forex mum verisi
    │   ├── index_collector.py             # Endeks mum verisi
    │   └── stock_collector.py             # Hisse senedi mum verisi
    ├── parite_collectors/         # Parite veri toplama modülleri
    │   ├── __pycache__/          # Python önbellek klasörü
    │   ├── binance_futures_collector.py    # Binance vadeli işlem parite verisi
    │   ├── binance_spot_collector.py       # Binance spot işlem parite verisi
    │   ├── commodity_collector.py          # Emtia parite verisi
    │   ├── forex_collector.py             # Forex parite verisi
    │   ├── index_collector.py             # Endeks parite verisi
    │   └── stock_collector.py             # Hisse senedi parite verisi
    ├── utils/                     # Yardımcı modüller
    ├── venv/                      # Python sanal ortamı
    │   ├── Include/              # C başlık dosyaları
    │   ├── Lib/                  # Python kütüphaneleri
    │   ├── Scripts/              # Çalıştırılabilir dosyalar
    │   └── pyvenv.cfg            # Sanal ortam yapılandırması
    ├── main.py                    # Ana program dosyası
    ├── README.md                  # Kullanım kılavuzu
    └── requirements.txt           # Python bağımlılıkları

## Açıklamalar

- Backend (back/):
  - Node.js tabanlı API sunucusu
  - MsSQL veritabanı entegrasyonu
  - MVC (Model-View-Controller) mimarisi
  - JWT tabanlı kimlik doğrulama
  - Middleware katmanı ile güvenlik kontrolleri
  - Modüler ve ölçeklenebilir yapı

- Frontend (front/):
  - React Native Web tabanlı kullanıcı arayüzü
  - Bileşen tabanlı modüler mimari
  - Context API ile merkezi durum yönetimi
  - Responsive ve modern UI tasarımı
  - Tailwind CSS ile stil yönetimi
  - Form validasyonları ve kullanıcı geri bildirimleri

- Veri Toplama (data_collector/):
  - Python tabanlı asenkron veri toplama servisi
  - Modüler kolektör yapısı
  - Hata yönetimi ve loglama sistemi
  - İki ana modül:
    - candle_collectors: Mum grafik verilerini toplar
    - parite_collectors: Parite verilerini toplar
  - Desteklenen veri kaynakları:
    - Binance (Spot ve Vadeli)
    - Forex
    - Endeksler
    - Hisse Senetleri
    - Emtia

- Konfigürasyon:
  - .env: Hassas bilgiler ve ortam değişkenleri
  - .cursorrules: Geliştirme standartları ve kuralları
  - .gitignore: Git kontrol dışı dosyalar

- Dokümantasyon:
  - database.md: Veritabanı şeması ve ilişkileri
  - folderStructure.md: Klasör yapısı (bu dosya)
  - README.md: Veri toplama servisi kullanımı

## Geliştirme Kuralları
- DRY (Don't Repeat Yourself) prensibi
- Kod tekrarından kaçınma
- Event handler'lar "handle" prefix'i ile başlamalı
- Fonksiyonlar const arrow function olarak tanımlanmalı
- Açıklayıcı değişken ve fonksiyon isimleri kullanılmalı
- TODO ve boş fonksiyon kullanılmamalı

## Servis Başlatma
```bash
# Backend
cd back && npm start

# Frontend
cd front && npm start

# Veri Toplama
cd data_collector && venv\Scripts\activate && python main.py
```
