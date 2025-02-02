# Varlık Yönetim Sistemi (VNNM)

Bu proje, kişisel varlık ve borç yönetimi için geliştirilmiş bir web uygulamasıdır.

## Proje Yapısı

```
/VarlikYonetim                      # Ana proje dizini
│
├── /back                           # Backend uygulaması (Node.js/Express)
│   ├── /config                     # Yapılandırma dosyaları
│   │   └── db.js                   # MsSQL veritabanı bağlantı yapılandırması
│   │
│   ├── /controllers                # API kontrolcüleri
│   │   ├── index.js               # Kontrolcü export tanımları
│   │   ├── kullaniciController.js # Kullanıcı işlemleri kontrolcüsü
│   │   ├── borcController.js      # Borç/gider işlemleri kontrolcüsü
│   │   ├── gelirController.js     # Gelir işlemleri kontrolcüsü
│   │   └── varlikController.js    # Varlık işlemleri kontrolcüsü
│   │
│   ├── /middleware                 # Express middleware'leri
│   │   ├── authMiddleware.js      # Kimlik doğrulama middleware'i
│   │   └── errorHandler.js        # Hata yakalama middleware'i
│   │
│   ├── /models                     # Veritabanı modelleri
│   │   ├── kullaniciModel.js      # Kullanıcı veri modeli
│   │   ├── borcModel.js          # Borç veri modeli
│   │   ├── gelirModel.js         # Gelir veri modeli
│   │   └── varlikModel.js        # Varlık veri modeli
│   │
│   ├── /routes                     # API rotaları
│   │   ├── kullaniciRoutes.js    # Kullanıcı API rotaları
│   │   ├── borcRoutes.js         # Borç API rotaları
│   │   ├── gelirRoutes.js        # Gelir API rotaları
│   │   └── varlikRoutes.js       # Varlık API rotaları
│   │
│   ├── /utils                      # Yardımcı araçlar
│   │   ├── auth.js               # Kimlik doğrulama yardımcıları
│   │   ├── email.js              # E-posta işlemleri
│   │   └── validation.js         # Veri doğrulama yardımcıları
│   │
│   ├── Main.js                     # Ana giriş noktası ve Express sunucusu
│   ├── package.json                # Backend bağımlılıkları
│   └── package-lock.json           # Backend bağımlılık kilitleri
│
├── /front                          # Frontend uygulaması (React Native Web)
│   ├── /assets                     # Statik varlıklar (resim, font vb.)
│   │
│   ├── /components                 # UI bileşenleri
│   │   ├── /Auth                  # Kimlik doğrulama bileşenleri
│   │   ├── /Buttons              # Özel buton bileşenleri
│   │   ├── /Common               # Genel kullanım bileşenleri
│   │   ├── /Forms                # Form bileşenleri
│   │   ├── /Header              # Başlık ve navigasyon bileşenleri
│   │   ├── /Layout              # Sayfa düzeni bileşenleri
│   │   ├── /Modal               # Modal dialog bileşenleri
│   │   └── /Tables              # Tablo bileşenleri
│   │
│   ├── /context                    # React context'leri
│   │   ├── AuthContext.js        # Kimlik doğrulama context'i
│   │   └── ThemeContext.js       # Tema context'i
│   │
│   ├── /public                     # Statik dosyalar
│   │   ├── index.html           # Ana HTML şablonu
│   │   └── assets/              # Resim ve diğer medya dosyaları
│   │
│   ├── /services                   # API servisleri
│   │   ├── api.js               # API istemcisi
│   │   ├── auth.js              # Kimlik doğrulama servisi
│   │   └── storage.js           # Yerel depolama servisi
│   │
│   ├── /src                        # Kaynak kodlar
│   │   ├── constants/           # Sabit değerler
│   │   ├── hooks/              # Özel React hook'ları
│   │   └── utils/              # Yardımcı fonksiyonlar
│   │
│   ├── /styles                     # Stil dosyaları
│   │   ├── global.css           # Global stiller
│   │   └── theme.js            # Tema yapılandırması
│   │
│   ├── App.js                      # Ana uygulama bileşeni
│   ├── app.json                    # Uygulama yapılandırması
│   ├── index.html                  # Ana HTML dosyası
│   ├── index.web.js                # Web giriş noktası
│   ├── webpack.config.js           # Webpack yapılandırması
│   ├── babel.config.js             # Babel yapılandırması
│   ├── .babelrc                    # Babel ek yapılandırması
│   ├── package.json                # Frontend bağımlılıkları
│   └── package-lock.json           # Frontend bağımlılık kilitleri
│
├── /data_collector                 # Veri toplama servisi (Python)
│   ├── /venv                       # Python sanal ortamı
│   ├── /__pycache__               # Python önbellek klasörü
│   ├── binance_futures_collector.py # Binance vadeli işlem veri toplayıcı
│   ├── binance_spot_collector.py   # Binance spot veri toplayıcı
│   ├── forex_collector.py          # Forex veri toplayıcı
│   ├── index_collector.py          # Endeks veri toplayıcı
│   ├── commodity_collector.py      # Emtia veri toplayıcı
│   ├── stock_collector.py          # Hisse senedi veri toplayıcı
│   ├── parite_collector.py         # Parite veri toplayıcı
│   ├── config.py                   # Servis yapılandırması
│   ├── database.py                 # Veritabanı işlemleri
│   ├── main.py                     # Servis giriş noktası
│   ├── requirements.txt            # Python bağımlılıkları
│   └── README.md                   # Servis dokümantasyonu
│
├── database.md                     # Veritabanı şema dokümantasyonu
├── folderStructure.md              # Klasör yapısı dokümantasyonu
├── .env                            # Ortam değişkenleri
├── .gitignore                      # Git yoksayma listesi
└── .cursorrules                    # Cursor IDE kuralları
```

## Servis Açıklamaları

### Backend Servisi (Node.js/Express)
- RESTful API sunucusu
- MsSQL veritabanı entegrasyonu
- JWT tabanlı kimlik doğrulama
- E-posta doğrulama ve şifre sıfırlama
- Varlık, borç, gelir ve gider yönetimi

### Frontend Uygulaması (React Native Web)
- Responsive web arayüzü
- Context tabanlı durum yönetimi
- Tema desteği
- Form validasyonları
- Gerçek zamanlı veri güncelleme
- Grafik ve raporlama araçları

### Veri Toplama Servisi (Python)
- Binance Futures ve Spot veri toplama
- Forex kurları toplama
- Endeks verileri toplama
- Emtia fiyatları toplama
- Hisse senedi verileri toplama
- Zamanlanmış otomatik veri güncelleme
- Veritabanına otomatik kayıt
- Modüler veri toplayıcı yapısı

## Teknoloji Yığını

- **Backend:** Node.js, Express, MsSQL, JWT
- **Frontend:** React Native Web, Context API, Axios
- **Veri Toplama:** Python, yfinance, python-binance, SQLAlchemy
- **DevOps:** Git
- **Diğer:** SMTP, REST API

## Notlar

- Backend servisi Node.js/Express ile geliştirilmiştir ve MsSQL veritabanı kullanmaktadır
- Frontend React Native Web kullanılarak geliştirilmiştir ve web tarayıcılarında çalışır
- Veri toplama servisi Python ile yazılmış olup, modüler yapıda tasarlanmıştır
- Her veri kaynağı için ayrı toplayıcı modüller bulunmaktadır
- Sistem modüler yapıda tasarlanmış olup, yeni özellikler kolayca eklenebilir


