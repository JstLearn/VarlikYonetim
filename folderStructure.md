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
│   │   ├── giderController.js     # Gider işlemleri kontrolcüsü
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
│   │   ├── giderRoutes.js        # Gider API rotaları
│   │   └── varlikRoutes.js       # Varlık API rotaları
│   │
│   ├── /utils                      # Yardımcı araçlar
│   │   ├── auth.js               # Kimlik doğrulama yardımcıları
│   │   ├── email.js              # E-posta işlemleri
│   │   └── validation.js         # Veri doğrulama yardımcıları
│   │
│   ├── Main.js                     # Ana giriş noktası ve Express sunucusu
│   └── package.json                # Backend bağımlılıkları
│
├── /front                          # Frontend uygulaması (React Native Web)
│   ├── /components                 # UI bileşenleri
│   │   ├── /Auth                  # Kimlik doğrulama bileşenleri
│   │   ├── /Buttons              # Özel buton bileşenleri
│   │   ├── /Common               # Genel kullanım bileşenleri
│   │   ├── /Forms                # Form bileşenleri
│   │   ├── /Header              # Başlık ve navigasyon bileşenleri
│   │   ├── /Layout              # Sayfa düzeni bileşenleri
│   │   ├── /Modal               # Modal dialog bileşenleri
│   │   ├── /Tables              # Tablo bileşenleri
│   │   ├── LoginModal.js        # Giriş yapma modal'ı
│   │   └── UserInfo.js          # Kullanıcı bilgileri bileşeni
│   │
│   ├── /context                    # React context'leri
│   │   ├── AuthContext.js        # Kimlik doğrulama context'i
│   │   └── ThemeContext.js       # Tema context'i
│   │
│   ├── /public                     # Statik dosyalar
│   │   ├── index.html           # Ana HTML şablonu
│   │   ├── favicon.ico          # Site ikonu
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
│   ├── index.html                  # Ana HTML dosyası
│   ├── index.web.js                # Web giriş noktası
│   ├── webpack.config.js           # Webpack yapılandırması
│   ├── babel.config.js             # Babel yapılandırması
│   ├── .babelrc                    # Babel ek yapılandırması
│   └── package.json                # Frontend bağımlılıkları
│
├── /data_collector                 # Veri toplama servisi (Python)
│   ├── /venv                       # Python sanal ortamı
│   ├── collectors.py               # Veri toplayıcı modüller (Borsa, Döviz, Kripto)
│   ├── config.py                   # Servis yapılandırması ve API anahtarları
│   ├── database.py                 # Veritabanı işlemleri ve model tanımları
│   ├── main.py                     # Servis giriş noktası ve zamanlanmış görevler
│   └── requirements.txt            # Python bağımlılıkları
│
├── /Yedek                          # Yedek dosyaları
│   └── ...                         # Otomatik ve manuel yedekler
│
├── database.md                     # Veritabanı şema dokümantasyonu
├── folderStructure.md              # Klasör yapısı dokümantasyonu
├── .env                            # Ortam değişkenleri (API anahtarları, DB bilgileri)
└── .gitignore                      # Git yoksayma listesi
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
- Döviz kurları toplama
- Kripto para fiyatları toplama
- Borsa verileri toplama
- Zamanlanmış otomatik veri güncelleme
- Veritabanına otomatik kayıt

## Teknoloji Yığını

- **Backend:** Node.js, Express, MsSQL, JWT
- **Frontend:** React Native Web, Context API, Axios
- **Veri Toplama:** Python, Requests, SQLAlchemy
- **DevOps:** Docker, Git
- **Diğer:** SMTP, REST API

## Notlar

- Backend servisi Node.js/Express ile geliştirilmiştir ve MsSQL veritabanı kullanmaktadır
- Frontend React Native Web kullanılarak geliştirilmiştir ve web tarayıcılarında çalışır
- Veri toplama servisi Python ile yazılmış olup, döviz kurları ve diğer finansal verileri toplar
- Tüm servisler Docker konteynerlerinde çalışacak şekilde yapılandırılabilir
- Sistem modüler yapıda tasarlanmış olup, yeni özellikler kolayca eklenebilir


