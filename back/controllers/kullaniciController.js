const { sql, poolPromise } = require('../config/db');
const jwt = require('jsonwebtoken');
const nodemailer = require('nodemailer');
const bcrypt = require('bcrypt');

// E-posta gönderme için transporter oluştur
const transporter = nodemailer.createTransport({
    host: 'smtp.mail.me.com',
    port: 587,
    secure: false,
    auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS
    },
    tls: {
        rejectUnauthorized: false
    }
});

// Test connection
transporter.verify(function(error, success) {
    if (error) {
        console.log('SMTP Bağlantı hatası:', error);
    } else {
        console.log('SMTP Sunucusu hazır');
    }
});

// 6 haneli doğrulama kodu oluştur
function generateVerificationCode() {
    return Math.floor(100000 + Math.random() * 900000).toString();
}

// Doğrulama denemelerini takip etmek için
const verificationAttempts = new Map();

// Deneme sayısını kontrol et ve güncelle
const checkVerificationAttempt = (email) => {
    const now = Date.now();
    const attempts = verificationAttempts.get(email) || [];
    
    // Son 1 dakika içindeki denemeleri filtrele
    const recentAttempts = attempts.filter(timestamp => now - timestamp < 60000);
    
    // 5'ten fazla deneme varsa false döndür
    if (recentAttempts.length >= 5) {
        return false;
    }
    
    // Yeni denemeyi ekle
    recentAttempts.push(now);
    verificationAttempts.set(email, recentAttempts);
    return true;
};

const addKullanici = async (req, res) => {
    try {
        const { kullanici, sifre } = req.body;
        const pool = await poolPromise;

        // E-posta kontrolü
        const checkResult = await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .query('SELECT * FROM [VARLIK_YONETIM].[dbo].[kullanicilar] WHERE kullanici = @kullanici');

        // Doğrulama kodu oluştur
        const verificationCode = generateVerificationCode();

        if (checkResult.recordset.length > 0) {
            const user = checkResult.recordset[0];
            
            // Kullanıcı var ama onaylanmamış
            if (!user.onaylandi) {
                // Doğrulama kodunu güncelle
                await pool.request()
                    .input('kullanici', sql.VarChar(150), kullanici)
                    .input('verificationCode', sql.VarChar(6), verificationCode)
                    .query(`
                        UPDATE [VARLIK_YONETIM].[dbo].[kullanicilar]
                        SET verification_token = @verificationCode
                        WHERE kullanici = @kullanici
                    `);

                // E-posta gönder
                const mailOptions = {
                    from: process.env.EMAIL_USER,
                    to: kullanici,
                    subject: 'Yeni E-posta Doğrulama Kodunuz',
                    html: `
                        <h2>Merhaba!</h2>
                        <p>Yeni e-posta doğrulama kodunuz:</p>
                        <h1 style="
                            color: #007bff;
                            font-size: 32px;
                            letter-spacing: 5px;
                            margin: 20px 0;
                        ">${verificationCode}</h1>
                        <p>Bu kodu girerek hesabınızı doğrulayabilirsiniz.</p>
                    `
                };

                await transporter.sendMail(mailOptions);

                return res.json({
                    success: true,
                    message: 'Yeni doğrulama kodu e-posta adresinize gönderildi.'
                });
            } else {
                return res.status(400).json({
                    success: false,
                    message: 'Bu e-posta adresi zaten kayıtlı ve doğrulanmış'
                });
            }
        }

        // Şifreyi hashle
        const saltRounds = 10;
        const hashedPassword = await bcrypt.hash(sifre, saltRounds);

        // Yeni kullanıcıyı kaydet
        const result = await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .input('sifre', sql.VarChar, hashedPassword)
            .input('verificationCode', sql.VarChar(6), verificationCode)
            .input('onaylandi', sql.Bit, 0)
            .query(`
                INSERT INTO [VARLIK_YONETIM].[dbo].[kullanicilar]
                (kullanici, sifre, verification_token, onaylandi)
                VALUES
                (@kullanici, @sifre, @verificationCode, @onaylandi)
            `);

        // E-posta gönder
        const mailOptions = {
            from: process.env.EMAIL_USER,
            to: kullanici,
            subject: 'E-posta Doğrulama Kodunuz',
            html: `
                <h2>Hoş Geldiniz!</h2>
                <p>E-posta doğrulama kodunuz:</p>
                <h1 style="
                    color: #007bff;
                    font-size: 32px;
                    letter-spacing: 5px;
                    margin: 20px 0;
                ">${verificationCode}</h1>
                <p>Bu kodu girerek hesabınızı doğrulayabilirsiniz.</p>
            `
        };

        await transporter.sendMail(mailOptions);

        res.json({
            success: true,
            message: 'Kullanıcı başarıyla oluşturuldu. Lütfen e-postanızı kontrol edin.'
        });
    } catch (err) {
        console.error('Kullanıcı ekleme hatası:', err);
        res.status(500).json({
            success: false,
            message: 'Kullanıcı eklenirken bir hata oluştu'
        });
    }
};

const verifyEmail = async (req, res) => {
    try {
        const { email, code } = req.body;
        const pool = await poolPromise;

        // Deneme hakkını kontrol et
        if (!checkVerificationAttempt(email)) {
            return res.status(429).json({
                success: false,
                message: 'Çok fazla deneme yaptınız. Lütfen 1 dakika bekleyin.'
            });
        }

        // Kullanıcıyı bul ve doğrulama kodunu kontrol et
        const result = await pool.request()
            .input('kullanici', sql.VarChar(150), email)
            .input('code', sql.VarChar(6), code)
            .query(`
                SELECT * FROM [VARLIK_YONETIM].[dbo].[kullanicilar]
                WHERE kullanici = @kullanici AND verification_token = @code
            `);

        if (result.recordset.length > 0) {
            // Kullanıcıyı onayla
            await pool.request()
                .input('kullanici', sql.VarChar(150), email)
                .query(`
                    UPDATE [VARLIK_YONETIM].[dbo].[kullanicilar]
                    SET onaylandi = 1
                    WHERE kullanici = @kullanici
                `);

            const tokenData = { 
                username: email,
                id: result.recordset[0].id
            };

            const token = jwt.sign(tokenData, process.env.JWT_SECRET || 'gizli-anahtar', { expiresIn: '24h' });

            // Başarılı doğrulamadan sonra deneme sayısını sıfırla
            verificationAttempts.delete(email);

            res.json({
                success: true,
                message: 'E-posta başarıyla doğrulandı',
                data: {
                    token,
                    username: email
                }
            });
        } else {
            res.status(400).json({
                success: false,
                message: 'Geçersiz doğrulama kodu'
            });
        }
    } catch (error) {
        console.error('E-posta doğrulama hatası:', error);
        res.status(500).json({
            success: false,
            message: 'Doğrulama işlemi sırasında bir hata oluştu'
        });
    }
};

const validateKullanici = async (req, res) => {
    try {
        const { kullanici, sifre } = req.body;
        const pool = await poolPromise;

        const result = await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .query('SELECT * FROM [VARLIK_YONETIM].[dbo].[kullanicilar] WHERE kullanici = @kullanici');

        if (result.recordset.length === 0) {
            return res.status(401).json({
                success: false,
                message: 'Geçersiz kullanıcı adı veya şifre'
            });
        }

        const user = result.recordset[0];

        // Şifre kontrolü
        const isPasswordValid = await bcrypt.compare(sifre, user.sifre);

        if (!isPasswordValid) {
            return res.status(401).json({
                success: false,
                message: 'Geçersiz kullanıcı adı veya şifre'
            });
        }

        // E-posta doğrulaması kontrolü
        if (!user.onaylandi) {
            return res.status(401).json({
                success: false,
                message: 'Lütfen önce e-posta adresinizi doğrulayın'
            });
        }

        const tokenData = { 
            username: kullanici,
            id: user.id
        };

        const token = jwt.sign(tokenData, process.env.JWT_SECRET || 'gizli-anahtar', { expiresIn: '24h' });

        res.json({
            success: true,
            data: {
                token,
                username: kullanici
            }
        });
    } catch (err) {
        console.error('Kullanıcı doğrulama hatası:', err);
        res.status(500).json({
            success: false,
            message: 'Giriş yapılırken bir hata oluştu'
        });
    }
};

const forgotPassword = async (req, res) => {
    try {
        const { kullanici } = req.body;
        const pool = await poolPromise;

        // Kullanıcıyı kontrol et
        const result = await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .query('SELECT * FROM [VARLIK_YONETIM].[dbo].[kullanicilar] WHERE kullanici = @kullanici');

        if (result.recordset.length === 0) {
            return res.status(404).json({
                success: false,
                message: 'Bu e-posta adresiyle kayıtlı kullanıcı bulunamadı'
            });
        }

        // Sıfırlama kodu oluştur
        const resetCode = Math.floor(100000 + Math.random() * 900000).toString();

        // Sıfırlama kodunu kaydet
        await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .input('resetCode', sql.VarChar(6), resetCode)
            .query(`
                UPDATE [VARLIK_YONETIM].[dbo].[kullanicilar]
                SET verification_token = @resetCode
                WHERE kullanici = @kullanici
            `);

        // E-posta gönder
        const mailOptions = {
            from: process.env.EMAIL_USER,
            to: kullanici,
            subject: 'Parola Sıfırlama Kodunuz',
            html: `
                <h2>Parola Sıfırlama</h2>
                <p>Parola sıfırlama kodunuz:</p>
                <h1 style="
                    color: #007bff;
                    font-size: 32px;
                    letter-spacing: 5px;
                    margin: 20px 0;
                ">${resetCode}</h1>
                <p>Bu kodu kullanarak yeni parolanızı belirleyebilirsiniz.</p>
            `
        };

        await transporter.sendMail(mailOptions);

        res.json({
            success: true,
            message: 'Parola sıfırlama kodu e-posta adresinize gönderildi'
        });
    } catch (error) {
        console.error('Parola sıfırlama hatası:', error);
        res.status(500).json({
            success: false,
            message: 'Parola sıfırlama işlemi sırasında bir hata oluştu'
        });
    }
};

const resetPassword = async (req, res) => {
    try {
        const { kullanici, code, yeniSifre } = req.body;
        const pool = await poolPromise;

        // Kullanıcı ve kodu kontrol et
        const result = await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .input('code', sql.VarChar(6), code)
            .query(`
                SELECT * FROM [VARLIK_YONETIM].[dbo].[kullanicilar]
                WHERE kullanici = @kullanici AND verification_token = @code
            `);

        if (result.recordset.length === 0) {
            return res.status(400).json({
                success: false,
                message: 'Geçersiz sıfırlama kodu'
            });
        }

        // Yeni şifreyi hashle
        const saltRounds = 10;
        const hashedPassword = await bcrypt.hash(yeniSifre, saltRounds);

        // Şifreyi güncelle ve sıfırlama kodunu temizle
        await pool.request()
            .input('kullanici', sql.VarChar(150), kullanici)
            .input('sifre', sql.VarChar, hashedPassword)
            .query(`
                UPDATE [VARLIK_YONETIM].[dbo].[kullanicilar]
                SET sifre = @sifre, verification_token = NULL
                WHERE kullanici = @kullanici
            `);

        res.json({
            success: true,
            message: 'Parolanız başarıyla güncellendi'
        });
    } catch (error) {
        console.error('Parola güncelleme hatası:', error);
        res.status(500).json({
            success: false,
            message: 'Parola güncellenirken bir hata oluştu'
        });
    }
};

module.exports = {
    addKullanici,
    validateKullanici,
    verifyEmail,
    forgotPassword,
    resetPassword
}; 