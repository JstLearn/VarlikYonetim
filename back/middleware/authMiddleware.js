const jwt = require('jsonwebtoken');

const createNewToken = (user) => {
    return jwt.sign(
        { username: user.username, id: user.id },
        process.env.JWT_SECRET || 'gizli-anahtar',
        { expiresIn: '24h' } // Token süresini 24 saat olarak ayarla
    );
};

const authMiddleware = (req, res, next) => {
    try {
        // Token'ı header'dan al
        const token = req.headers.authorization?.split(' ')[1];
        
        if (!token) {
            return res.status(401).json({
                success: false,
                message: 'Token bulunamadı'
            });
        }

        try {
            // Token'ı doğrula
            const decoded = jwt.verify(token, process.env.JWT_SECRET || 'gizli-anahtar');
            
            // Kullanıcı bilgilerini request'e ekle
            req.user = {
                username: decoded.username,
                id: decoded.id
            };

            // Token'ın süresi dolmak üzereyse yenile
            const tokenExp = decoded.exp * 1000; // JWT exp değeri saniye cinsinden
            const now = Date.now();
            const timeUntilExp = tokenExp - now;
            
            // Eğer token'ın süresi 1 saatten az kaldıysa yenile
            if (timeUntilExp < 3600000) {
                const newToken = createNewToken(req.user);
                res.setHeader('New-Token', newToken);
                res.setHeader('Access-Control-Expose-Headers', 'New-Token');
            }
            
            next();
        } catch (verifyError) {
            if (verifyError.name === 'TokenExpiredError') {
                try {
                    // Süresi dolmuş token'ı decode et
                    const decoded = jwt.decode(token);
                    
                    if (!decoded) {
                        throw new Error('Invalid token format');
                    }

                    // Yeni token oluştur
                    const newToken = createNewToken({ 
                        username: decoded.username, 
                        id: decoded.id 
                    });

                    // Yeni token'ı response header'a ekle
                    res.setHeader('New-Token', newToken);
                    res.setHeader('Access-Control-Expose-Headers', 'New-Token');
                    
                    // Kullanıcı bilgilerini request'e ekle
                    req.user = {
                        username: decoded.username,
                        id: decoded.id
                    };
                    
                    next();
                } catch (decodeError) {
                    console.error('Token decode hatası:', decodeError);
                    return res.status(401).json({
                        success: false,
                        message: 'Geçersiz token formatı'
                    });
                }
            } else {
                throw verifyError;
            }
        }
    } catch (error) {
        console.error('Auth middleware hatası:', error);
        return res.status(401).json({
            success: false,
            message: 'Geçersiz token'
        });
    }
};

module.exports = authMiddleware; 