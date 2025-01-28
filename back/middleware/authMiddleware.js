const jwt = require('jsonwebtoken');

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

        // Token'ı doğrula
        const decoded = jwt.verify(token, process.env.JWT_SECRET || 'gizli-anahtar');
        
        // Kullanıcı bilgilerini request'e ekle
        req.user = {
            username: decoded.username,
            id: decoded.id
        };
        
        next();
    } catch (error) {
        console.error('Auth middleware hatası:', error);
        return res.status(401).json({
            success: false,
            message: 'Geçersiz token'
        });
    }
};

module.exports = authMiddleware; 