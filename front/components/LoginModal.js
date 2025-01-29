import React, { useState, useEffect } from 'react';
import { Modal, View, Text, TextInput, TouchableOpacity, StyleSheet } from 'react-native';
import { useUser } from '../context/UserContext';

const API_BASE_URL = 'http://192.168.1.106:3000';

const LoginModal = ({ visible, onClose, onSuccess, actionType: initialActionType }) => {
  const { setUser } = useUser();
  const [email, setEmail] = useState('');
  const [isValidEmail, setIsValidEmail] = useState(false);
  const [password, setPassword] = useState('');
  const [passwordChecks, setPasswordChecks] = useState({
    length: false,
    upperCase: false,
    lowerCase: false,
    number: false,
    special: false
  });
  const [verificationCode, setVerificationCode] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [showVerification, setShowVerification] = useState(false);
  const [actionType, setActionType] = useState('giris');
  const [showResetPassword, setShowResetPassword] = useState(false);
  const [newPassword, setNewPassword] = useState('');
  const [isValidCode, setIsValidCode] = useState(false);
  const [isCodeVerified, setIsCodeVerified] = useState(false);
  const [countdown, setCountdown] = useState(0);
  const [isLocked, setIsLocked] = useState(false);

  useEffect(() => {
    if (visible) {
      setEmail('');
      setPassword('');
      setVerificationCode('');
      setError('');
      setSuccessMessage('');
      setShowVerification(false);
      setShowResetPassword(false);
      setActionType('giris');
      setIsCodeVerified(false);
      setIsValidCode(false);
      setIsLocked(false);
      setCountdown(0);
    }
  }, [visible]);

  useEffect(() => {
    let timer;
    if (countdown > 0) {
      timer = setInterval(() => {
        setCountdown(prev => prev - 1);
      }, 1000);
    } else if (countdown === 0) {
      setIsLocked(false);
    }
    return () => clearInterval(timer);
  }, [countdown]);

  const resetForm = () => {
    setEmail('');
    setPassword('');
    setVerificationCode('');
    setError('');
    setSuccessMessage('');
    setShowVerification(false);
  };

  const toggleActionType = () => {
    if (actionType === 'giris') {
      setActionType('kaydet');
      if (!isValidEmail) setEmail('');
      if (!Object.values(passwordChecks).every(check => check)) setPassword('');
    } else {
      setActionType('giris');
      setEmail('');
      setPassword('');
    }
    setError('');
    setSuccessMessage('');
  };

  const verifyCode = async (code, isPasswordReset = false) => {
    try {
      const response = await fetch(`${API_BASE_URL}/api/kullanicilar/verify`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          email: email,
          code: code
        })
      });

      const data = await response.json();
      
      // Rate limit kontrolü
      if (response.status === 429) {
        setError(data.message);
        setIsValidCode(false);
        setIsLocked(true);
        setCountdown(60);
        setVerificationCode('');
        return false;
      }

      const isValid = data.success;
      setIsValidCode(isValid);
      
      if (isValid) {
        setIsCodeVerified(true);
        if (!isPasswordReset && data.data?.token) {
          localStorage.setItem('token', data.data.token);
          setUser({
            username: email,
            token: data.data.token
          });
          onSuccess();
          onClose();
        }
      } else {
        setError(data.message || 'Geçersiz doğrulama kodu');
      }
      return isValid;
    } catch (error) {
      console.error('Kod kontrolü hatası:', error);
      setIsValidCode(false);
      setError('Doğrulama sırasında bir hata oluştu');
      return false;
    }
  };

  const renderVerificationInput = (isPasswordReset = false) => (
    <View style={styles.inputContainer}>
      <TextInput
        style={[
          styles.input,
          verificationCode && (isValidCode ? styles.validInput : styles.invalidInput),
          isLocked && styles.lockedInput
        ]}
        placeholder={isLocked ? `${countdown} saniye bekleyin...` : "Doğrulama Kodu"}
        value={verificationCode}
        onChangeText={async (text) => {
          if (!isLocked && !isCodeVerified) {
            setError('');
            setVerificationCode(text);
            if (text.length === 6) {
              await verifyCode(text, isPasswordReset);
            } else {
              setIsValidCode(false);
            }
          }
        }}
        keyboardType="number-pad"
        maxLength={6}
        autoComplete="off"
        textContentType="none"
        editable={!isLocked && !isCodeVerified}
      />
      {isLocked && (
        <Text style={styles.countdownText}>
          {countdown}
        </Text>
      )}
    </View>
  );

  const checkPassword = (password) => {
    const checks = {
      length: password.length >= 8,
      upperCase: /[A-Z]/.test(password),
      lowerCase: /[a-z]/.test(password),
      number: /\d/.test(password),
      special: /[!@#$%^&*(),.?":{}|<>-]/.test(password)
    };
    setPasswordChecks(checks);
    return Object.values(checks).every(check => check);
  };

  const validateEmail = (email) => {
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailRegex.test(email);
  };

  const handleSubmit = async () => {
    try {
      setLoading(true);
      setError('');
      setSuccessMessage('');

      if (!email || !password) {
        setError('Lütfen e-posta ve şifre alanlarını doldurun');
        return;
      }

      if (actionType === 'kaydet' && !Object.values(passwordChecks).every(check => check)) {
        setError('Lütfen tüm şifre gereksinimlerini karşılayın');
        return;
      }

      const endpoint = actionType === 'kaydet' 
        ? `${API_BASE_URL}/api/kullanicilar` 
        : `${API_BASE_URL}/api/kullanicilar/validate`;

      console.log('İstek gönderiliyor:', endpoint);
      
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          kullanici: email,
          sifre: password
        })
      });
      
      const data = await response.json();
      console.log('Sunucu yanıtı:', data);

      if (data.success) {
        if (actionType === 'kaydet') {
          if (data.message.includes('Yeni doğrulama kodu')) {
            setSuccessMessage('Hesabınız zaten mevcut ama doğrulanmamış. Yeni doğrulama kodu e-posta adresinize gönderildi.');
          } else {
            setSuccessMessage('Doğrulama kodu e-posta adresinize gönderildi');
          }
          setShowVerification(true);
        } else {
          if (data.data && data.data.token) {
            localStorage.setItem('token', data.data.token);
            setUser({
              username: email,
              token: data.data.token
            });
            onSuccess();
            onClose();
          } else {
            setError('Giriş yapılamadı: Sunucudan geçerli bir yanıt alınamadı');
          }
        }
      } else {
        if (actionType === 'giris') {
          setError('Giriş başarısız: E-posta veya şifre hatalı. Kayıtlı değilseniz kayıt olabilirsiniz.');
        } else {
          if (data.message.includes('zaten kayıtlı ve doğrulanmış')) {
            setError('Bu e-posta adresi zaten kayıtlı ve doğrulanmış. Lütfen giriş yapın.');
            setActionType('giris');
          } else {
            setError(data.message || 'Kayıt işlemi başarısız oldu. Lütfen farklı bir e-posta adresi deneyin.');
          }
        }
      }
    } catch (error) {
      console.error('Hata:', error);
      setError('Sunucu bağlantısı başarısız oldu. Lütfen internet bağlantınızı kontrol edip tekrar deneyin.');
    } finally {
      setLoading(false);
    }
  };

  const renderPasswordRequirements = () => {
    if ((actionType === 'kaydet' && password) || (showResetPassword && newPassword)) {
      return [
        { key: 'length', text: 'En az 8 karakter' },
        { key: 'upperCase', text: 'En az bir büyük harf' },
        { key: 'lowerCase', text: 'En az bir küçük harf' },
        { key: 'number', text: 'En az bir rakam' },
        { key: 'special', text: 'En az bir özel karakter (!@#$%^&*(),.?":{}|<>-)' }
      ].map(req => (
        <Text
          key={req.key}
          style={[
            styles.requirementText,
            passwordChecks[req.key] ? styles.validRequirement : styles.invalidRequirement
          ]}
        >
          • {req.text}
        </Text>
      ));
    }
    return null;
  };

  const handleForgotPassword = async () => {
    try {
      setLoading(true);
      setError('');
      setIsCodeVerified(false);
      setIsValidCode(false);
      setVerificationCode('');
      setIsLocked(false);
      setCountdown(0);

      if (!email) {
        setError('Lütfen e-posta adresinizi girin');
        return;
      }

      if (!validateEmail(email)) {
        setError('Lütfen geçerli bir e-posta adresi girin');
        return;
      }

      const response = await fetch(`${API_BASE_URL}/api/kullanicilar/forgot-password`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          kullanici: email
        })
      });

      const data = await response.json();

      if (data.success) {
        setSuccessMessage(data.message);
        setShowResetPassword(true);
      } else {
        setError(data.message);
      }
    } catch (error) {
      console.error('Parola sıfırlama hatası:', error);
      setError('Parola sıfırlama işlemi sırasında bir hata oluştu');
    } finally {
      setLoading(false);
    }
  };

  const handleResetPassword = async () => {
    try {
      setLoading(true);
      setError('');

      if (!verificationCode || !newPassword) {
        setError('Lütfen tüm alanları doldurun');
        return;
      }

      if (!Object.values(passwordChecks).every(check => check)) {
        setError('Lütfen tüm şifre gereksinimlerini karşılayın');
        return;
      }

      const response = await fetch(`${API_BASE_URL}/api/kullanicilar/reset-password`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          kullanici: email,
          code: verificationCode,
          yeniSifre: newPassword
        })
      });

      const data = await response.json();

      // Rate limit kontrolü
      if (response.status === 429) {
        setError(data.message);
        setIsValidCode(false);
        setIsLocked(true);
        setCountdown(60); // 60 saniyelik sayaç başlat
        setVerificationCode(''); // Input'u temizle
        return;
      }

      if (data.success) {
        setSuccessMessage('Parolanız başarıyla güncellendi');
        
        // Otomatik giriş yap
        const loginResponse = await fetch(`${API_BASE_URL}/api/kullanicilar/validate`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            kullanici: email,
            sifre: newPassword
          })
        });

        const loginData = await loginResponse.json();

        if (loginData.success) {
          localStorage.setItem('token', loginData.data.token);
          setUser({
            username: email,
            token: loginData.data.token
          });
          onSuccess();
          onClose();
        }
      } else {
        setError(data.message);
      }
    } catch (error) {
      console.error('Parola güncelleme hatası:', error);
      setError('Parola güncellenirken bir hata oluştu');
    } finally {
      setLoading(false);
    }
  };

  const handleBackToLogin = () => {
    setShowResetPassword(false);
    setError('');
    setSuccessMessage('');
    setIsCodeVerified(false);
    setIsValidCode(false);
    setVerificationCode('');
    setIsLocked(false);
    setCountdown(0);
  };

  return (
    <Modal
      visible={visible}
      transparent={true}
      animationType="fade"
    >
      <View style={styles.modalContainer}>
        <View style={styles.modalContent}>
          <TouchableOpacity style={styles.closeButton} onPress={onClose}>
            <Text style={styles.closeButtonText}>×</Text>
          </TouchableOpacity>
          
          {showResetPassword && (
            <TouchableOpacity style={styles.backButton} onPress={handleBackToLogin}>
              <Text style={styles.backButtonText}>←</Text>
            </TouchableOpacity>
          )}
          
          <Text style={styles.title}>
            {showResetPassword ? 'Parola Sıfırlama' : actionType === 'kaydet' ? 'Kullanıcı Kaydı' : 'Kullanıcı Girişi'}
          </Text>
          
          {error ? <Text style={styles.errorText}>{error}</Text> : null}
          {successMessage ? <Text style={styles.successText}>{successMessage}</Text> : null}
          
          {showResetPassword ? (
            <>
              {renderVerificationInput(true)}
              <TextInput
                style={[
                  styles.input,
                  newPassword && (Object.values(passwordChecks).every(check => check) ? styles.validInput : styles.invalidInput)
                ]}
                placeholder="Yeni Şifre"
                value={newPassword}
                onChangeText={(text) => {
                  setNewPassword(text);
                  checkPassword(text);
                }}
                secureTextEntry
                autoComplete="new-password"
                textContentType="newPassword"
                passwordRules="minlength: 8; required: upper; required: lower; required: digit; required: [-]"
              />
              {newPassword && renderPasswordRequirements()}
              <TouchableOpacity 
                style={[styles.button, loading && styles.buttonDisabled]} 
                onPress={handleResetPassword}
                disabled={loading || isLocked}
              >
                <Text style={styles.buttonText}>
                  {loading ? 'İşleniyor...' : 'Şifreyi Güncelle'}
                </Text>
              </TouchableOpacity>
            </>
          ) : showVerification ? (
            <>
              {renderVerificationInput(false)}
              {!isCodeVerified && !isLocked && (
                <TouchableOpacity 
                  style={[styles.button, loading && styles.buttonDisabled]} 
                  onPress={() => verifyCode(verificationCode)}
                  disabled={loading || !verificationCode}
                >
                  <Text style={styles.buttonText}>
                    {loading ? 'İşleniyor...' : 'Doğrula'}
                  </Text>
                </TouchableOpacity>
              )}
            </>
          ) : (
            <>
              <TextInput
                style={[
                  styles.input,
                  email && (isValidEmail ? styles.validInput : styles.invalidInput)
                ]}
                placeholder="E-posta Adresi"
                value={email}
                onChangeText={(text) => {
                  setEmail(text);
                  setIsValidEmail(validateEmail(text));
                  setError('');
                  setSuccessMessage('');
                }}
                keyboardType="email-address"
                autoCapitalize="none"
              />
              
              {actionType === 'giris' && (
                <>
                  <TextInput
                    style={[
                      styles.input,
                      password && (Object.values(passwordChecks).every(check => check) ? styles.validInput : styles.invalidInput)
                    ]}
                    placeholder="Şifre"
                    value={password}
                    onChangeText={(text) => {
                      setPassword(text);
                      checkPassword(text);
                    }}
                    secureTextEntry
                    autoComplete="current-password"
                    textContentType="password"
                  />
                  <TouchableOpacity 
                    style={styles.forgotPasswordButton} 
                    onPress={handleForgotPassword}
                  >
                    <Text style={styles.forgotPasswordText}>
                      Parolamı Unuttum
                    </Text>
                  </TouchableOpacity>
                </>
              )}
              
              {actionType === 'kaydet' && (
                <>
                  <TextInput
                    style={[
                      styles.input,
                      password && (Object.values(passwordChecks).every(check => check) ? styles.validInput : styles.invalidInput)
                    ]}
                    placeholder="Şifre"
                    value={password}
                    onChangeText={(text) => {
                      setPassword(text);
                      checkPassword(text);
                    }}
                    secureTextEntry
                    autoComplete="new-password"
                    textContentType="newPassword"
                    passwordRules="minlength: 8; required: upper; required: lower; required: digit; required: [-]"
                  />
                  {renderPasswordRequirements()}
                </>
              )}
              
              <TouchableOpacity 
                style={[styles.button, loading && styles.buttonDisabled]} 
                onPress={handleSubmit}
                disabled={loading}
              >
                <Text style={styles.buttonText}>
                  {loading ? 'İşleniyor...' : actionType === 'kaydet' ? 'Kayıt Ol' : 'Giriş Yap'}
                </Text>
              </TouchableOpacity>

              <TouchableOpacity 
                style={styles.toggleButton} 
                onPress={toggleActionType}
                disabled={loading}
              >
                <Text style={styles.toggleButtonText}>
                  {actionType === 'kaydet' ? 'Zaten hesabınız var mı? Giriş yapın' : 'Hesabınız yok mu? Kayıt olun'}
                </Text>
              </TouchableOpacity>
            </>
          )}
        </View>
      </View>
    </Modal>
  );
};

const styles = StyleSheet.create({
  modalContainer: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.7)',
    justifyContent: 'center',
    alignItems: 'center',
  },
  modalContent: {
    backgroundColor: '#1a1f25',
    padding: 24,
    borderRadius: 16,
    width: '90%',
    maxWidth: 400,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
  },
  closeButton: {
    position: 'absolute',
    right: 16,
    top: 16,
  },
  closeButtonText: {
    fontSize: 24,
    fontWeight: 'bold',
    color: '#fff',
    opacity: 0.7,
  },
  title: {
    fontSize: 20,
    fontWeight: 'bold',
    marginBottom: 24,
    textAlign: 'center',
    color: '#fff',
  },
  input: {
    backgroundColor: 'rgba(255, 255, 255, 0.1)',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.2)',
    padding: 12,
    marginBottom: 16,
    borderRadius: 8,
    color: '#fff',
    fontSize: 16,
  },
  button: {
    backgroundColor: '#007bff',
    padding: 14,
    borderRadius: 8,
    alignItems: 'center',
    marginBottom: 12,
  },
  buttonDisabled: {
    backgroundColor: 'rgba(0, 123, 255, 0.5)',
  },
  buttonText: {
    color: '#fff',
    fontWeight: '600',
    fontSize: 16,
  },
  errorText: {
    color: '#ff4d4f',
    marginBottom: 16,
    textAlign: 'center',
    fontSize: 14,
  },
  successText: {
    color: '#52c41a',
    marginBottom: 16,
    textAlign: 'center',
    fontSize: 14,
  },
  toggleButton: {
    padding: 12,
  },
  toggleButtonText: {
    color: '#007bff',
    textAlign: 'center',
    fontSize: 14,
  },
  validInput: {
    borderColor: '#52c41a',
    borderWidth: 1,
    color: '#52c41a'
  },
  invalidInput: {
    borderColor: '#ff4d4f',
    borderWidth: 1,
    color: '#ff4d4f'
  },
  requirementText: {
    fontSize: 12,
    marginBottom: 4,
    marginLeft: 4,
  },
  validRequirement: {
    color: '#52c41a',
  },
  invalidRequirement: {
    color: '#ff4d4f',
  },
  forgotPasswordButton: {
    alignSelf: 'flex-end',
    marginBottom: 16,
  },
  forgotPasswordText: {
    color: '#007bff',
    fontSize: 14,
  },
  backButton: {
    position: 'absolute',
    left: 16,
    top: 16,
    zIndex: 1,
  },
  backButtonText: {
    fontSize: 24,
    fontWeight: 'bold',
    color: '#fff',
    opacity: 0.7,
  },
  inputContainer: {
    position: 'relative',
    width: '100%',
  },
  countdownText: {
    position: 'absolute',
    right: 12,
    top: '50%',
    transform: [{ translateY: -10 }],
    color: '#ff4d4f',
    fontWeight: 'bold',
  },
  lockedInput: {
    backgroundColor: 'rgba(255, 77, 79, 0.1)',
    borderColor: '#ff4d4f',
    color: '#ff4d4f',
  },
});

export default LoginModal; 