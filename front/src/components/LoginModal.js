import React, { useState } from 'react';
import { Modal, View, Text, TextInput, TouchableOpacity, StyleSheet } from 'react-native';
import axios from 'axios';

const LoginModal = ({ visible, onClose, onSuccess, actionType: initialActionType }) => {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [actionType, setActionType] = useState(initialActionType);

  const resetForm = () => {
    setEmail('');
    setPassword('');
    setError('');
  };

  const toggleActionType = () => {
    setActionType(prev => prev === 'kaydet' ? 'giris' : 'kaydet');
    resetForm();
  };

  const handleSubmit = async () => {
    try {
      setLoading(true);
      setError('');

      if (!email || !password) {
        setError('Lütfen tüm alanları doldurun');
        return;
      }

      const baseURL = 'http://localhost:3000/api/kullanicilar';
      const endpoint = actionType === 'kaydet' ? baseURL : `${baseURL}/validate`;

      console.log('İstek gönderiliyor:', endpoint);
      
      const response = await axios.post(endpoint, {
        kullanici: email,
        sifre: password
      });
      
      console.log('Sunucu yanıtı:', response.data);

      if (response.data.success) {
        if (actionType === 'kaydet') {
          alert('Kullanıcı başarıyla kaydedildi! Şimdi giriş yapabilirsiniz.');
          setActionType('giris');
          resetForm();
        } else {
          onSuccess(response.data.data);
          onClose();
        }
      } else {
        setError(response.data.message || 'Bir hata oluştu');
      }
    } catch (error) {
      console.error('Hata detayı:', error.response || error);
      setError(
        error.response?.data?.message || 
        error.response?.data?.error || 
        error.message || 
        'Bir hata oluştu'
      );
    } finally {
      setLoading(false);
    }
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
          
          <Text style={styles.title}>
            {actionType === 'kaydet' ? 'Kullanıcı Kaydı' : 'Kullanıcı Girişi'}
          </Text>
          
          {error ? <Text style={styles.errorText}>{error}</Text> : null}
          
          <TextInput
            style={styles.input}
            placeholder="Kullanıcı Adı"
            value={email}
            onChangeText={(text) => {
              setEmail(text);
              setError('');
            }}
          />
          
          <TextInput
            style={styles.input}
            placeholder="Şifre"
            value={password}
            onChangeText={(text) => {
              setPassword(text);
              setError('');
            }}
            secureTextEntry
          />
          
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
        </View>
      </View>
    </Modal>
  );
};

const styles = StyleSheet.create({
  modalContainer: {
    flex: 1,
    backgroundColor: 'rgba(0, 0, 0, 0.5)',
    justifyContent: 'center',
    alignItems: 'center',
  },
  modalContent: {
    backgroundColor: 'white',
    padding: 20,
    borderRadius: 10,
    width: '80%',
    maxWidth: 400,
  },
  closeButton: {
    position: 'absolute',
    right: 10,
    top: 10,
  },
  closeButtonText: {
    fontSize: 24,
    fontWeight: 'bold',
  },
  title: {
    fontSize: 18,
    fontWeight: 'bold',
    marginBottom: 20,
    textAlign: 'center',
  },
  input: {
    borderWidth: 1,
    borderColor: '#ddd',
    padding: 10,
    marginBottom: 10,
    borderRadius: 5,
  },
  button: {
    backgroundColor: '#007AFF',
    padding: 12,
    borderRadius: 5,
    alignItems: 'center',
    marginBottom: 10,
  },
  buttonDisabled: {
    backgroundColor: '#ccc',
  },
  buttonText: {
    color: 'white',
    fontWeight: 'bold',
  },
  errorText: {
    color: 'red',
    marginBottom: 10,
    textAlign: 'center',
  },
  toggleButton: {
    padding: 10,
  },
  toggleButtonText: {
    color: '#007AFF',
    textAlign: 'center',
  }
});

export default LoginModal; 