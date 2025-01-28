// front/components/Modal/AlertModal.js
import React, { useEffect, useState } from 'react';
import { View, Text, TouchableOpacity, Animated } from 'react-native';

const AlertModal = ({ visible, title, message, onClose, success }) => {
  const [progress] = useState(new Animated.Value(0));
  
  useEffect(() => {
    if (visible && success) {
      // Progress barı 2 saniyede doldur
      Animated.timing(progress, {
        toValue: 100,
        duration: 2000,
        useNativeDriver: false
      }).start();

      // 2 saniye sonra modalı kapat
      const timer = setTimeout(() => {
        onClose();
      }, 2000);

      return () => clearTimeout(timer);
    }
  }, [visible, success]);

  if (!visible) return null;

  return (
    <View style={{
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0, 0, 0, 0.7)',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      zIndex: 1000,
    }}>
      <View style={{
        backgroundColor: '#1a1f25',
        borderRadius: 12,
        padding: 24,
        minWidth: 320,
        maxWidth: '90%',
        borderWidth: 1,
        borderColor: success ? '#007bff' : 'rgba(255, 255, 255, 0.1)',
        boxShadow: '0 8px 24px rgba(0, 0, 0, 0.2)',
        position: 'relative',
        overflow: 'hidden'
      }}>
        {success && (
          <Animated.View style={{
            position: 'absolute',
            bottom: 0,
            left: 0,
            height: 3,
            backgroundColor: '#007bff',
            width: progress.interpolate({
              inputRange: [0, 100],
              outputRange: ['0%', '100%']
            }),
            boxShadow: '0 0 8px rgba(0, 123, 255, 0.5)'
          }} />
        )}
        <View style={{
          width: 56,
          height: 56,
          borderRadius: 28,
          backgroundColor: success ? '#007bff' : '#dc3545',
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          marginBottom: 20,
          marginLeft: 'auto',
          marginRight: 'auto',
          boxShadow: success ? '0 0 16px rgba(0, 123, 255, 0.3)' : '0 0 16px rgba(220, 53, 69, 0.3)'
        }}>
          {success ? (
            <Text style={{ fontSize: 28, color: '#fff' }}>✓</Text>
          ) : (
            <Text style={{ fontSize: 28, color: '#fff' }}>!</Text>
          )}
        </View>
        <Text style={{
          color: '#fff',
          fontSize: 22,
          fontWeight: 'bold',
          marginBottom: 12,
          textAlign: 'center',
        }}>
          {success ? 'Başarılı' : title}
        </Text>
        <Text style={{
          color: 'rgba(255, 255, 255, 0.7)',
          fontSize: 16,
          marginBottom: success ? 8 : 24,
          textAlign: 'center',
          lineHeight: 22,
        }}>
          {message}
        </Text>
        {!success && (
          <TouchableOpacity
            onPress={onClose}
            style={{
              backgroundColor: '#007bff',
              padding: 14,
              borderRadius: 8,
              alignItems: 'center',
              transition: 'all 0.2s ease',
              boxShadow: '0 4px 12px rgba(0, 123, 255, 0.2)'
            }}
          >
            <Text style={{
              color: '#fff',
              fontSize: 16,
              fontWeight: 'bold',
            }}>
              Tamam
            </Text>
          </TouchableOpacity>
        )}
      </View>
    </View>
  );
};

export default AlertModal;
