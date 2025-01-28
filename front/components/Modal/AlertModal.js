// front/components/Modal/AlertModal.js
import React from 'react';
import { View, Text, TouchableOpacity } from 'react-native';

const AlertModal = ({ visible, title, message, onClose, success }) => {
  if (!visible) return null;

  return (
    <View style={{
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0, 0, 0, 0.5)',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      zIndex: 1000,
    }}>
      <View style={{
        backgroundColor: success ? '#28a745' : '#1a1f25',
        borderRadius: 10,
        padding: 20,
        minWidth: 300,
        maxWidth: '90%',
        borderWidth: 1,
        borderColor: success ? '#28a745' : 'rgba(255, 255, 255, 0.2)',
        boxShadow: success ? '0 0 10px rgba(40, 167, 69, 0.5)' : 'none',
      }}>
        <Text style={{
          color: '#fff',
          fontSize: 20,
          fontWeight: 'bold',
          marginBottom: 10,
          textAlign: 'center',
        }}>
          {success ? 'Başarılı' : title}
        </Text>
        <Text style={{
          color: '#fff',
          fontSize: 16,
          marginBottom: 20,
          textAlign: 'center',
        }}>
          {message}
        </Text>
        <TouchableOpacity
          onPress={onClose}
          style={{
            backgroundColor: '#007bff',
            padding: 10,
            borderRadius: 5,
            alignItems: 'center',
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
      </View>
    </View>
  );
};

export default AlertModal;
