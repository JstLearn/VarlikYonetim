// front/components/Forms/FormField.js
import React, { useEffect } from 'react';
import { View, Text, TextInput, TouchableOpacity } from 'react-native';
import styles from '../../styles/styles';

const FormField = ({ field, value, onChange, hasError }) => {
  const { label, type, required } = field;

  const formatDate = (date) => {
    const d = new Date(date);
    return d.toISOString().split('T')[0];
  };

  useEffect(() => {
    if (type === 'date' && !value) {
      onChange(formatDate(new Date()));
    }
  }, []);

  const handleDateChange = (event) => {
    const selectedDate = event.target.value;
    if (selectedDate) {
      onChange(selectedDate);
    }
  };

  const renderInput = () => {
    switch (type) {
      case 'checkbox':
        return (
          <TouchableOpacity
            style={[
              styles.checkboxButton,
              hasError && styles.errorBorder
            ]}
            onPress={() => onChange(!value)}
          >
            <View
              style={{
                width: 16,
                height: 16,
                borderRadius: 4,
                backgroundColor: value ? '#007bff' : 'transparent',
                borderWidth: 1,
                borderColor: value ? '#007bff' : '#fff',
                justifyContent: 'center',
                alignItems: 'center'
              }}
            >
              {value && (
                <Text style={{ color: '#fff', fontSize: 12, lineHeight: 16 }}>✓</Text>
              )}
            </View>
            <Text style={styles.checkboxText}>{label}</Text>
          </TouchableOpacity>
        );

      case 'date':
        const today = new Date().toISOString().split('T')[0];
        return (
          <View style={[styles.formInput, hasError && styles.errorBorder]}>
            <input
              type="date"
              onChange={handleDateChange}
              value={value || today}
              style={{
                width: '100%',
                height: '100%',
                border: 'none',
                outline: 'none',
                fontSize: '16px',
                backgroundColor: 'transparent'
              }}
            />
          </View>
        );

      default:
        return (
          <TextInput
            style={[
              styles.formInput,
              hasError && styles.errorBorder
            ]}
            value={value?.toString() || ''}
            onChangeText={onChange}
            placeholder={`${label} giriniz...`}
            placeholderTextColor="#999"
            keyboardType={type === 'number' ? 'numeric' : 'default'}
          />
        );
    }
  };

  return (
    <View style={{ marginBottom: 10 }}>
      {type !== 'checkbox' && (
        <Text style={styles.formLabel}>
          {label} {required && <Text style={{ color: 'red' }}>*</Text>}
        </Text>
      )}
      {renderInput()}
      {hasError && <Text style={styles.errorText}>{hasError}</Text>}
    </View>
  );
};

export default FormField;
