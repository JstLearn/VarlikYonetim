// front/components/Forms/DynamicForm.js
import React from 'react';
import { View, TouchableOpacity, Text, ScrollView } from 'react-native';
import FormField from './FormField';
import styles from '../../styles/styles';

const DynamicForm = ({ 
  formFields, 
  formData, 
  setFormData, 
  errors, 
  setErrors, 
  onSubmit,
  submitButtonStyle,
  submitButtonTextStyle
}) => {
  const handleChange = (id, value) => {
    setFormData({ 
      ...formData, 
      [id]: value
    });

    if (errors[id]) {
      setErrors({ ...errors, [id]: null });
    }
  };

  const handleSubmit = () => {
    // Form verilerini hazırla
    const preparedData = {};

    // Tüm form alanlarını kontrol et ve varsayılan değerleri ekle
    formFields.forEach(field => {
      // Eğer değer girilmişse onu kullan, girilmemişse varsayılan değer ata
      if (field.type === 'checkbox') {
        // Checkbox değerlerini 1/0 olarak gönder
        preparedData[field.id] = formData[field.id] ? 1 : 0;
      } else if (field.type === 'number') {
        preparedData[field.id] = formData[field.id] ? parseFloat(formData[field.id]) : 0;
      } else if (field.type === 'date') {
        preparedData[field.id] = formData[field.id] || new Date().toISOString();
      } else {
        // Text alanları için boş string varsayılan değer
        preparedData[field.id] = (formData[field.id] || '').toString().trim();
      }
    });

    console.log('Gönderilecek veriler:', preparedData);
    onSubmit(preparedData);
  };

  const renderFormField = (field) => {
    const { id } = field;
    const value = formData[id];
    const hasError = errors[id];

    return (
      <View key={id} style={styles.formGroup}>
        <FormField
          field={field}
          value={value}
          onChange={(val) => handleChange(id, val)}
          hasError={hasError}
        />
      </View>
    );
  };

  // Form alanlarını türlerine göre ayır
  const textFields = formFields.filter(field => field.type === 'text');
  const otherFields = formFields.filter(field => field.type !== 'text');

  return (
    <ScrollView>
      <View style={styles.formContainer}>
        <View style={styles.formRow}>
          <View style={styles.formColumn}>
            {textFields.map((field) => renderFormField(field))}
          </View>
          <View style={styles.formColumn}>
            {otherFields.map((field) => renderFormField(field))}
          </View>
        </View>
      </View>
      <TouchableOpacity
        style={submitButtonStyle}
        onPress={handleSubmit}
        activeOpacity={0.8}
      >
        <Text style={submitButtonTextStyle}>Ekle</Text>
      </TouchableOpacity>
    </ScrollView>
  );
};

export default DynamicForm;
