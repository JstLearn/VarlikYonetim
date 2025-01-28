import React, { useState } from 'react';
import DynamicForm from './DynamicForm';

const BorcForm = ({ onSubmit }) => {
  const [formData, setFormData] = useState({
    duzenlimi: false,
    faiz_binecekmi: false,
    odendi_mi: false,
    talimat_varmi: false,
    borc: '',
    tutar: '',
    para_birimi: 'TRY',
    kalan_taksit: '1',
    odeme_tarihi: new Date().toISOString().split('T')[0],
    bagimli_oldugu_gelir: ''
  });
  
  const [errors, setErrors] = useState({});

  const formFields = [
    {
      id: 'borc',
      label: 'Borç',
      type: 'text',
      placeholder: 'Borç adını girin'
    },
    {
      id: 'duzenlimi',
      label: 'Düzenli mi?',
      type: 'checkbox'
    },
    {
      id: 'tutar',
      label: 'Tutar',
      type: 'number',
      placeholder: '0.00'
    },
    {
      id: 'para_birimi',
      label: 'Para Birimi',
      type: 'text',
      placeholder: 'TRY'
    },
    {
      id: 'kalan_taksit',
      label: 'Kalan Taksit',
      type: 'number',
      placeholder: '1'
    },
    {
      id: 'odeme_tarihi',
      label: 'Ödeme Tarihi',
      type: 'date',
      placeholder: 'GG.AA.YYYY'
    },
    {
      id: 'faiz_binecekmi',
      label: 'Faiz Binecek mi?',
      type: 'checkbox'
    },
    {
      id: 'odendi_mi',
      label: 'Ödendi mi?',
      type: 'checkbox'
    },
    {
      id: 'talimat_varmi',
      label: 'Talimat Var mı?',
      type: 'checkbox'
    },
    {
      id: 'bagimli_oldugu_gelir',
      label: 'Bağımlı Olduğu Gelir',
      type: 'text',
      placeholder: 'Gelir Adı'
    }
  ];

  const handleSubmit = async (data) => {
    try {
      await onSubmit(data);
      // Form başarıyla gönderildiyse formu sıfırla
      setFormData({
        duzenlimi: false,
        faiz_binecekmi: false,
        odendi_mi: false,
        talimat_varmi: false,
        borc: '',
        tutar: '',
        para_birimi: 'TRY',
        kalan_taksit: '1',
        odeme_tarihi: new Date().toISOString().split('T')[0],
        bagimli_oldugu_gelir: ''
      });
      setErrors({});
    } catch (error) {
      // Hata mesajlarını ayarla
      if (error.response?.data?.errors) {
        const newErrors = {};
        error.response.data.errors.forEach(err => {
          newErrors[err.path] = err.msg;
        });
        setErrors(newErrors);
      }
    }
  };

  return (
    <DynamicForm
      formFields={formFields}
      formData={formData}
      setFormData={setFormData}
      errors={errors}
      setErrors={setErrors}
      onSubmit={handleSubmit}
      submitButtonStyle={{
        backgroundColor: '#007AFF',
        padding: 15,
        borderRadius: 8,
        alignItems: 'center',
        marginTop: 20,
      }}
      submitButtonTextStyle={{
        color: '#fff',
        fontSize: 16,
        fontWeight: 'bold',
      }}
    />
  );
};

export default BorcForm; 