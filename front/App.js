// front/App.js
import React, { useState } from 'react';
import { SafeAreaView, View, Text, ScrollView, Alert, Animated, TouchableOpacity } from 'react-native';
import Header from './components/Header/Header';
import UserInfo from './components/UserInfo';
import MainButton from './components/Buttons/MainButton';
import SubButton from './components/Buttons/SubButton';
import DynamicForm from './components/Forms/DynamicForm';
import DataTable from './components/Tables/DataTable';
import styles from './styles/styles';
import { postData, fetchData } from './services/api';
import AlertModal from './components/Modal/AlertModal';
import LoginModal from './components/LoginModal';
import { UserProvider, useUser } from './context/UserContext';

const AppContent = () => {
  // State tanımlamaları
  const [isMainButtonsSmall, setIsMainButtonsSmall] = useState(false);
  const [showDataButtons, setShowDataButtons] = useState(false);
  const [showQueryButtons, setShowQueryButtons] = useState(false);
  const [showForm, setShowForm] = useState(false);
  const [currentEndpoint, setCurrentEndpoint] = useState('');
  const [formFields, setFormFields] = useState([]);
  const [formData, setFormData] = useState({});
  const [errors, setErrors] = useState({});
  const [showTable, setShowTable] = useState(false);
  const [tableTitle, setTableTitle] = useState('');
  const [tableData, setTableData] = useState([]);
  const [alertModal, setAlertModal] = useState({
    visible: false,
    title: '',
    message: '',
    onClose: null
  });
  const [isLoginModalVisible, setLoginModalVisible] = useState(false);
  const [currentAction, setCurrentAction] = useState('');
  const { user } = useUser();

  // Ana butonların işleyicileri
  const handleAddData = () => {
    setIsMainButtonsSmall(true);
    setShowDataButtons(true);
    setShowQueryButtons(false);
    setShowForm(false);
    setShowTable(false);
  };

  const handleQueryData = () => {
    setIsMainButtonsSmall(true);
    setShowQueryButtons(true);
    setShowDataButtons(false);
    setShowForm(false);
    setShowTable(false);
  };

  // Alt menü butonlarının işleyicileri
  const handleAddVarlik = () => {
    setCurrentEndpoint('varlik');
    setFormFields([
      { label: 'Varlık', id: 'varlik', type: 'text', required: true },
      { label: 'Tür', id: 'tur', type: 'text', required: true },
      { label: 'Nerede', id: 'nerede', type: 'text', required: true },
      { label: 'Alış Tarihi', id: 'alis_tarihi', type: 'date', required: true },
      { label: 'Alış Fiyatı', id: 'alis_fiyati', type: 'number', required: true },
      { label: 'Alış Adedi', id: 'alis_adedi', type: 'number', required: true },
    ]);
    setShowForm(true);
  };

  const handleAddBorc = () => {
    setCurrentEndpoint('borc');
    setFormFields([
      { label: 'Borç', id: 'borc', type: 'text', required: true },
      { label: 'Düzenli mi', id: 'duzenlimi', type: 'checkbox' },
      { label: 'Tutar', id: 'tutar', type: 'number', required: true },
      { label: 'Para Birimi', id: 'para_birimi', type: 'text', required: true },
      { label: 'Kalan Taksit', id: 'kalan_taksit', type: 'number', required: true },
      { label: 'Ödeme Tarihi', id: 'odeme_tarihi', type: 'date', required: true },
      { label: 'Faiz Binecek mi', id: 'faiz_binecekmi', type: 'checkbox' },
      { label: 'Ödendi mi', id: 'odendi_mi', type: 'checkbox' },
      { label: 'Talimat Var mı', id: 'talimat_varmi', type: 'checkbox' },
      { label: 'Bağımlı Olduğu Gelir', id: 'bagimli_oldugu_gelir', type: 'text' },
    ]);
    setShowForm(true);
  };

  const handleAddGelir = () => {
    setCurrentEndpoint('gelir');
    setFormFields([
      { label: 'Gelir', id: 'gelir', type: 'text', required: true },
      { label: 'Düzenli mi', id: 'duzenlimi', type: 'checkbox' },
      { label: 'Tutar', id: 'tutar', type: 'number', required: true },
      { label: 'Para Birimi', id: 'para_birimi', type: 'text', required: true },
      { label: 'Kalan Taksit', id: 'kalan_taksit', type: 'number', required: true },
      { label: 'Tahsilat Tarihi', id: 'tahsilat_tarihi', type: 'date', required: true },
      { label: 'Faiz Binecek mi', id: 'faiz_binecekmi', type: 'checkbox' },
      { label: 'Alındı mı', id: 'alindi_mi', type: 'checkbox' },
      { label: 'Talimat Var mı', id: 'talimat_varmi', type: 'checkbox' },
      { label: 'Bağımlı Olduğu Gider', id: 'bagimli_oldugu_gider', type: 'text' },
    ]);
    setShowForm(true);
  };

  const handleAddGider = () => {
    setCurrentEndpoint('gider');
    setFormFields([
      { label: 'Gider', id: 'gider', type: 'text', required: true },
      { label: 'Düzenli mi', id: 'duzenlimi', type: 'checkbox' },
      { label: 'Tutar', id: 'tutar', type: 'number', required: true },
      { label: 'Para Birimi', id: 'para_birimi', type: 'text', required: true },
      { label: 'Kalan Taksit', id: 'kalan_taksit', type: 'number', required: true },
      { label: 'Ödeme Tarihi', id: 'odeme_tarihi', type: 'date', required: true },
      { label: 'Faiz Binecek mi', id: 'faiz_binecekmi', type: 'checkbox' },
      { label: 'Ödendi mi', id: 'odendi_mi', type: 'checkbox' },
      { label: 'Talimat Var mı', id: 'talimat_varmi', type: 'checkbox' },
      { label: 'Bağımlı Olduğu Gelir', id: 'bagimli_oldugu_gelir', type: 'text' },
    ]);
    setShowForm(true);
  };

  // Form verilerini doğrulama fonksiyonu
  const validateFormData = (data, fields) => {
    const errors = {};
    
    fields.forEach((field) => {
      const value = data[field.id];
      
      // Zorunlu alan kontrolü
      if (field.required) {
        if (field.type === 'checkbox' && value !== true && value !== false) {
          errors[field.id] = 'Bu alan zorunludur';
        } else if (field.type !== 'checkbox' && (!value || value.toString().trim() === '')) {
          errors[field.id] = 'Bu alan zorunludur';
        }
      }
      
      // Sayısal değer kontrolü
      if (field.type === 'number' && value) {
        if (isNaN(Number(value))) {
          errors[field.id] = 'Geçerli bir sayı giriniz';
        }
      }
      
      // Tarih formatı kontrolü
      if (field.type === 'date' && value) {
        const date = new Date(value);
        if (isNaN(date.getTime())) {
          errors[field.id] = 'Geçerli bir tarih giriniz';
        }
      }
    });
    
    return errors;
  };

  const showAlert = (title, message, onClose, success = false) => {
    setAlertModal({
      visible: true,
      title,
      message,
      onClose: () => {
        // Modalı kapat
        setAlertModal(prev => ({ ...prev, visible: false }));
        
        // Sadece başarılı durumda form verilerini temizle ve görünümü sıfırla
        if (success) {
          setFormData({});
          setErrors({});
          setCurrentEndpoint('');
          setShowTable(false);
          setShowForm(false);
          setShowDataButtons(false);
          setShowQueryButtons(false);
        }
        
        // Callback'i çağır
        if (onClose) onClose();
      },
      success
    });
  };

  // ---------------------------------------------------------
  // Formu Gönderme (POST)
  // ---------------------------------------------------------
  const handleSubmitForm = async () => {
    try {
      // Form verilerini kontrol et
      const validationErrors = validateFormData(formData, formFields);
      if (Object.keys(validationErrors).length > 0) {
        setErrors(validationErrors);
        showAlert('Uyarı', 'Lütfen tüm zorunlu alanları doldurun ve değerleri doğru formatta girin.', null, false);
        return;
      }

      // Endpoint'i sakla
      const endpoint = currentEndpoint;

      // Veriyi gönder
      const response = await postData(endpoint, formData);
      
      if (response && response.success) {
        showAlert('Başarılı', 'Veri başarıyla eklendi.', async () => {
          // Eklenen verinin türüne göre ilgili tabloyu göster
          if (endpoint === 'varlik') {
            await handleFetchVarlik();
          } else if (endpoint === 'borc') {
            await handleFetchBorc();
          } else if (endpoint === 'gelir') {
            await handleFetchGelir();
          } else if (endpoint === 'gider') {
            await handleFetchGider();
          }

          // Ana butonları küçült
          setIsMainButtonsSmall(true);
        }, true);
      } else {
        showAlert('Uyarı', response?.message || 'Veri eklenirken bir hata oluştu.', null, false);
      }
    } catch (error) {
      console.error('Form gönderme hatası:', error);
      showAlert('Uyarı', 'Veri eklenirken bir hata oluştu. Lütfen tekrar deneyin.', null, false);
    }
  };

  // ---------------------------------------------------------
  // Verileri Sorgulama (GET)
  // ---------------------------------------------------------
  const handleFetchVarlik = async () => {
    try {
      setShowTable(false);
      const response = await fetchData('varlik');
      if (response && Array.isArray(response.data)) {
        setTableData(response.data);
        setTableTitle('Varlıklar');
        setShowTable(true);
        setShowForm(false);
      } else {
        throw new Error('Geçersiz veri formatı');
      }
    } catch (error) {
      console.error('Varlık verileri çekilirken hata:', error);
      showAlert('Hata', 'Varlık verileri çekilirken hata oluştu: ' + error.message);
    }
  };

  const handleFetchBorc = async () => {
    try {
      setShowTable(false);
      const response = await fetchData('borc');
      if (response && Array.isArray(response.data)) {
        setTableData(response.data);
        setTableTitle('Borçlar');
        setShowTable(true);
        setShowForm(false);
      } else {
        throw new Error('Geçersiz veri formatı');
      }
    } catch (error) {
      console.error('Borç verileri çekilirken hata:', error);
      showAlert('Hata', 'Borç verileri çekilirken hata oluştu: ' + error.message);
    }
  };

  const handleFetchGelir = async () => {
    try {
      setShowTable(false);
      const response = await fetchData('gelir');
      if (response && Array.isArray(response.data)) {
        setTableData(response.data);
        setTableTitle('Gelirler');
        setShowTable(true);
        setShowForm(false);
      } else {
        throw new Error('Geçersiz veri formatı');
      }
    } catch (error) {
      console.error('Gelir verileri çekilirken hata:', error);
      showAlert('Hata', 'Gelir verileri çekilirken hata oluştu: ' + error.message);
    }
  };

  const handleFetchGider = async () => {
    try {
      setShowTable(false);
      const response = await fetchData('gider');
      if (response && Array.isArray(response.data)) {
        setTableData(response.data);
        setTableTitle('Giderler');
        setShowTable(true);
        setShowForm(false);
      } else {
        throw new Error('Geçersiz veri formatı');
      }
    } catch (error) {
      console.error('Gider verileri çekilirken hata:', error);
      showAlert('Hata', 'Gider verileri çekilirken hata oluştu: ' + error.message);
    }
  };

  const handleActionClick = (action) => {
    if (user) {
      // Kullanıcı giriş yapmışsa direkt işlemi başlat
      if (action === 'ekle') {
        handleAddData();
      } else if (action === 'sorgula') {
        handleQueryData();
      }
    } else {
      // Kullanıcı giriş yapmamışsa modal'ı göster
      setCurrentAction(action);
      setLoginModalVisible(true);
    }
  };

  const handleModalClose = () => {
    setLoginModalVisible(false);
    setCurrentAction('');
    resetAllStates(); // Ana ekrana dönmek için tüm state'leri sıfırla
  };

  const resetAllStates = () => {
    setIsMainButtonsSmall(false);
    setShowDataButtons(false);
    setShowQueryButtons(false);
    setShowForm(false);
    setShowTable(false);
    setCurrentEndpoint('');
    setFormFields([]);
    setFormData({});
    setErrors({});
    setTableTitle('');
    setTableData([]);
  };

  const handleModalSuccess = () => {
    // Kullanıcı girişi başarılı olduğunda yapılacak işlemler
    if (currentAction === 'ekle') {
      handleAddData();
    } else if (currentAction === 'sorgula') {
      handleQueryData();
    }
    setLoginModalVisible(false);
    setCurrentAction('');
  };

  return (
    <SafeAreaView style={styles.container}>
      <UserInfo onLogout={resetAllStates} />
      <ScrollView 
        contentContainerStyle={styles.scrollViewContent}
        showsVerticalScrollIndicator={true}
        showsHorizontalScrollIndicator={true}
      >
        <Header />

        {/* Ana Butonlar */}
        <View style={[
          styles.mainButtonsContainer,
          isMainButtonsSmall && styles.mainButtonsContainerSmall
        ]}>
          <MainButton 
            title="Ekle" 
            onPress={() => handleActionClick('ekle')}
            style={[
              styles.mainButton,
              isMainButtonsSmall && styles.mainButtonSmall
            ]}
            textStyle={[
              styles.mainButtonText,
              isMainButtonsSmall && styles.mainButtonTextSmall
            ]}
          />
          <MainButton
            title="Sorgula"
            onPress={() => handleActionClick('sorgula')}
            style={[
              styles.mainButton,
              isMainButtonsSmall && styles.mainButtonSmall
            ]}
            textStyle={[
              styles.mainButtonText,
              isMainButtonsSmall && styles.mainButtonTextSmall
            ]}
          />
        </View>

        {/* Alt Menüler */}
        {showDataButtons && !showForm && (
          <View style={styles.glassCard}>
            <Text style={styles.cardTitle}>Ne eklemek istersiniz?</Text>
            <View style={styles.flexRowWrap}>
              <SubButton title="Varlık Ekle" onPress={handleAddVarlik} />
              <SubButton title="Borç Ekle" onPress={handleAddBorc} />
              <SubButton title="Gelir Ekle" onPress={handleAddGelir} />
              <SubButton title="Gider Ekle" onPress={handleAddGider} />
            </View>
          </View>
        )}

        {/* Sorgulama Butonları */}
        {showQueryButtons && !showTable && (
          <View style={styles.glassCard}>
            <Text style={styles.cardTitle}>Hangisini sorgulayacaksınız?</Text>
            <View style={styles.flexRowWrap}>
              <SubButton title="Varlık Sorgula" onPress={handleFetchVarlik} />
              <SubButton title="Borç Sorgula" onPress={handleFetchBorc} />
              <SubButton title="Gelir Sorgula" onPress={handleFetchGelir} />
              <SubButton title="Gider Sorgula" onPress={handleFetchGider} />
            </View>
          </View>
        )}

        {/* Form */}
        {showForm && (
          <View style={styles.glassCard}>
            <Text style={styles.cardTitle}>Yeni Veri Ekle</Text>
            <DynamicForm
              formFields={formFields}
              formData={formData}
              setFormData={setFormData}
              errors={errors}
              setErrors={setErrors}
              onSubmit={handleSubmitForm}
              submitButtonStyle={styles.formSubmitButton}
              submitButtonTextStyle={styles.formSubmitButtonText}
            />
          </View>
        )}

        {/* Tablo */}
        {showTable && (
          <View style={styles.glassCard}>
            <DataTable data={tableData} title={tableTitle} />
          </View>
        )}
      </ScrollView>

      <AlertModal
        visible={alertModal.visible}
        title={alertModal.title}
        message={alertModal.message}
        onClose={alertModal.onClose}
        success={alertModal.title === 'Başarılı'}
      />

      <LoginModal
        visible={isLoginModalVisible}
        onClose={handleModalClose}
        onSuccess={handleModalSuccess}
        actionType={currentAction === 'ekle' ? 'kaydet' : 'sorgula'}
      />
    </SafeAreaView>
  );
};

export default function App() {
  return (
    <UserProvider>
      <AppContent />
    </UserProvider>
  );
}
