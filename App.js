// App.js
import React, { useState } from 'react';
import {
  SafeAreaView,
  View,
  Text,
  ScrollView,
  TouchableOpacity,
  TextInput,
  Alert,
  FlatList,
  StyleSheet,
} from 'react-native';

export default function App() {
  // Butonların görünürlüğünü yöneten durumlar
  const [showDataButtons, setShowDataButtons] = useState(false);
  const [showQueryButtons, setShowQueryButtons] = useState(false);

  // Form ile ilgili
  const [showForm, setShowForm] = useState(false);
  const [currentEndpoint, setCurrentEndpoint] = useState('');
  const [formFields, setFormFields] = useState([]);
  const [formData, setFormData] = useState({});

  // Tabloyla ilgili
  const [showTable, setShowTable] = useState(false);
  const [tableTitle, setTableTitle] = useState('');
  const [tableData, setTableData] = useState([]);

  // API adresiniz (emülatörde `http://10.0.2.2:3000`, gerçek cihazda IP vb.)
  const BASE_URL = 'http://192.168.1.36:3000';

  // ---------------------------------------------------------
  // Ana butonlar: "Veri Ekle" ve "Verileri Sorgula"
  // ---------------------------------------------------------
  const handleAddData = () => {
    setShowDataButtons(true);
    setShowQueryButtons(false);
    setShowForm(false);
    setShowTable(false);
  };

  const handleQueryData = () => {
    setShowQueryButtons(true);
    setShowDataButtons(false);
    setShowForm(false);
    setShowTable(false);
  };

  // ---------------------------------------------------------
  // Varlık, Borç, Gelir, Gider Ekle Butonları
  // ---------------------------------------------------------
  const handleAddVarlik = () => {
    setCurrentEndpoint('add-varlik');
    setFormFields([
      { label: 'Varlık', id: 'varlik', type: 'text', required: true },
      { label: 'Tür', id: 'tur', type: 'text', required: true },
      { label: 'Nerede', id: 'nerede', type: 'text', required: true },
      { label: 'Alış Tarihi', id: 'alis_tarihi', type: 'date', required: true },
      { label: 'Alış Fiyatı', id: 'alis_fiyati', type: 'number', required: true },
      { label: 'Alış Adedi', id: 'alis_adedi', type: 'number', required: true },
    ]);
    setShowForm(true);
    setShowTable(false);
  };

  const handleAddBorc = () => {
    setCurrentEndpoint('add-borc');
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
    setShowTable(false);
  };

  const handleAddGelir = () => {
    setCurrentEndpoint('add-gelir');
    setFormFields([
      { label: 'Gelir', id: 'gelir', type: 'text', required: true },
      { label: 'Düzenli mi', id: 'duzenlimi', type: 'checkbox' },
      { label: 'Tutar', id: 'tutar', type: 'number', required: true },
      { label: 'Para Birimi', id: 'para_birimi', type: 'text', required: true },
      { label: 'Kalan Taksit', id: 'kalan_taksit', type: 'number' },
      { label: 'Tahsilat Tarihi', id: 'tahsilat_tarihi', type: 'date', required: true },
      { label: 'Faiz Binecek mi', id: 'faiz_binecekmi', type: 'checkbox' },
      { label: 'Alındı mı', id: 'alindi_mi', type: 'checkbox' },
      { label: 'Talimat Var mı', id: 'talimat_varmi', type: 'checkbox' },
      { label: 'Bağımlı Olduğu Gider', id: 'bagimli_oldugu_gider', type: 'text' },
    ]);
    setShowForm(true);
    setShowTable(false);
  };

  const handleAddGider = () => {
    setCurrentEndpoint('add-gider');
    setFormFields([
      { label: 'Borç/Gider', id: 'borc', type: 'text', required: true },
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
    setShowTable(false);
  };

  // ---------------------------------------------------------
  // Formu Gönderme (POST)
  // ---------------------------------------------------------
  const handleSubmitForm = async () => {
    if (!currentEndpoint) {
      Alert.alert('Uyarı', 'Hangi tabloya ekleme yapacağınızı seçmediniz!');
      return;
    }

    try {
      const response = await fetch(`${BASE_URL}/${currentEndpoint}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData),
      });
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const result = await response.json();
      Alert.alert('Başarılı', result.message || 'Kayıt oluşturuldu!');
      // Formu temizle
      setFormData({});
      setShowForm(false);
      setShowDataButtons(false);
    } catch (err) {
      Alert.alert('Hata', `Veri gönderilirken bir hata oluştu: ${err.message}`);
      console.error(err);
    }
  };

  // ---------------------------------------------------------
  // Verileri Sorgulama (GET)
  // ---------------------------------------------------------
  const handleFetchVarlik = async () => {
    try {
      const res = await fetch(`${BASE_URL}/get-all-varlik`);
      if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
      const data = await res.json();
      setTableData(data);
      setTableTitle('Varlıklar');
      setShowTable(true);
      setShowForm(false);
    } catch (error) {
      console.error('Varlık verileri çekilirken hata:', error);
      Alert.alert('Hata', 'Varlık verileri çekilirken hata oluştu.');
    }
  };

  const handleFetchBorc = async () => {
    try {
      const res = await fetch(`${BASE_URL}/get-all-borc`);
      if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
      const data = await res.json();
      setTableData(data);
      setTableTitle('Borçlar');
      setShowTable(true);
      setShowForm(false);
    } catch (error) {
      console.error('Borç verileri çekilirken hata:', error);
      Alert.alert('Hata', 'Borç verileri çekilirken hata oluştu.');
    }
  };

  const handleFetchGelir = async () => {
    try {
      const res = await fetch(`${BASE_URL}/get-all-gelir`);
      if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
      const data = await res.json();
      setTableData(data);
      setTableTitle('Gelirler');
      setShowTable(true);
      setShowForm(false);
    } catch (error) {
      console.error('Gelir verileri çekilirken hata:', error);
      Alert.alert('Hata', 'Gelir verileri çekilirken hata oluştu.');
    }
  };

  const handleFetchGider = async () => {
    try {
      const res = await fetch(`${BASE_URL}/get-all-gider`);
      if (!res.ok) throw new Error(`HTTP error: ${res.status}`);
      const data = await res.json();
      setTableData(data);
      setTableTitle('Giderler');
      setShowTable(true);
      setShowForm(false);
    } catch (error) {
      console.error('Gider verileri çekilirken hata:', error);
      Alert.alert('Hata', 'Gider verileri çekilirken hata oluştu.');
    }
  };

  // ---------------------------------------------------------
  // Dinamik Form Alanını Render Eder
  // ---------------------------------------------------------
  const renderFormField = (field) => {
    const { label, id, type, required } = field;
    const value = formData[id] || '';

    // Checkbox
    if (type === 'checkbox') {
      return (
        <View key={id} style={{ marginBottom: 15 }}>
          <Text style={styles.formLabel}>
            {label}
          </Text>
          <TouchableOpacity
            style={[
              styles.checkboxButton,
              { backgroundColor: value ? '#4caf50' : '#ccc' },
            ]}
            onPress={() => setFormData({ ...formData, [id]: !value })}
          >
            <Text>{value ? 'Evet' : 'Hayır'}</Text>
          </TouchableOpacity>
        </View>
      );
    }

    // Date alanı
    if (type === 'date') {
      return (
        <View key={id} style={{ marginBottom: 15 }}>
          <Text style={styles.formLabel}>
            {label} {required && '*'}
          </Text>
          <TextInput
            style={styles.formInput}
            placeholder="YYYY-MM-DD"
            onChangeText={(txt) => setFormData({ ...formData, [id]: txt })}
            value={value}
          />
        </View>
      );
    }

    // Text veya Number
    return (
      <View key={id} style={{ marginBottom: 15 }}>
        <Text style={styles.formLabel}>
          {label} {required && '*'}
        </Text>
        <TextInput
          keyboardType={type === 'number' ? 'numeric' : 'default'}
          style={styles.formInput}
          placeholder={label}
          onChangeText={(txt) => setFormData({ ...formData, [id]: txt })}
          value={value}
        />
      </View>
    );
  };

  // ---------------------------------------------------------
  // Tablo Elemanlarını Render Eder (FlatList kullandık)
  // ---------------------------------------------------------
  const renderTableItem = ({ item, index }) => {
    // Her satırı "card" gibi gösteriyoruz (Object.keys ile item içindeki tüm sütunları basıyoruz).
    return (
      <View
        style={[
          styles.tableRow,
          { backgroundColor: index % 2 === 0 ? '#333' : '#444' },
        ]}
      >
        {Object.keys(item).map((key) => (
          <Text key={key} style={styles.tableCell}>
            <Text style={{ fontWeight: 'bold' }}>{key}:</Text> {String(item[key])}
          </Text>
        ))}
      </View>
    );
  };

  // ---------------------------------------------------------
  // Ekran Arayüzü
  // ---------------------------------------------------------
  return (
    <SafeAreaView style={styles.container}>
      <ScrollView contentContainerStyle={{ paddingBottom: 40 }}>
        {/* Navbar / Hero benzeri kısım */}
        <View style={{ padding: 20, alignItems: 'center' }}>
          <Text style={styles.heroTitle}>Varlık ve Yokluk Yönetimi</Text>
          <Text style={styles.heroSubtitle}>Kişisel finansınızı modern bir şekilde yönetin.</Text>

          {/* Üst Butonlar */}
          <View style={{ flexDirection: 'row', marginTop: 20 }}>
            <TouchableOpacity style={styles.mainButton} onPress={handleAddData}>
              <Text style={styles.mainButtonText}>Veri Ekle</Text>
            </TouchableOpacity>

            <TouchableOpacity
              style={[styles.mainButton, { marginLeft: 10 }]}
              onPress={handleQueryData}
            >
              <Text style={styles.mainButtonText}>Verileri Sorgula</Text>
            </TouchableOpacity>
          </View>
        </View>

        {/* 1) Varlık/Borç/Gelir/Gider Ekleme Butonları */}
        {showDataButtons && (
          <View style={styles.glassCard}>
            <Text style={styles.cardTitle}>Ne eklemek istersiniz?</Text>
            <View style={styles.flexRowWrap}>
              <TouchableOpacity style={styles.subButton} onPress={handleAddVarlik}>
                <Text style={styles.subButtonText}>Varlık Ekle</Text>
              </TouchableOpacity>

              <TouchableOpacity style={styles.subButton} onPress={handleAddBorc}>
                <Text style={styles.subButtonText}>Borç Ekle</Text>
              </TouchableOpacity>

              <TouchableOpacity style={styles.subButton} onPress={handleAddGelir}>
                <Text style={styles.subButtonText}>Gelir Ekle</Text>
              </TouchableOpacity>

              <TouchableOpacity style={styles.subButton} onPress={handleAddGider}>
                <Text style={styles.subButtonText}>Gider Ekle</Text>
              </TouchableOpacity>
            </View>
          </View>
        )}

        {/* 2) Veri Ekleme Formu */}
        {showForm && (
          <View style={styles.glassCard}>
            <Text style={styles.cardTitle}>Yeni Veri Ekle</Text>
            {formFields.map((field) => renderFormField(field))}

            <TouchableOpacity style={styles.mainButton} onPress={handleSubmitForm}>
              <Text style={styles.mainButtonText}>Ekle</Text>
            </TouchableOpacity>
          </View>
        )}

        {/* 3) Verileri Sorgula Butonları */}
        {showQueryButtons && (
          <View style={styles.glassCard}>
            <Text style={styles.cardTitle}>Hangisini sorgulayacaksınız?</Text>
            <View style={styles.flexRowWrap}>
              <TouchableOpacity style={styles.subButton} onPress={handleFetchVarlik}>
                <Text style={styles.subButtonText}>Varlık Sorgula</Text>
              </TouchableOpacity>

              <TouchableOpacity style={styles.subButton} onPress={handleFetchBorc}>
                <Text style={styles.subButtonText}>Borç Sorgula</Text>
              </TouchableOpacity>

              <TouchableOpacity style={styles.subButton} onPress={handleFetchGelir}>
                <Text style={styles.subButtonText}>Gelir Sorgula</Text>
              </TouchableOpacity>

              <TouchableOpacity style={styles.subButton} onPress={handleFetchGider}>
                <Text style={styles.subButtonText}>Gider Sorgula</Text>
              </TouchableOpacity>
            </View>
          </View>
        )}

        {/* 4) Tablo Görünümü */}
        {showTable && (
          <View style={[styles.glassCard, { maxHeight: 500 }]}>
            <Text style={styles.cardTitle}>{tableTitle}</Text>
            {tableData.length === 0 ? (
              <Text style={{ color: '#fff', textAlign: 'center', marginTop: 10 }}>
                Hiç veri yok.
              </Text>
            ) : (
              <FlatList
                data={tableData}
                renderItem={renderTableItem}
                keyExtractor={(item, index) => index.toString()}
              />
            )}
          </View>
        )}
      </ScrollView>
    </SafeAreaView>
  );
}

// ---------------------------------------------------------
// Stil Tanımları
// ---------------------------------------------------------
const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#0f2027',
  },
  heroTitle: {
    fontSize: 24,
    fontWeight: '700',
    color: '#fff',
    textAlign: 'center',
  },
  heroSubtitle: {
    fontSize: 14,
    color: '#fff',
    marginTop: 5,
    textAlign: 'center',
    opacity: 0.9,
  },
  mainButton: {
    backgroundColor: '#007bff',
    paddingVertical: 12,
    paddingHorizontal: 20,
    borderRadius: 25,
  },
  mainButtonText: {
    color: '#fff',
    fontWeight: '500',
  },
  glassCard: {
    backgroundColor: 'rgba(255, 255, 255, 0.07)',
    borderRadius: 20,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.2)',
    padding: 20,
    marginHorizontal: 20,
    marginBottom: 20,
  },
  cardTitle: {
    color: '#fff',
    fontSize: 16,
    marginBottom: 10,
    fontWeight: 'bold',
    textAlign: 'center',
  },
  flexRowWrap: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    justifyContent: 'center',
  },
  subButton: {
    backgroundColor: '#6c757d',
    paddingVertical: 10,
    paddingHorizontal: 15,
    borderRadius: 25,
    margin: 5,
  },
  subButtonText: {
    color: '#fff',
  },
  formLabel: {
    color: '#fff',
    marginBottom: 3,
    fontWeight: '500',
  },
  formInput: {
    backgroundColor: '#fff',
    borderRadius: 12,
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  checkboxButton: {
    borderRadius: 12,
    paddingHorizontal: 15,
    paddingVertical: 8,
    alignSelf: 'flex-start',
    marginTop: 5,
  },
  tableRow: {
    marginVertical: 2,
    padding: 10,
    borderRadius: 5,
  },
  tableCell: {
    color: '#fff',
    marginBottom: 2,
  },
});
