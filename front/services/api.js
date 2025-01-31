// front/services/api.js
const BASE_URL = 'http://192.168.1.106:3000/api';

// Token işlemleri
const getToken = () => {
  try {
    return localStorage.getItem('token');
  } catch (error) {
    console.error('Token alınamadı:', error);
    return null;
  }
};

const setToken = (token) => {
  try {
    localStorage.setItem('token', token);
  } catch (error) {
    console.error('Token kaydedilemedi:', error);
  }
};

const removeToken = () => {
  try {
    localStorage.removeItem('token');
  } catch (error) {
    console.error('Token silinemedi:', error);
  }
};

// Yeni token kontrolü ve güncelleme
const checkAndUpdateToken = (response) => {
  const newToken = response.headers.get('New-Token');
  if (newToken) {
    setToken(newToken);
    console.log('Token yenilendi');
  }
};

// API istekleri için ortak header'ları oluştur
const getHeaders = () => {
  const headers = { 
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  };

  const token = getToken();
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }

  return headers;
};

// API isteği yap ve token kontrolü
const makeRequest = async (url, options) => {
  try {
    const response = await fetch(url, options);
    
    // Yeni token varsa güncelle
    checkAndUpdateToken(response);
    
    if (!response.ok) {
      if (response.status === 0) {
        throw new Error('Sunucu bağlantısı başarısız oldu. Lütfen internet bağlantınızı ve sunucunun çalıştığını kontrol edin.');
      }
      
      if (response.status === 401) {
        // Token hatası - kullanıcıyı logout yap
        removeToken();
        window.location.href = '/login';
        throw new Error('Oturum süresi doldu. Lütfen tekrar giriş yapın.');
      }
      
      const errorData = await response.json();
      throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
    }
    
    return response;
  } catch (error) {
    console.error('API Error:', error);
    throw error;
  }
};

export const postData = async (endpoint, data) => {
  try {
    console.log('POST isteği:', `${BASE_URL}/${endpoint}`, data);
    
    const response = await makeRequest(`${BASE_URL}/${endpoint}`, {
      method: 'POST',
      headers: getHeaders(),
      body: JSON.stringify(data)
    });
    
    const responseData = await response.json();
    console.log('POST yanıtı:', responseData);

    if (endpoint === 'kullanicilar/validate' && responseData.success && responseData.data.token) {
      setToken(responseData.data.token);
    }

    return responseData;
  } catch (error) {
    console.error('API Error:', error);
    throw error;
  }
};

export const fetchData = async (endpoint) => {
  try {
    const url = `${BASE_URL}/${endpoint}`;
    console.log('GET isteği:', url);
    
    const response = await makeRequest(url, {
      method: 'GET',
      headers: getHeaders()
    });
    
    const data = await response.json();
    console.log('GET yanıtı:', data);
    return data;
  } catch (error) {
    console.error('API Error:', error);
    throw error;
  }
};
