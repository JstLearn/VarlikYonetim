// front/components/Header/Header.js
import React from 'react';
import { View, Text } from 'react-native';
import styles from '../../styles/styles';

const Header = () => {
  return (
    <View style={{ 
      padding: 20, 
      paddingTop: 60,
      alignItems: 'center',
      position: 'relative',
      zIndex: 1,
      width: '100%',
      maxWidth: 1200,
      marginLeft: 'auto',
      marginRight: 'auto'
    }}>
      <Text style={styles.heroTitle}>Varlık ve Yokluk Yönetimi</Text>
      <Text style={styles.heroSubtitle}>
        Kişisel finansınızı modern bir şekilde yönetin.
      </Text>
    </View>
  );
};

export default Header;
