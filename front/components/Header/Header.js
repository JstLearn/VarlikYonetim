// front/components/Header/Header.js
import React from 'react';
import { View, Text } from 'react-native';
import styles from '../../styles/styles';

const Header = () => {
  return (
    <View style={{ padding: 20, alignItems: 'center' }}>
      <Text style={styles.heroTitle}>Varlık ve Yokluk Yönetimi</Text>
      <Text style={styles.heroSubtitle}>
        Kişisel finansınızı modern bir şekilde yönetin.
      </Text>
    </View>
  );
};

export default Header;
