// front/components/Buttons/MainButton.js
import React from 'react';
import { TouchableOpacity, Text } from 'react-native';
import styles from '../../styles/styles';

const MainButton = ({ title, onPress, style, textStyle }) => {
  return (
    <TouchableOpacity 
      style={[styles.mainButton, style]}
      onPress={onPress}
      activeOpacity={0.8}
    >
      <Text style={[styles.mainButtonText, textStyle]}>{title}</Text>
    </TouchableOpacity>
  );
};

export default MainButton;
