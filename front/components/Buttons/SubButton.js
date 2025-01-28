// front/components/Buttons/SubButton.js
import React from 'react';
import { TouchableOpacity, Text } from 'react-native';
import styles from '../../styles/styles';

const SubButton = ({ onPress, title, style }) => {
  return (
    <TouchableOpacity style={[styles.subButton, style]} onPress={onPress}>
      <Text style={styles.subButtonText}>{title}</Text>
    </TouchableOpacity>
  );
};

export default SubButton;
