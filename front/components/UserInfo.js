import React from 'react';
import { View, Text, TouchableOpacity, StyleSheet } from 'react-native';
import { useUser } from '../context/UserContext';

const UserInfo = ({ onLogout }) => {
    const { user, logout } = useUser();

    if (!user) return null;

    const handleLogout = () => {
        logout();
        if (onLogout) onLogout();
    };

    return (
        <View style={styles.container}>
            <Text style={styles.username}>Hoş geldiniz, {user.username}</Text>
            <TouchableOpacity style={styles.logoutButton} onPress={handleLogout}>
                <Text style={styles.logoutText}>Çıkış Yap</Text>
            </TouchableOpacity>
        </View>
    );
};

const styles = StyleSheet.create({
    container: {
        backgroundColor: 'rgba(0, 123, 255, 0.1)',
        padding: 8,
        paddingHorizontal: 16,
        borderRadius: 20,
        position: 'absolute',
        top: 10,
        right: 10,
        borderWidth: 1,
        borderColor: 'rgba(0, 123, 255, 0.2)',
        zIndex: 1000,
        flexDirection: 'row',
        alignItems: 'center',
        gap: 10,
    },
    username: {
        color: '#fff',
        fontSize: 14,
        fontWeight: '500',
    },
    logoutButton: {
        backgroundColor: 'rgba(255, 59, 48, 0.2)',
        paddingVertical: 4,
        paddingHorizontal: 8,
        borderRadius: 12,
        borderWidth: 1,
        borderColor: 'rgba(255, 59, 48, 0.3)',
    },
    logoutText: {
        color: '#fff',
        fontSize: 12,
        fontWeight: '500',
    }
});

export default UserInfo; 