import React, { useState, useEffect } from 'react';
import { View, Text, TouchableOpacity, StyleSheet } from 'react-native';
import { useUser } from '../context/UserContext';

const UserInfo = ({ onLogout }) => {
    const { user, logout } = useUser();
    const [isVisible, setIsVisible] = useState(true);
    const [lastScrollY, setLastScrollY] = useState(0);

    useEffect(() => {
        const handleScroll = () => {
            const currentScrollY = window.scrollY;
            
            if (currentScrollY < lastScrollY || currentScrollY < 100) {
                setIsVisible(true);
            } else if (currentScrollY > lastScrollY && currentScrollY > 100) {
                setIsVisible(false);
            }
            
            setLastScrollY(currentScrollY);
        };

        window.addEventListener('scroll', handleScroll, { passive: true });
        return () => window.removeEventListener('scroll', handleScroll);
    }, [lastScrollY]);

    if (!user) return null;

    const handleLogout = () => {
        logout();
        if (onLogout) onLogout();
    };

    return (
        <View style={[
            styles.container,
            {
                transform: [{ translateY: isVisible ? 0 : -100 }],
                opacity: isVisible ? 1 : 0
            }
        ]}>
            <Text style={styles.username}>{user.username}</Text>
            <TouchableOpacity 
                style={styles.logoutButton} 
                onPress={handleLogout}
                activeOpacity={0.8}
            >
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
        position: 'fixed',
        top: 10,
        right: 10,
        borderWidth: 1,
        borderColor: 'rgba(0, 123, 255, 0.2)',
        zIndex: 1000,
        flexDirection: 'row',
        alignItems: 'center',
        gap: 10,
        transition: 'all 0.3s ease'
    },
    username: {
        color: '#fff',
        fontSize: 14,
        fontWeight: '500'
    },
    logoutButton: {
        backgroundColor: 'rgba(255, 59, 48, 0.2)',
        paddingVertical: 4,
        paddingHorizontal: 8,
        borderRadius: 12,
        borderWidth: 1,
        borderColor: 'rgba(255, 59, 48, 0.3)'
    },
    logoutText: {
        color: '#fff',
        fontSize: 12,
        fontWeight: '500'
    }
});

export default UserInfo; 