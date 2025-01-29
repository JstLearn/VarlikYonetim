import React, { useEffect, useState } from 'react';
import { Image, TouchableOpacity, StyleSheet } from 'react-native';

const Logo = ({ onReset }) => {
    const [isVisible, setIsVisible] = useState(true);
    const [lastScrollY, setLastScrollY] = useState(0);
    const [isHovered, setIsHovered] = useState(false);

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

    const handlePress = () => {
        if (onReset) {
            onReset();
            window.scrollTo(0, 0);
        }
    };

    return (
        <TouchableOpacity
            style={[
                styles.container,
                {
                    opacity: isVisible ? 1 : 0,
                    transform: [{translateY: isVisible ? 0 : -100}],
                    backgroundColor: isHovered ? 'rgba(0, 123, 255, 0.2)' : 'rgba(0, 123, 255, 0.1)',
                    elevation: isHovered ? 4 : 0
                }
            ]}
            onPress={handlePress}
            onHoverIn={() => setIsHovered(true)}
            onHoverOut={() => setIsHovered(false)}
            activeOpacity={0.8}
        >
            <Image
                source={require('../assets/logo.png')}
                style={styles.image}
                resizeMode="contain"
            />
        </TouchableOpacity>
    );
};

const styles = StyleSheet.create({
    container: {
        backgroundColor: 'rgba(0, 123, 255, 0.1)',
        padding: 4,
        borderRadius: 22,
        position: 'fixed',
        top: 10,
        left: 10,
        borderWidth: 1,
        borderColor: 'rgba(0, 123, 255, 0.2)',
        zIndex: 1000,
        width: 44,
        height: 44,
        justifyContent: 'center',
        alignItems: 'center',
        overflow: 'hidden',
        transition: 'all 0.3s ease'
    },
    image: {
        width: '220%',
        height: '220%'
    }
});

export default Logo; 
