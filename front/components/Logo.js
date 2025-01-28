import React, { useEffect, useState } from 'react';
import { Image } from 'react-native';

const Logo = ({ onReset }) => {
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

    return (
        <div 
            style={{
                backgroundColor: 'rgba(0, 123, 255, 0.1)',
                padding: '4px',
                borderRadius: '50%',
                position: 'fixed',
                top: '10px',
                left: '10px',
                border: '1px solid rgba(0, 123, 255, 0.2)',
                zIndex: 1000,
                cursor: 'pointer',
                transition: 'all 0.3s ease',
                width: '44px',
                height: '44px',
                display: 'flex',
                justifyContent: 'center',
                alignItems: 'center',
                overflow: 'hidden',
                opacity: isVisible ? 1 : 0,
                transform: `translateY(${isVisible ? '0' : '-100px'})`,
                pointerEvents: isVisible ? 'auto' : 'none'
            }}
            onClick={() => {
                if (onReset) {
                    onReset();
                    window.scrollTo(0, 0);
                }
            }}
            onMouseOver={(e) => {
                if (isVisible) {
                    e.currentTarget.style.backgroundColor = 'rgba(0, 123, 255, 0.2)';
                    e.currentTarget.style.transform = 'translateY(-2px)';
                    e.currentTarget.style.boxShadow = '0 4px 8px rgba(0,123,255,0.2)';
                }
            }}
            onMouseOut={(e) => {
                if (isVisible) {
                    e.currentTarget.style.backgroundColor = 'rgba(0, 123, 255, 0.1)';
                    e.currentTarget.style.transform = 'none';
                    e.currentTarget.style.boxShadow = 'none';
                }
            }}
        >
            <img
                src={require('../assets/logo.png')}
                style={{
                    width: '220%',
                    height: '220%',
                    objectFit: 'contain'
                }}
                alt="Logo"
            />
        </div>
    );
};

export default Logo; 
