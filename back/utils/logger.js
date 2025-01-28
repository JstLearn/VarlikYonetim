// back/utils/logger.js
const morgan = require('morgan');

const logger = {
  info: (...params) => {
    console.log(...params);
  },
  error: (...params) => {
    console.error(...params);
  },
};

// Middleware olarak Morgan'u kullanmak için hazır hale getirme
const morganMiddleware = morgan('dev');

module.exports = { logger, morganMiddleware };
