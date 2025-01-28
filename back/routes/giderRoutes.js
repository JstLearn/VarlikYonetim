// back/routes/giderRoutes.js
const express = require("express");
const router = express.Router();
const { addGider, getAllGider } = require("../controllers/giderController");
const authMiddleware = require("../middleware/authMiddleware");

// Tüm route'lar için auth middleware'ini kullan
router.use(authMiddleware);

// GET /api/gider
router.get("/", getAllGider);

// POST /api/gider
router.post("/", addGider);

module.exports = router;
