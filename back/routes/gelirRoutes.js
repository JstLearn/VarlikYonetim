// back/routes/gelirRoutes.js
const express = require("express");
const router = express.Router();
const { addGelir, getAllGelir } = require("../controllers/gelirController");
const authMiddleware = require("../middleware/authMiddleware");

// Tüm route'lar için auth middleware'ini kullan
router.use(authMiddleware);

// GET /api/gelir
router.get("/", getAllGelir);

// POST /api/gelir
router.post("/", addGelir);

module.exports = router;
