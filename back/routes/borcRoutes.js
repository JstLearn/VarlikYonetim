// back/routes/borcRoutes.js
const express = require("express");
const router = express.Router();
const { addBorc, getAllBorc } = require("../controllers/borcController");
const authMiddleware = require("../middleware/authMiddleware");

// Tüm route'lar için auth middleware'ini kullan
router.use(authMiddleware);

// GET /api/borc
router.get("/", getAllBorc);

// POST /api/borc
router.post("/", addBorc);

module.exports = router;
