const express = require('express');
const database = require('./database');
require('dotenv').config();

const app = express();
const PORT = process.env.API_PORT || 3000;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Logger middleware
app.use((req, res, next) => {
  console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
  next();
});

// Routes

// GET /messages - Récupérer tous les messages
app.get('/messages', async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 100;
    const offset = parseInt(req.query.offset) || 0;
    
    const messages = await database.getAllMessages(limit, offset);
    const stats = await database.getStats();
    
    res.json({
      success: true,
      data: messages,
      pagination: {
        limit,
        offset,
        count: messages.length
      },
      stats
    });
  } catch (error) {
    console.error('Erreur GET /messages:', error);
    res.status(500).json({
      success: false,
      error: 'Erreur lors de la récupération des messages'
    });
  }
});

// GET /messages/:id - Récupérer un message par son ID
app.get('/messages/:id', async (req, res) => {
  try {
    const id = parseInt(req.params.id);
    
    if (isNaN(id)) {
      return res.status(400).json({
        success: false,
        error: 'ID invalide'
      });
    }
    
    const message = await database.getMessageById(id);
    
    if (!message) {
      return res.status(404).json({
        success: false,
        error: 'Message non trouvé'
      });
    }
    
    res.json({
      success: true,
      data: message
    });
  } catch (error) {
    console.error('Erreur GET /messages/:id:', error);
    res.status(500).json({
      success: false,
      error: 'Erreur lors de la récupération du message'
    });
  }
});

// GET /stats - Statistiques
app.get('/stats', async (req, res) => {
  try {
    const stats = await database.getStats();
    res.json({
      success: true,
      data: stats
    });
  } catch (error) {
    console.error('Erreur GET /stats:', error);
    res.status(500).json({
      success: false,
      error: 'Erreur lors de la récupération des statistiques'
    });
  }
});

// GET /health - Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString(),
    services: {
      api: 'running',
      kafka: 'connected',
      database: 'connected'
    }
  });
});

// Démarrage du serveur
app.listen(PORT, () => {
  console.log(`🚀 API REST démarrée sur http://localhost:${PORT}`);
  console.log(`📋 Points d'accès disponibles:`);
  console.log(`   - GET  /messages    - Liste des messages`);
  console.log(`   - GET  /messages/:id - Détail d'un message`);
  console.log(`   - GET  /stats       - Statistiques`);
  console.log(`   - GET  /health      - Health check`);
});