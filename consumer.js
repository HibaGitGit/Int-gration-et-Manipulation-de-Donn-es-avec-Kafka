const { Kafka } = require('kafkajs');
const database = require('./database');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'tp9-consumer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'tp9-consumer-group' });
const topic = process.env.KAFKA_TOPIC || 'test-topic';

// Initialisation de la base de données
const initDatabase = async () => {
  await database.createTable();
  console.log('Base de données initialisée');
};

const run = async () => {
  try {
    // Initialiser la base de données
    await initDatabase();
    
    // Se connecter au consommateur
    await consumer.connect();
    console.log('Consommateur Kafka connecté');
    
    // S'abonner au topic
    await consumer.subscribe({ 
      topic: topic, 
      fromBeginning: false // Ne pas lire les anciens messages
    });
    
    console.log(`Consommateur abonné au topic: ${topic}`);
    
    // Traiter les messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          // Parser le message
          const key = message.key ? message.key.toString() : null;
          const payload = JSON.parse(message.value.toString());
          const offset = parseInt(message.offset);
          
          console.log('Message reçu:', {
            topic,
            partition,
            offset,
            key,
            payload
          });
          
          // Sauvegarder dans PostgreSQL
          const savedMessage = await database.saveMessage(
            topic,
            partition,
            offset,
            key,
            payload
          );
          
          console.log(` Message sauvegardé en base avec ID: ${savedMessage.id}`);
          
        } catch (error) {
          console.error(' Erreur lors du traitement du message:', error);
        }
      },
    });
    
  } catch (error) {
    console.error('Erreur du consommateur:', error);
  }
};

// Gestion de l'arrêt propre
process.on('SIGINT', async () => {
  console.log('\nArrêt du consommateur...');
  await consumer.disconnect();
  process.exit(0);
});

run().catch(console.error);