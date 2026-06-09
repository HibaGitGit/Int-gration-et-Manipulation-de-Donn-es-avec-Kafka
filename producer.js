const { Kafka } = require('kafkajs');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'tp9-producer',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092'],
});

const producer = kafka.producer();
const topic = process.env.KAFKA_TOPIC || 'test-topic';

// Fonction pour générer des données aléatoires
const generateEvent = () => {
  const devices = ['sensor-01', 'sensor-02', 'sensor-03', 'thermostat-01', 'humidity-01'];
  const deviceId = devices[Math.floor(Math.random() * devices.length)];
  
  return {
    deviceId: deviceId,
    temperature: Number((15 + Math.random() * 20).toFixed(2)),
    humidity: Number((30 + Math.random() * 50).toFixed(2)),
    battery: Number((50 + Math.random() * 50).toFixed(0)),
    status: ['active', 'standby', 'error'][Math.floor(Math.random() * 3)],
    timestamp: new Date().toISOString(),
  };
};

const run = async () => {
  try {
    await producer.connect();
    console.log('Producteur Kafka connecté');
    
    let messageCount = 0;
    
    setInterval(async () => {
      const event = generateEvent();
      const key = event.deviceId;
      
      try {
        await producer.send({
          topic,
          messages: [{ 
            key: key, 
            value: JSON.stringify(event),
            headers: {
              'source': 'tp9-producer',
              'version': '1.0'
            }
          }],
        });
        
        messageCount++;
        console.log(`[${messageCount}] Message produit:`, {
          key,
          ...event
        });
      } catch (error) {
        console.error('Erreur lors de la production du message:', error);
      }
    }, 2000); // Envoi toutes les 2 secondes
    
  } catch (error) {
    console.error('Erreur de connexion du producteur:', error);
  }
};

// Gestion de l'arrêt propre
process.on('SIGINT', async () => {
  console.log('\nArrêt du producteur...');
  await producer.disconnect();
  process.exit(0);
});

run().catch(console.error);