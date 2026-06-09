const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const createTable = async () => {
  // Utilisez message_offset si vous avez renommé, sinon "offset"
  const query = `
    CREATE TABLE IF NOT EXISTS kafka_messages (
      id SERIAL PRIMARY KEY,
      topic VARCHAR(100) NOT NULL,
      partition INTEGER NOT NULL,
      message_offset BIGINT NOT NULL,
      message_key VARCHAR(255),
      payload JSONB NOT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(topic, partition, message_offset)
    );
    CREATE INDEX IF NOT EXISTS idx_created_at ON kafka_messages(created_at);
    CREATE INDEX IF NOT EXISTS idx_topic ON kafka_messages(topic);
  `;
  await pool.query(query);
  console.log('Table kafka_messages vérifiée/créée');
};

const saveMessage = async (topic, partition, offset, key, payload) => {
  // Adaptez le nom de colonne ici aussi
  const query = `
    INSERT INTO kafka_messages (topic, partition, message_offset, message_key, payload)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (topic, partition, message_offset) 
    DO UPDATE SET payload = EXCLUDED.payload
    RETURNING id
  `;
  const result = await pool.query(query, [topic, partition, offset, key, payload]);
  return result.rows[0];
};

const getAllMessages = async (limit = 100, offset = 0) => {
  const query = `SELECT * FROM kafka_messages ORDER BY created_at DESC LIMIT $1 OFFSET $2`;
  const result = await pool.query(query, [limit, offset]);
  return result.rows;
};

const getMessageById = async (id) => {
  const query = `SELECT * FROM kafka_messages WHERE id = $1`;
  const result = await pool.query(query, [id]);
  return result.rows[0];
};

const getStats = async () => {
  const query = `
    SELECT COUNT(*) as total_messages, 
           COUNT(DISTINCT topic) as unique_topics,
           MIN(created_at) as oldest_message,
           MAX(created_at) as newest_message
    FROM kafka_messages
  `;
  const result = await pool.query(query);
  return result.rows[0];
};

module.exports = {
  pool,
  createTable,
  saveMessage,
  getAllMessages,
  getMessageById,
  getStats
};