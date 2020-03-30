const { Pool } = require("pg");

const pool = new Pool({
  host: process.env.APP_DB_PGQL_HOST || "localhost",
  user: process.env.APP_DB_PGQL_USER || "postgres",
  password: process.env.APP_DB_PGQL_PASS || "123456",
  database: process.env.APP_DB_PGQL_DB_SISMED || "SALUDSISMED",
  port: process.env.APP_DB_PGQL_PORT || 5432
});

module.exports = pool;
