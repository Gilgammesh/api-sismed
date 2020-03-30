const express = require("express");
const routes = require("./routes/index.js");
const dotenv = require("dotenv");
const cookie = require("cookie-parser");
const cors = require("cors");

// variables de entorno
dotenv.config();
const app = express();

// middlewares
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookie());
app.use("*", cors());

// routes
app.use(routes);

// vars
const port = 4001;

// run
app.listen(port, () => {
  console.log("Servidor Listo!!! ");
  console.log("http://localhost:" + port);
});
