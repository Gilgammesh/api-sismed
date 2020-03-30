const express = require("express");
const routes = require("./routes/index.js");
const dotenv = require("dotenv");

// variables de entorno
dotenv.config();
const app = express();

// middlewares
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// routes
app.use(routes);

// vars
const port = 4001;

// run
app.listen(port, () => {
  console.log("Servidor Listo!!! ");
  console.log("http://localhost:" + port);
});
