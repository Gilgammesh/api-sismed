const { Router } = require("express");
const router = Router();

const {
  getRedes,
  getMicroRedes,
  getEstablecimientos,
  getCategorias,
  getDisponibilidad1,
  getDisponibilidad2,
  getDisponibilidad2Categ
} = require("../controllers/index.controllers");

router.get("/redes", getRedes);
router.get("/microredes/red/:red", getMicroRedes);
router.get("/establecimientos/red/:red/microred/:microred", getEstablecimientos);
router.get("/categorias", getCategorias);
router.get("/indicadores/disponibilidad1/almcod/:almcod", getDisponibilidad1);
router.get("/indicadores/disponibilidad2/almcod/:almcod", getDisponibilidad2);
router.get("/indicadores/disponibilidad2/almcod/:almcod/categ/:categ", getDisponibilidad2Categ);

module.exports = router;
