const pool = require("../database/index");

const getRedes = async (req, res) => {
  const result = await pool.query(`
    SELECT redccodred, trim(redtdscl) as redtdscl 
    FROM sismed.mred 
    WHERE redcdisa = '30' 
    ORDER BY redccodred ASC
  `);
  res.send(result.rows);
};

const getMicroRedes = async (req, res) => {
  const { red } = req.params;
  const result = await pool.query(`
    SELECT mrecmicro, trim(mretdscl) as mretdscl
    FROM sismed.mmicrored
    WHERE mrecdisa = '30' AND mrecred = '${red}'
    ORDER BY mrecmicro ASC
  `);
  res.send(result.rows);
};

const getEstablecimientos = async (req, res) => {
  const { red, microred } = req.params;
  const result = await pool.query(`
    SELECT trim(almcod) as almcod, trim(almdes) as almdes
    FROM sismed.malmacen  
    WHERE LENGTH(trim(almcod)) = 5 AND almcdisa = '30' 
    AND almcred = '${red}' 
    AND almcmred = '${microred}'
    ORDER BY trim(almcod) ASC
  `);
  res.send(result.rows);
};

const getCategorias = async (req, res) => {
  const result = await pool.query(`
    SELECT id, nombre
    FROM sismed.categorias
    ORDER BY nombre ASC
  `);
  res.send(result.rows);
};

const getDisponibilidad1 = async (req, res) => {
  const { almcod } = req.params;
  const result = await pool.query(`
    SELECT a.med_inter, SUM(a.consumo) as consumo
    FROM
    (SELECT a.medcod, trim(b.mednom) as mednom, a.entradas, a.salidas, a.stock, 
    a.mes1, a.mes2, a.mes3, a.mes4, a.mes5, a.mes6, 
    a.mes7, a.mes8, a.mes9, a.mes10, a.mes11, a.mes12,
    a.consumo, a.meses, a.ult_4meses, a.cpma, a.cpma_inter, 
    a.med, (CASE
    WHEN a.med IS NULL THEN null
    WHEN (a.med > 0 AND a.cpma = 0) THEN 'Sin rotación'
    WHEN a.med = 0 THEN 'Desabastecido'
    WHEN (a.med > 0 AND a.med < 2) THEN 'Substock'
    WHEN (a.med >= 2 AND a.med <= 6) THEN 'Normostock'
    WHEN (a.med >= 6) THEN 'Sobrestock'
    END) as med_inter
    FROM
    (SELECT a.medcod, a.entradas, a.salidas, a.stock, 
    a.mes1, a.mes2, a.mes3, a.mes4, a.mes5, a.mes6, 
    a.mes7, a.mes8, a.mes9, a.mes10, a.mes11, a.mes12,
    a.consumo, a.meses, a.ult_4meses, a.cpma, (CASE
    WHEN cpma IS NULL THEN 'SIN CONSUMO'
    ELSE null
    END) as cpma_inter, (CASE
    WHEN a.cpma IS NULL THEN null
    WHEN a.cpma = 0 THEN null
    ELSE (a.stock / a.cpma) 
    END) as med
    FROM
    (SELECT a.medcod, a.entradas, a.salidas, a.stock, 
    a.mes1, a.mes2, a.mes3, a.mes4, a.mes5, a.mes6, 
    a.mes7, a.mes8, a.mes9, a.mes10, a.mes11, a.mes12,
    a.consumo, a.meses, a.ult_4meses, (CASE 
    WHEN (a.ult_4meses = 0 AND a.stock IS NULL ) THEN null
    WHEN (a.ult_4meses = 0 AND a.stock <= 0 ) THEN null
    WHEN a.consumo = 0 THEN 0
    ELSE (a.consumo / a.meses)
    END) as cpma
    FROM
    (SELECT a.medcod, b.entradas, c.salidas, b.entradas - c.salidas as stock, 
    m1.consumo as mes1, m2.consumo as mes2, m3.consumo as mes3, m4.consumo as mes4,
    m5.consumo as mes5, m6.consumo as mes6, m7.consumo as mes7, m8.consumo as mes8,
    m9.consumo as mes9, m10.consumo as mes10, m11.consumo as mes11, m12.consumo as mes12,
    (CASE WHEN m.consumo IS NOT NULL THEN m.consumo ELSE 0 END) as consumo, (
    (CASE WHEN m1.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m2.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m3.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m4.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m5.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m6.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m7.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m8.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m9.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m10.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m11.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m12.consumo IS NOT NULL THEN 1 ELSE 0 END)
    ) as meses, (
    (CASE WHEN m9.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m10.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m11.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m12.consumo IS NOT NULL THEN 1 ELSE 0 END)
    ) as ult_4meses
    FROM (SELECT trim(a.medcod) as medcod
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    GROUP BY trim(a.medcod)) as a
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as entradas
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'E'
    GROUP BY trim(a.medcod)
    ) as b ON (a.medcod = b.medcod)
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as salidas
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    GROUP BY trim(a.medcod)
    ) as c ON (a.medcod = c.medcod)
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '12 month')::date AND a.movfechult::date < (now() - interval '11 month')::date)
    GROUP BY trim(a.medcod)
    ) as m1 ON (a.medcod = m1.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '11 month')::date AND a.movfechult::date < (now() - interval '10 month')::date)
    GROUP BY trim(a.medcod)
    ) as m2 ON (a.medcod = m2.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '10 month')::date AND a.movfechult::date < (now() - interval '9 month')::date)
    GROUP BY trim(a.medcod)
    ) as m3 ON (a.medcod = m3.medcod)
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '9 month')::date AND a.movfechult::date < (now() - interval '8 month')::date)
    GROUP BY trim(a.medcod)
    ) as m4 ON (a.medcod = m4.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '8 month')::date AND a.movfechult::date < (now() - interval '7 month')::date)
    GROUP BY trim(a.medcod)
    ) as m5 ON (a.medcod = m5.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '7 month')::date AND a.movfechult::date < (now() - interval '6 month')::date)
    GROUP BY trim(a.medcod)
    ) as m6 ON (a.medcod = m6.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '6 month')::date AND a.movfechult::date < (now() - interval '5 month')::date)
    GROUP BY trim(a.medcod)
    ) as m7 ON (a.medcod = m7.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '5 month')::date AND a.movfechult::date < (now() - interval '4 month')::date)
    GROUP BY trim(a.medcod)
    ) as m8 ON (a.medcod = m8.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '4 month')::date AND a.movfechult::date < (now() - interval '3 month')::date)
    GROUP BY trim(a.medcod)
    ) as m9 ON (a.medcod = m9.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '3 month')::date AND a.movfechult::date < (now() - interval '2 month')::date)
    GROUP BY trim(a.medcod)
    ) as m10 ON (a.medcod = m10.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '2 month')::date AND a.movfechult::date < (now() - interval '1 month')::date)
    GROUP BY trim(a.medcod)
    ) as m11 ON (a.medcod = m11.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND a.movfechult::date >= (now() - interval '1 month')::date
    GROUP BY trim(a.medcod)
    ) as m12 ON (a.medcod = m12.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND a.movfechult::date >= (now() - interval '12 month')::date
    GROUP BY trim(a.medcod)
    ) as m ON (a.medcod = m.medcod)
    ) as a) as a) as a 
    INNER JOIN sismed.mproducto as b ON (trim(a.medcod) = trim(b.medcod))
    ) as a
    WHERE a.med_inter = 'Desabastecido' OR a.med_inter = 'Substock' 
    OR a.med_inter = 'Normostock' OR a.med_inter = 'Sobrestock'
    GROUP BY a.med_inter
    ORDER BY a.med_inter
  `);
  res.send(result.rows);
};

const getDisponibilidad2 = async (req, res) => {
  const { almcod, categ } = req.params;
  let queryCateg = "";
  if (categ) {
    queryCateg = `INNER JOIN sismed.producto_categoria as c ON (trim(a.medcod) = trim(c.medcod) AND categ = ${categ})`;
  }
  const result = await pool.query(`
    SELECT a.medcod, trim(b.mednom) as mednom, a.entradas, a.salidas, a.stock, 
    a.mes1, a.mes2, a.mes3, a.mes4, a.mes5, a.mes6, 
    a.mes7, a.mes8, a.mes9, a.mes10, a.mes11, a.mes12,
    a.consumo, a.meses, a.ult_4meses, ROUND(a.cpma, 2) as cpma, a.cpma_inter, 
    ROUND(a.med, 2) as med, (CASE
    WHEN a.med IS NULL THEN null
    WHEN (a.med > 0 AND a.cpma = 0) THEN 'Sin rotación'
    WHEN a.med <= 0 THEN 'Desabastecido'
    WHEN (a.med > 0 AND a.med < 2) THEN 'Substock'
    WHEN (a.med >= 2 AND a.med <= 6) THEN 'Normostock'
    WHEN (a.med >= 6) THEN 'Sobrestock'
    END) as med_inter
    FROM
    (SELECT a.medcod, a.entradas, a.salidas, a.stock, 
    a.mes1, a.mes2, a.mes3, a.mes4, a.mes5, a.mes6, 
    a.mes7, a.mes8, a.mes9, a.mes10, a.mes11, a.mes12,
    a.consumo, a.meses, a.ult_4meses, a.cpma, (CASE
    WHEN cpma IS NULL THEN 'SIN CONSUMO'
    ELSE null
    END) as cpma_inter, (CASE
    WHEN a.cpma IS NULL THEN null
    WHEN a.cpma = 0 THEN null
    ELSE (a.stock / a.cpma) 
    END) as med
    FROM
    (SELECT a.medcod, a.entradas, a.salidas, a.stock, 
    a.mes1, a.mes2, a.mes3, a.mes4, a.mes5, a.mes6, 
    a.mes7, a.mes8, a.mes9, a.mes10, a.mes11, a.mes12,
    a.consumo, a.meses, a.ult_4meses, (CASE 
    WHEN (a.ult_4meses = 0 AND a.stock IS NULL ) THEN null
    WHEN (a.ult_4meses = 0 AND a.stock <= 0 ) THEN null
    WHEN a.consumo = 0 THEN 0
    ELSE (a.consumo / a.meses)
    END) as cpma
    FROM
    (SELECT a.medcod, b.entradas, c.salidas, b.entradas - c.salidas as stock, 
    m1.consumo as mes1, m2.consumo as mes2, m3.consumo as mes3, m4.consumo as mes4,
    m5.consumo as mes5, m6.consumo as mes6, m7.consumo as mes7, m8.consumo as mes8,
    m9.consumo as mes9, m10.consumo as mes10, m11.consumo as mes11, m12.consumo as mes12,
    (CASE WHEN m.consumo IS NOT NULL THEN m.consumo ELSE 0 END) as consumo, (
    (CASE WHEN m1.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m2.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m3.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m4.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m5.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m6.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m7.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m8.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m9.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m10.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m11.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m12.consumo IS NOT NULL THEN 1 ELSE 0 END)
    ) as meses, (
    (CASE WHEN m9.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m10.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m11.consumo IS NOT NULL THEN 1 ELSE 0 END) +
    (CASE WHEN m12.consumo IS NOT NULL THEN 1 ELSE 0 END)
    ) as ult_4meses
    FROM (SELECT trim(a.medcod) as medcod
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    GROUP BY trim(a.medcod)) as a
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as entradas
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'E'
    GROUP BY trim(a.medcod)
    ) as b ON (a.medcod = b.medcod)
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as salidas
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    GROUP BY trim(a.medcod)
    ) as c ON (a.medcod = c.medcod)
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '12 month')::date AND a.movfechult::date < (now() - interval '11 month')::date)
    GROUP BY trim(a.medcod)
    ) as m1 ON (a.medcod = m1.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '11 month')::date AND a.movfechult::date < (now() - interval '10 month')::date)
    GROUP BY trim(a.medcod)
    ) as m2 ON (a.medcod = m2.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '10 month')::date AND a.movfechult::date < (now() - interval '9 month')::date)
    GROUP BY trim(a.medcod)
    ) as m3 ON (a.medcod = m3.medcod)
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '9 month')::date AND a.movfechult::date < (now() - interval '8 month')::date)
    GROUP BY trim(a.medcod)
    ) as m4 ON (a.medcod = m4.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '8 month')::date AND a.movfechult::date < (now() - interval '7 month')::date)
    GROUP BY trim(a.medcod)
    ) as m5 ON (a.medcod = m5.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '7 month')::date AND a.movfechult::date < (now() - interval '6 month')::date)
    GROUP BY trim(a.medcod)
    ) as m6 ON (a.medcod = m6.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '6 month')::date AND a.movfechult::date < (now() - interval '5 month')::date)
    GROUP BY trim(a.medcod)
    ) as m7 ON (a.medcod = m7.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '5 month')::date AND a.movfechult::date < (now() - interval '4 month')::date)
    GROUP BY trim(a.medcod)
    ) as m8 ON (a.medcod = m8.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '4 month')::date AND a.movfechult::date < (now() - interval '3 month')::date)
    GROUP BY trim(a.medcod)
    ) as m9 ON (a.medcod = m9.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '3 month')::date AND a.movfechult::date < (now() - interval '2 month')::date)
    GROUP BY trim(a.medcod)
    ) as m10 ON (a.medcod = m10.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND (a.movfechult::date >= (now() - interval '2 month')::date AND a.movfechult::date < (now() - interval '1 month')::date)
    GROUP BY trim(a.medcod)
    ) as m11 ON (a.medcod = m11.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND a.movfechult::date >= (now() - interval '1 month')::date
    GROUP BY trim(a.medcod)
    ) as m12 ON (a.medcod = m12.medcod) 
    LEFT JOIN (
    SELECT trim(a.medcod) as medcod, SUM(a.movcantid) as consumo
    FROM sismed.tmovimdet as a
    INNER JOIN (SELECT trim(medcod) as medcod
    FROM sismed.mstkalmde  
    WHERE substring(trim(almcod), 1, 5) = '${almcod}'
    GROUP BY trim(medcod)) as b ON (trim(a.medcod) = b.medcod)
    WHERE a.movcoditip = 'S'
    AND a.medfechvto >= now()::date 
    AND a.movfechult::date >= (now() - interval '12 month')::date
    GROUP BY trim(a.medcod)
    ) as m ON (a.medcod = m.medcod)
    ) as a) as a) as a 
    INNER JOIN sismed.mproducto as b ON (trim(a.medcod) = trim(b.medcod))
    ${queryCateg}
    ORDER BY a.medcod ASC
  `);
  res.send(result.rows);
};

module.exports = {
  getRedes,
  getMicroRedes,
  getEstablecimientos,
  getCategorias,
  getDisponibilidad1,
  getDisponibilidad2
};
