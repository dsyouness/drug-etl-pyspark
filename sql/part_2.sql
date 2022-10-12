-- Réaliser une requête un peu plus complexe qui permet de déterminer, par client et sur la période allant
-- du 1er janvier 2020 au 31 décembre 2020, les ventes meuble et déco réalisées.

WITH ventes_meuble AS (
  SELECT 
    t.client_id AS client_id, 
    SUM(t.prod_price * t.prod_qty) AS ventes_meuble 
  FROM 
    TRANSACTION t 
    INNER JOIN PRODUCT_NOMENCLATURE p ON t.prod_id = p.product_id 
  WHERE 
    t.date BETWEEN CAST("2020-01-01" AS DATE) 
    AND CAST("2020-12-31" AS DATE) 
    AND p.product_type = "MEUBLE" 
  GROUP BY 
    t.client_id
), 
ventes_deco AS (
  SELECT 
    t.client_id AS client_id, 
    SUM(t.prod_price * t.prod_qty) AS ventes_deco 
  FROM 
    TRANSACTION t 
    INNER JOIN PRODUCT_NOMENCLATURE p ON t.prod_id = p.product_id 
  WHERE 
    t.date BETWEEN CAST("2020-01-01" AS DATE) 
    AND CAST("2020-12-31" AS DATE) 
    AND p.product_type = "DECO" 
  GROUP BY 
    t.client_id
), 
ventes AS (
  SELECT 
    m.client_id as m_client_id, 
    m.ventes_meuble, 
    d.client_id as d_client_id, 
    d.ventes_deco 
  FROM 
    ventes_meuble as m 
    LEFT JOIN ventes_deco as d ON m.client_id = d.client_id 
  UNION 
  SELECT 
    m.client_id as m_client_id, 
    m.ventes_meuble, 
    d.client_id as d_client_id, 
    d.ventes_deco 
  FROM 
    ventes_meuble as m 
    RIGHT JOIN ventes_deco as d ON m.client_id = d.client_id
) 

SELECT 
  IFNULL(
    m_client_id, d_client_id
  ) AS client_id, 
  ventes_meuble, 
  ventes_deco 
FROM 
  ventes;
