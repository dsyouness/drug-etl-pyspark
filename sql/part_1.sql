
-- Je vous propose de commencer par réaliser une requête SQL simple permettant de trouver le chiffre d’affaires
-- (le montant total des ventes), jour par jour, du 1er janvier 2020 au 31 décembre 2020. Le résultat sera trié
-- sur la date à laquelle la commande a été passée.

SELECT
  t.date,
  sum(t.prod_price * t.prod_qty) as ventes
FROM
  TRANSACTION as t
where
  t.date BETWEEN CAST("2020-01-01" AS DATE)
  AND CAST("2020-12-31" AS DATE)
GROUP BY
  t.date
ORDER BY
  t.date;
