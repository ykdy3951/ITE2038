SELECT name
FROM (
  SELECT name, SUM(level) as Sum
  FROM CatchedPokemon as CP, Trainer as T
  WHERE T.id=CP.owner_id
  GROUP BY T.name
  ORDER BY SUM(level) DESC
  ) as NewTable
WHERE NewTable.Sum=(
SELECT SUM(level)
FROM CatchedPokemon as CP, Trainer as T
WHERE T.id=CP.owner_id
GROUP BY T.name
ORDER BY SUM(level) DESC
LIMIT 1);