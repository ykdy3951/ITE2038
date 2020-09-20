SELECT T.name, SUM(level)
FROM CatchedPokemon as CP, Trainer as T
WHERE T.id=CP.owner_id
GROUP BY T.name
ORDER BY SUM(level) DESC
LIMIT 1;