SELECT T.name, MAX(level)
FROM Trainer as T, CatchedPokemon as CP
WHERE T.id=CP.owner_id
GROUP BY T.name
HAVING COUNT(T.name) >= 4
ORDER BY T.name;