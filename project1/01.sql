SELECT T.name
FROM Trainer as T, CatchedPokemon as CP
WHERE T.id = CP.owner_id
GROUP BY T.name
HAVING COUNT(*) >= 3
ORDER BY COUNT(*) DESC;