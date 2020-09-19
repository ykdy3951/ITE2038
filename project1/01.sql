SELECT T.name
FROM Trainer as T, CatchedPokemon as CP
WHERE T.id = CP.owner_id
GROUP BY T.name
HAVING COUNT(*) > 2
ORDER BY COUNT(*) DESC;