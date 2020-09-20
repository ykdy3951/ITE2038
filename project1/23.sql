SELECT T.name
FROM CatchedPokemon as CP, Trainer as T
WHERE T.id=CP.owner_id AND CP.level <= 10
GROUP BY T.name
ORDER BY T.name;