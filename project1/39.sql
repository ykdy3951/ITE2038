SELECT DISTINCT T.name
FROM Trainer as T, CatchedPokemon as CP1, CatchedPokemon as CP2
WHERE T.id=CP1.owner_id AND T.id=CP2.owner_id AND CP1.pid=CP2.pid AND CP1.id<>CP2.id
ORDER BY T.name;