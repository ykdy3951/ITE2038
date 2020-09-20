SELECT T.name, AVG(level)
FROM Trainer as T, CatchedPokemon as CP, Pokemon as P
WHERE T.id=CP.owner_id AND CP.pid=P.id AND (P.type='Normal' OR P.type='Electric')
GROUP BY T.name
ORDER BY AVG(level);