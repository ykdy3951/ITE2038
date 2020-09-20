SELECT name, COUNT(name)
FROM Trainer as T, CatchedPokemon as CP
WHERE hometown='Sangnok City' AND T.id=owner_id
GROUP BY name
ORDER BY COUNT(name);