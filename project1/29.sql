SELECT COUNT(type)
FROM CatchedPokemon, Pokemon
WHERE pid=Pokemon.id
GROUP BY type
ORDER BY type;