SELECT DISTINCT name, type
FROM CatchedPokemon, Pokemon
WHERE pid=Pokemon.id AND level >= 30
ORDER BY name;