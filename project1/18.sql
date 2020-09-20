SELECT AVG(level)
FROM Gym as G, CatchedPokemon as CP
WHERE G.leader_id=CP.owner_id;