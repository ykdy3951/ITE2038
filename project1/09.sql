SELECT name, AVG(level)
FROM Trainer, CatchedPokemon
WHERE Trainer.id=CatchedPokemon.owner_id AND Trainer.id IN (
  SELECT leader_id
  FROM Gym
  )
GROUP BY name
ORDER BY name;