SELECT id, name
FROM Trainer, (
  SELECT owner_id
  FROM CatchedPokemon
  GROUP BY owner_id
  ORDER BY COUNT(owner_id) DESC LIMIT 1
  ) as BestTrainer
WHERE id=owner_id;