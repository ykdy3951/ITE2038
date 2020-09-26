SELECT id, CNT
FROM Trainer, (
  SELECT owner_id, COUNT(owner_id) as CNT
  FROM CatchedPokemon
  GROUP BY owner_id
  ORDER BY COUNT(owner_id) DESC
  ) as BestTrainer
WHERE id=owner_id AND CNT=(
  SELECT COUNT(owner_id) as CNT
  FROM CatchedPokemon
  GROUP BY owner_id
  ORDER BY COUNT(owner_id) DESC LIMIT 1)
ORDER BY id;