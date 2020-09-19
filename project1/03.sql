SELECT AVG(level)
FROM CatchedPokemon
WHERE owner_id IN (
  SELECT Trainer.id
  FROM Trainer
  WHERE hometown='Sangnok City'
);