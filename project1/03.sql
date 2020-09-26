SELECT AVG(level)
FROM CatchedPokemon, Pokemon
WHERE CatchedPokemon.pid=Pokemon.id AND Pokemon.type='Electric' AND owner_id IN (
  SELECT Trainer.id
  FROM Trainer
  WHERE hometown='Sangnok City'
);
