SELECT Pokemon.name, Pokemon.id
FROM Pokemon, CatchedPokemon
WHERE Pokemon.id=CatchedPokemon.pid AND CatchedPokemon.owner_id IN (
  SELECT Trainer.id
  FROM Trainer
  WHERE Trainer.hometown = 'Sangnok City'
  )
ORDER BY Pokemon.id;