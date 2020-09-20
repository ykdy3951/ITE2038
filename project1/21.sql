SELECT Trainer.name, COUNT(Trainer.name)
FROM Gym, Trainer, CatchedPokemon
WHERE Gym.leader_id=Trainer.id AND Trainer.id=CatchedPokemon.owner_id
GROUP BY Trainer.name
ORDER BY Trainer.name;