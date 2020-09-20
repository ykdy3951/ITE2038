SELECT hometown, AVG(level)
FROM Trainer, CatchedPokemon
WHERE Trainer.id=CatchedPokemon.owner_id
GROUP BY hometown
ORDER BY AVG(level);