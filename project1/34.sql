SELECT name, level, nickname
FROM CatchedPokemon as CP, Pokemon as P
WHERE CP.pid=P.id AND CP.owner_id IN (
  SELECT id
    FROM Gym, Trainer
    WHERE Gym.leader_id=Trainer.id
  ) AND CP.nickname LIKE 'A%'
ORDER BY P.name DESC, CP.level DESC, CP.nickname DESC;