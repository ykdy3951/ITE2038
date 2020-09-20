SELECT hometown, nickname
FROM (
    SELECT hometown, level, nickname
    FROM Trainer as T, CatchedPokemon as CP
    WHERE T.id=CP.owner_id
    ORDER BY level DESC
  ) as MaxLevel
GROUP BY hometown
ORDER BY hometown;