SELECT DISTINCT P.name
FROM Pokemon as P, (
  SELECT CP.pid
    FROM CatchedPokemon as CP, Trainer as T
    WHERE T.hometown='Sangnok City' AND T.id=CP.owner_id
  ) as Sangnok,
    (
  SELECT CP.pid
    FROM CatchedPokemon as CP, Trainer as T
    WHERE T.hometown='Brown City' AND T.id=CP.owner_id
    ) as Brown
WHERE Sangnok.pid=Brown.pid AND Sangnok.pid=P.id
ORDER BY P.name;