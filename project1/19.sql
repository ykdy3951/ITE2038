SELECT COUNT(*)
FROM (
  SELECT type
  FROM Gym as G, Trainer as T, CatchedPokemon as CP, Pokemon as P
  WHERE T.hometown='Sangnok City' AND G.leader_id=T.id AND T.id=CP.owner_id AND CP.pid=P.id
  GROUP BY type
  ) as TypeTable;  
