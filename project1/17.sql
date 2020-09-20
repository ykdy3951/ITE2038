SELECT COUNT(*)
FROM (
  SELECT type
  FROM Pokemon as P, CatchedPokemon as CP, Trainer as T
  WHERE T.hometown='Sangnok City' AND CP.owner_id=T.id AND CP.pid=P.id 
  GROUP BY type
  ) as TypeTable;