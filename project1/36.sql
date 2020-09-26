SELECT T.name
FROM CatchedPokemon as CP, Trainer as T, (SELECT after_id
    FROM Evolution as E
    WHERE E.after_id NOT IN (
    SELECT before_id
    FROM Evolution
  )) as FinalEvol
WHERE T.id=CP.owner_id AND CP.pid=FinalEvol.after_id
ORDER BY T.name;