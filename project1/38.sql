SELECT name
FROM Evolution, Pokemon
WHERE after_id NOT IN (
  SELECT before_id
    FROM Evolution
) AND after_id=id
ORDER BY name;
