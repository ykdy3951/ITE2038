SELECT name
FROM Evolution, Pokemon
WHERE after_id < before_id AND before_id=id
ORDER BY name;