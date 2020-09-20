SELECT type, COUNT(type)
FROM Pokemon
GROUP BY type
ORDER BY COUNT(type), type;