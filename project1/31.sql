SELECT type
FROM Pokemon
WHERE id IN (
  SELECT before_id
FROM Evolution
  )
GROUP BY type
HAVING COUNT(type) >= 3
ORDER BY type DESC;