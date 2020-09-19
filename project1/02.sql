SELECT name
FROM Pokemon,
(
  SELECT P.type, COUNT(P.type) as CNT
  FROM Pokemon as P
  GROUP BY P.type
) as Type_Info, 
(
  SELECT COUNT(P1.type) as CNT 
  FROM Pokemon as P1
  GROUP BY P1.type
  ORDER BY COUNT(P1.type) DESC LIMIT 2
) as Type_RANK
WHERE Pokemon.type=Type_Info.type AND Type_Info.CNT=Type_Rank.CNT
ORDER BY name;