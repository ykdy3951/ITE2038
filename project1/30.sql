SELECT P1.id, P1.name, P2.name, P3.name
FROM Pokemon as P1, Pokemon as P2, Pokemon as P3, Evolution as E1, Evolution as E2
WHERE P1.id=E1.before_id AND E1.after_id=P2.id AND P2.id=E2.before_id AND E2.after_id=P3.id
ORDER BY P1.id;