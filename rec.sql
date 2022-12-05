WITH RECURSIVE rectree AS (
  SELECT * 
    FROM tree 
   WHERE id = 0 
UNION ALL 
  SELECT t.* 
    FROM tree t 
    JOIN rectree
      ON t.parent_id = rectree.id
) SELECT SUM(size) FROM rectree;