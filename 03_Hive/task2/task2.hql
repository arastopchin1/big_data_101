SELECT DATEDIFF(srch_co, srch_ci)  total_days, srch_ci, srch_co
FROM train
WHERE srch_adults_cnt == 2 AND srch_children_cnt == 1
ORDER BY total_days DESC LIMIT 1;