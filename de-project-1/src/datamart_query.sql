/** заполним витрину **/
insert into analysis.dm_rfm_segments (user_id, recency, frequency, monetary_value)
select 
u.id, r.recency, f.frequency , m.monetary_value 
from 
analysis.users u 
left join analysis.tmp_rfm_recency r on u.id = r.user_id 
left join analysis.tmp_rfm_frequency f on u.id = f.user_id 
left join analysis.tmp_rfm_monetary_value m on u.id = m.user_id;

/**
0	1	3	4
1	4	3	3
2	2	3	5
3	2	3	3
4	4	3	3
5	5	5	5
6	1	3	5
7	4	2	2
8	1	1	3
9	1	3	2
**/
