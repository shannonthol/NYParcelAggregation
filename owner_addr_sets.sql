create table parcels.stdown_set as

select 
string_agg(distinct cast(id as varchar), ', ') as ids,
std_owner, 
string_agg(distinct cast(owner_type as varchar), ', ') as own_types,
round(sum(calc_acres::float)::float,2) as sum_acres, 
count(id) as num
from parcels.rpscentroids
where std_owner != ''
group by std_owner
order by num desc


create table parcels.stdaddr_set as

select 
string_agg(distinct cast(id as varchar), ', ') as ids,
std_addr, 
string_agg(distinct cast(owner_type as varchar), ', ') as own_types,
round(sum(calc_acres)::numeric,2) as sum_acres, 
count(id) as num
from parcels.rpscentroids
where std_addr != ''
group by std_addr
order by num desc
