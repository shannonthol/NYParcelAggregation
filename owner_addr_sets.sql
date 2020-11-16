--code for creating a table of standard owner names with unique ids (uuids) for parcel centroids that have a non-null standard owner names
--destination table ny-state-parcel-ownership:tnc_ny_parcels.rps_stdown_set
--FIELDS: 
--uuid = unique id for standard owner name set
--ids = comma separated list of ids from rpscentroids_all with the specified standardized owner name
--std_owner = standardized owner name from rpscentroids_all
--own_types = comma separated list of owner types with the specified standardized owner name
--sum_acres = sum of calculated areas field from rpscentroids_all for all records with the specified standardized owner name
--num = number of records from rpscentroids_all with the specified standardized owner name
select 
  generate_uuid() as uuid,
  string_agg(distinct cast(id as string), ", ") as ids,
  std_owner,
  string_agg(distinct cast(owner_type as string), ", ") as own_types,
  sum(calc_acres) as sum_acres,
  count(id) as num
from 
  tnc_ny_parcels.rpscentroids_all
where 
  std_owner != ''
group by 
  std_owner
order by 
  num desc


--code for creating a table of standard owner addresses with unique ids (uuids) for parcel centroids that have a non-null standard address
--destination table ny-state-parcel-ownership:tnc_ny_parcels.rps_stdaddr_set
--FIELDS: 
--uuid = unique id for standard owner name set
--ids = comma separated list of ids from rpscentroids_all with the specified standardized owner address
--std_addr = standardized owner address from rpscentroids_all
--own_types = comma separated list of owner types with the specified standardized owner address
--sum_acres = sum of calculated areas field from rpscentroids_all for all records with the specified standardized owner address
--num = number of records from rpscentroids_all with the specified standardized owner address
select 
  generate_uuid() as uuid,
  string_agg(distinct cast(id as string), ", ") as ids,
  std_addr,
  string_agg(distinct cast(owner_type as string), ", ") as own_types,
  sum(calc_acres) as sum_acres,
  count(id) as num
from 
  tnc_ny_parcels.rpscentroids_all
where 
  std_addr != ''
group by 
  std_addr
order by 
  num desc
  
--code for creating a table of standard owner names and address sets for parcel centroids  
with foo as
(select 
  id, std_owner, std_addr, concat(std_owner, " ", std_addr) as own_add, calc_acres 
from 
  tnc_ny_parcels.rpscentroids_all 
  )
select 
  row_number() over() as setid, count(id) as num_ids, length(std_owner) as own_strnum, length(std_addr) as add_strnum, sum(calc_acres) as sum_acres, string_agg(distinct cast(id as string), ", ") as ids, std_owner, std_addr, own_add 
from 
  foo 
group by 
  own_add, std_owner, std_addr 
order by 
  num_ids desc
