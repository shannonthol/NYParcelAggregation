--This code (step 5) creates sets of unique owner name and mailing address combinations from the RPS centroid data in BigQuery
--Run the code in BigQuery, saving results as rps_stdown_stdaddr_set

--subquery concatenates standard owner (std_owner) and standard address (std_addr) into a new own_add field
with foo as
(select 
  id, 
  std_owner, 
  std_addr, 
  concat(std_owner, " ", std_addr) as own_add, 
  calc_acres 
from 
  tnc_ny_parcels.rpscentroids_all 
  )
--main query selects records from the subquery, grouped by own_add, std_owner, and std_addr
select 
  count(id) as num_ids, 
  length(std_owner) as own_strnum, 
  length(std_addr) as add_strnum, 
  sum(calc_acres) as sum_acres, 
  string_agg(distinct cast(id as string), ", ") as ids, 
  std_owner, 
  std_addr, 
  own_add 
from 
  foo 
group by 
  own_add, 
  std_owner, 
  std_addr 
order by 
  num_ids desc
