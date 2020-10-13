--code for creating a table of standard owner names with unique ids (uuids) for parcel centroids that have a non-null standard owner names
--destination table ny-state-parcel-ownership:tnc_ny_parcels.rps_stdown_set
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
