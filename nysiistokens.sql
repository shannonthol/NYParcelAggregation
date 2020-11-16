--This code creates a table of unique nysiis tokens from the standardized parcel owner names (for parcels that are greater than 1 ac in area) 
--along with lists and numbers of uuids that contain them 
--FIELDS:
--numsetids = number of setids that contain the specified nysiis token
--setids = comma separated list of setids that contain the specified nysiis token
--nysiistokens = unique nysiis tokens from tokens in the standardized parcel owner names 
--destination table: ny-state-parcel-ownership:tnc_ny_parcels.rps_stdown_nysiistokens

--initial subquery creates a table of nysiis codes for all tokens in a the standardized parcel owner names (for parcels that are greater than 1 ac in area)
with foo as (
select 
  setid,
  tnc_ny_parcels.dq_fm_NYSIIS(std_owner) as nysiis,
  split(tnc_ny_parcels.dq_fm_NYSIIS(std_owner), " ") as nysiistokens
from 
  tnc_ny_parcels.rps_stdown_stdaddr_set 
where 
  sum_acres >1 and std_owner != '')

--main query groups the data by nysiis tokens and gets a count and comma separated list of uuids that contain them
select 
  count(setid) as numsetids,
  string_agg(distinct(cast(setid as string)), ", ") as setids, 
  nysiistokens
from 
  foo
    cross join 
  unnest(foo.nysiistokens) as nysiistokens
group by 
  nysiistokens
order by 
  numsetids desc
  
  
--code to retrieve the number of nysiis tokens that are greater than 3 characters in length
select numuuids, nysiistokens from tnc_ny_parcels.rps_stdown_nysiistokens where length(nysiistokens)>3 order by numuuids desc
--returns 94400 records, the first 14 of which are present in nysiis codes for more than 10,000 records (numuuids>10000)
