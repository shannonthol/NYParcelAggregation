
--select polygons that contain the centroids of ownership collections with total area greater than or equal to 1000 acres
create table tnc_ny_parcels.polyscontain_rpscentroids_collectiondata_gtet1000acres
select polys.gid, polys.parcel_id,
pts.parcid, pts.setid, pts.collid, pts.std_own, pts.std_add, pts.coll_own, pts.coll_add, pts.parc_acres, pts.coll_acres, pts.coll_numparcs,
polys.geography
from tnc_ny_parcels.rpscentroids_collectiondata_gtet1000acres as pts, tnc_ny_parcels.ny_all_parcels as polys
where st_within(pts.geog, polys.geography)
