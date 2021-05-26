-- yes...

-- The latest item - loc - segment data
-- Cut down the table so easier on the join
drop table if exists newton_lab_dev.vg_seg_latest
go
create table newton_lab_dev.vg_seg_latest
stored as parquet
as
(
select item
, loc
, sku_segments 
from jdafulfillment_raw_prod.u_sku_segments 
where 10000*year+100*month+day  
in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.u_sku_segments) 
and loc in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
)
go
compute stats newton_lab_dev.vg_seg_latest
go


drop table if exists newton_lab_dev.vg_proj_latest
go
create table newton_lab_dev.vg_proj_latest
stored as parquet
as
(
select item
, loc
, ss
, totdmd
, ignoreddmd
from jdafulfillment_raw_prod.u_skuprojstatic
where 10000*year+100*month+day  
in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.u_skuprojstatic)
and loc in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
--and item = '000000022248387002' and loc = '2639'
)
go
compute stats newton_lab_dev.vg_proj_latest
go

-- latest forward forecast
drop table if exists newton_lab_dev.vg_fcst_latest
go
create table newton_lab_dev.vg_fcst_latest
stored as parquet
as
(
select item
, skuloc loc
, totfcst
from  jdafulfillment_raw_prod.dfutoskufcst
where 10000*year+100*month+day  
in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.dfutoskufcst)
and startdate between date_add(now(),-6) and now()
and skuloc in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
)
go
compute stats newton_lab_dev.vg_fcst_latest
go


-- The latest item data
-- Cut down the table so easier on the join
drop table if exists newton_lab_dev.vg_itm_latest
go
create table newton_lab_dev.vg_itm_latest
stored as parquet
as
(
select * 
from jdafulfillment_raw_prod.item 
where 10000*year+100*month+day 
in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.item)  
)
go
compute stats newton_lab_dev.vg_itm_latest
go

-- The latest loc data
-- Cut down the table so easier on the join
drop table if exists newton_lab_dev.vg_loc_latest
go
create table newton_lab_dev.vg_loc_latest
stored as parquet
as
(
select loc
, descr 
from jdafulfillment_raw_prod.loc 
where 10000*year+100*month+day  in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.loc) 
and loc in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
)
go
compute stats newton_lab_dev.vg_loc_latest
go

-- Latest on hand data
drop table if exists newton_lab_dev.vg_sku_latest
go
create table newton_lab_dev.vg_sku_latest
stored as parquet
as
(
select item
, loc
, oh 
from jdafulfillment_raw_prod.sku 
where 10000*year+100*month+day in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.sku)
and loc in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
)
go
compute stats newton_lab_dev.vg_sku_latest
go

-- Extract markdown and newness flag from 'segmentation logic table'
-- Extracted from here to exactly mirror the exclusions 
drop table if exists newton_lab_dev.vg_logic_latest
go
create table newton_lab_dev.vg_logic_latest
stored as parquet
as
(
select item
, skuloc
, markdown
, newness_flag
from  ana_ldm_lab_prod.ms_str_frame_history where refresh_date in (select max(refresh_date) from ana_ldm_lab_prod.ms_str_frame_history) 
and skuloc in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
)
go
compute stats newton_lab_dev.vg_logic_latest
go

-- In transit
-- Don't need to know where it is coming from and when
-- , just that it has been put through to allocation
drop table if exists newton_lab_dev.vg_po_sto_latest
go
create table newton_lab_dev.vg_po_sto_latest
stored as parquet
as
(
select article_variant as item
, receive_site as loc
, sum(advice_qty) as transit_qty 
from jdafulfillment_raw_prod.u_po_sto 
where 10000*year+100*month+day 
in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.u_po_sto) 
and receive_site in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
group by 1,2
)
go
compute stats newton_lab_dev.vg_po_sto_latest
go

-- Latest view of recships
-- For the next 7 days
drop table if exists newton_lab_dev.vg_rs_latest
go
create table newton_lab_dev.vg_rs_latest
stored as parquet
as
(select item
, dest as loc
, sum(qty) as qty 
from jdafulfillment_raw_prod.recship 
where 10000*year+100*month+day in (select max(10000*year+100*month+day) from jdafulfillment_raw_prod.recship) 
and deliverydate between eah_load_datetime and  date_add(eah_load_datetime,7)
and dest in ('1292' --Vangarde
, '2639','3353', '1216' -- 3 POC
, '1861','1366' -- Vagarde control stores
, '2781', '0343', '5298', '2189', '3227', '3298' -- 3 POC control stores
--, 1232,4064,2985,589,6444,1355,2817,3104,2202 ,137,1300,301,8714,6156,1863,54,246 -- 'old trial' 17 stores
)
group by 1,2
)
go
compute stats newton_lab_dev.vg_rs_latest
go



drop table if exists newton_lab_dev.vg_target_for_vm
go
create table newton_lab_dev.vg_target_for_vm
stored as parquet as (
select
  proj.item
  , itm.u_article_id
, proj.loc
, loc.descr
, case when seg.sku_segments = 'KNIFE BLOCK' then 'SINGLE SIZE LROS' 
when seg.sku_segments is null and newness_flag is not null then concat('Not on segmentation ',right(newness_flag,3))
when seg.sku_segments is null and markdown is not null then concat('Not on segmentation ',markdown)
when seg.sku_segments is null then 'Not on segmentation'
else seg.sku_segments end as segment
, itm.u_bu_desc 
, concat(itm.u_g5_dept_id, " ", itm.u_dept_desc) as u_g5_dept_id
, itm.u_range_desc
, itm.u_stroke_id
, itm.u_colour_desc
, itm.u_size_desc
, itm.u_article_desc
, itm.u_upc
, sto.transit_qty
, rec.qty as recship_qty
, sku.oh
, CEIL(proj.ss) as safety_stock
, CEIL(proj.ss + 5*(proj.totdmd+ proj.ignoreddmd)) as target_stock
, proj.totdmd+ proj.ignoreddmd as daily_dmd
, isnull(fcst.totfcst,0) as week_fcst
, CEIL(proj.ss + isnull(fcst.totfcst,0)) as wk_target_stock
, case when sku.oh > CEIL(proj.ss + 5*(proj.totdmd+ proj.ignoreddmd)) then sku.oh - CEIL(proj.ss + 5*(proj.totdmd+ proj.ignoreddmd)) else 0 end as excess_to_target
, case when sku.oh > CEIL(proj.ss + isnull(fcst.totfcst,0) ) then sku.oh - CEIL(proj.ss + isnull(fcst.totfcst,0) ) else 0 end as excess_to_wk_target
, case when sku.oh < CEIL(proj.ss) then CEIL(proj.ss) - sku.oh else 0 end as short_to_safety
, u_display_type
, u_brand_desc
, u_mcdq
from newton_lab_dev.vg_proj_latest proj
left join newton_lab_dev.vg_seg_latest seg
on proj.item = seg.item
and proj.loc = seg.loc
left join  newton_lab_dev.vg_itm_latest itm 
on itm.item = proj.item
inner join newton_lab_dev.vg_loc_latest loc 
on loc.loc = proj.loc
left join newton_lab_dev.vg_sku_latest sku 
on sku.loc = proj.loc 
and sku.item = proj.item
left join newton_lab_dev.vg_logic_latest hist 
on hist.skuloc = proj.loc 
and hist.item = proj.item
left join newton_lab_dev.vg_po_sto_latest sto
on sto.loc = proj.loc 
and sto.item = proj.item
left join newton_lab_dev.vg_rs_latest rec
on rec.loc = proj.loc 
and rec.item = proj.item
left join newton_lab_dev.vg_fcst_latest fcst
on proj.loc = fcst.loc
and proj.item = fcst.item
where u_g5_dept_id <> "T46"
and u_bu_desc <> "OTHER"
and proj.item <> "000000021130599001"
)
go
compute stats  newton_lab_dev.vg_target_for_vm


