--60天内列表页位置区域行政区、商圈有过点击筛选的数量汇总
use bnb_hive_db;
drop table tmp_zc_filter_parsing;
create table tmp_zc_filter_parsing as
select log.key 
 , log.d 
 , log.uid
 , log.vid
 , log.value
 , log.pageid
 , info.*
 , filter.* 
 , position.*
from dw_mobdb.factmbtracelog_hybrid log
lateral view json_tuple(get_json_object(log.value, '$'), 'cityid'
  , 'result'
  , 'checkin'
  , 'checkout'
  , 'keyword'
  , 'priceMin'
  , 'priceMax'
  , 'hotelLevel'
  , 'sort') info as cityid
  , result
  , checkin
  , checkout
  , keyword
  , priceMin
  , priceMax
  , hotelLevel
  , sorts
lateral view json_tuple(get_json_object(log.value, '$.filter'), 'people'
  , 'type'
  , 'labelid'
  , 'facilityid') filter as people
  , type
  , label
  , facility
lateral view json_tuple(get_json_object(log.value, '$.position'), 'business'
  , 'viewpoint'
  , 'metro'
  , 'university'
  , 'hospital'
  , 'district'
  , 'station'
  , 'distance'
  , 'other') position as business
  , viewpoint
  , metro
  , university
  , hospital
  , district
  , station
  , distance
  , other
where log.key = 'c_bnb_inn_list_filter_app'
 and log.d >= date_add('2018-06-14',-60)
 and log.d<='2018-06-14'

select business,count(*) from tmp_zc_filter_parsing
group by business

select district,count(*) from tmp_zc_filter_parsing
group by district