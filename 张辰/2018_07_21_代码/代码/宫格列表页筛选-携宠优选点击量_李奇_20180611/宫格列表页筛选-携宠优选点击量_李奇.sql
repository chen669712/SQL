--建表解析value_json
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
 and log.d >= '2018-05-01'
 and log.d<='2018-06-11'

 --统计2018年5月1日至2018年6月10日，该筛选的日UV和PV
select d,count(distinct uid) uv,count(*) pv from tmp_zc_filter_parsing
where label like '%携宠优选%'
group by d
