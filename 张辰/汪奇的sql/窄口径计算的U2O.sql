
--搜索输入排行
use bnb_hive_db;
drop table tmp_zc_bnb_inn_searching_list_result;
create table tmp_zc_bnb_inn_searching_list_result as
select cityid,input,count(*) as cn_input from (
    select get_json_object(value,'$.cityid') as cityid
          ,get_json_object(value,'$.input') as input
    from dw_mobdb.factmbtracelog_hybrid t
    where key in ('bnb_inn_searching_list_result_app','bnb_inn_searching_list_result_h5')
    and t.d >= '2018-05-01'
    and t.d <='2018-05-31'
) a
group by cityid,input
order by cn_input desc
limit 500

--取出城市名称
select a.cityid,b.cityname,a.input
from tmp_zc_bnb_inn_searching_list_result a
left outer join ( 
select cityid,cityname
from ods_htl_groupwormholedb.bnb_city
where d = '2018-06-06'
) b
on a.cityid = b.cityid 

--随机取出1000条关键词有对应的城市
use bnb_hive_db;
drop table tmp_zc_bnb_inn_searching_list;
create table tmp_zc_bnb_inn_searching_list as
select get_json_object(value,'$.cityid') as cityid
      ,get_json_object(value,'$.keyword') as keyword
from dw_mobdb.factmbtracelog_hybrid a
where get_json_object(value,'$.cityid') is not null
and a.d = '2018-05-31'
and key in ('c_bnb_inn_searching_list_app','c_bnb_inn_searching_list_h5')
limit 1000

--取出城市名称
select a.cityid,b.cityname,a.keyword
from bnb_hive_db.tmp_zc_bnb_inn_searching_list a
left outer join ( 
select cityid,cityname
from ods_htl_groupwormholedb.bnb_city
where d = '2018-06-06'
) b
on a.cityid = b.cityid 
