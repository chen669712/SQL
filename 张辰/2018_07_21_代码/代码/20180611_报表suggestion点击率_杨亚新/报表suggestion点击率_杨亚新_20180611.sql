select distinct get_json_object(value,'$.type') from dw_mobdb.factmbtracelog_hybrid where d = '2018-06-06'

--计算uv
count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber

--操作搜索功能的用户
select a.d,count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
from dw_mobdb.factmbtracelog_hybrid a
where key in ('c_bnb_inn_searching_list_app','c_bnb_inn_searching_list_h5')
and a.d >= '20180528' and a.d <= '20180612'
group by a.d

--点选的用户
select a.d,count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
from dw_mobdb.factmbtracelog_hybrid a
where key in ('c_bnb_inn_searching_list_app','c_bnb_inn_searching_list_h5')
and get_json_object(value,'$.type') in ('product','businessZone','landMark','district')
and a.d >= '20180528' and a.d <= '20180612'
group by a.d

--报表20180611_suggestion点击量统计报表_杨亚新
select a.d,a.type,a.uv,a.pv,a.uv/b.uv from
  (select 1 as t,d,get_json_object(value,'$.type') as type,count(DISTINCT newvalue.data['env_clientcode']) as uv,count(*) as pv
	from dw_mobdb.factmbtracelog_hybrid a
	where key in ('c_bnb_inn_searching_list_app')
	and get_json_object(value,'$.type') in ('product','businessZone','landMark','district','keywords','cancel')
	and a.d = '2018-06-12' 
	group by 1,d,get_json_object(value,'$.type')) a
inner join 
(select 1 as t,d,count(DISTINCT newvalue.data['env_clientcode']) AS uv
   from dw_mobdb.factmbtracelog_hybrid a
   where key in ('c_bnb_inn_searching_list_app')
   and d = '2018-06-12'
   group by 1,d) b
on a.t = b.t