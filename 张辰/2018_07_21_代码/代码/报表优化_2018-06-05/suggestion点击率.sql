select distinct get_json_object(value,'$.type') from dw_mobdb.factmbtracelog_hybrid where d = '2018-06-06'

--计算uv
count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber

--操作搜索功能的用户
select a.d,count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
from dw_mobdb.factmbtracelog_hybrid a
where key in ('c_bnb_inn_searching_list_app','c_bnb_inn_searching_list_h5')
and a.d >= '20180528' and a.d <= '20180606'
group by a.d

--点选的用户
select a.d,count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
from dw_mobdb.factmbtracelog_hybrid a
where key in ('c_bnb_inn_searching_list_app','c_bnb_inn_searching_list_h5')
and get_json_object(value,'$.type') in ('product','businessZone','landMark','district')
and a.d >= '20180528' and a.d <= '20180606'
group by a.d