----老key
select distinct get_json_object(value,'$.type') from dw_mobdb.factmbtracelog_hybrid where d = '2018-06-06'

--计算uv
count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber

--操作搜索功能的用户
select a.d,count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
from dw_mobdb.factmbtracelog_hybrid a
where key in ('c_bnb_inn_searching_list_app','c_bnb_inn_searching_list_h5')
and a.d >= '2018-05-28' and a.d <= '2018-06-06'
group by a.d

--点选的用户
select a.d,count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
from dw_mobdb.factmbtracelog_hybrid a
where key in ('c_bnb_inn_searching_list_app','c_bnb_inn_searching_list_h5')
and get_json_object(value,'$.type') in ('product','businessZone','landMark','district')
and a.d >= '2018-05-28' and a.d <= '2018-06-06'
group by a.d


----新key
--分母使用suggestion的人数
select count(distinct b.cid) from
(select get_json_object(value,'$.sequenceId') sequenceId from dw_mobdb.factmbtracelog_hybrid where d = '2018-06-28' and key = 'o_bnb_inn_search_list_app') a
inner join
(select cid,transactionkey from bnb_hive_db.bnb_longtimelog where d = '2018-06-28') b
on a.sequenceId = b.transactionkey

--点选的用户
select a.d,count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
from dw_mobdb.factmbtracelog_hybrid a
where key = 'c_bnb_inn_search_list_app'
and get_json_object(value,'$.operation') = 100
and get_json_object(get_json_object(value,'$.item'),'$.typeid') != 100
and a.d = '2018-06-28'
group by a.d