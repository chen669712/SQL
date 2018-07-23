--目的地搜索关键词埋点数据验证
select key
	, get_json_object(value,'$.keyword') as keyword
	, get_json_object(value,'$.value') as value
	，get_json_object(value,'$.wordid') as wordid
	, value
from dw_mobdb.factmbtracelog_hybrid
where d = '2018-06-11'
and key in ('c_bnb_inn_city_search_result_app','c_bnb_inn_city_search_result_h5')
and get_json_object(value,'$.keyword') in ('北京','兰州')
and get_json_object(value,'$.wordid') = 2482205


--搜索操作
select key 
	, get_json_object(value,'$.keyword') as keyword
	, get_json_object(value,'$.value') as value
	, get_json_object(value,'$.type') as type
	, get_json_object(value,'$.ops') as ops
	, value
from dw_mobdb.factmbtracelog_hybrid
where d = '2018-06-11'
and key in ('c_bnb_inn_city_search_ops_app','c_bnb_inn_city_search_ops_h5')
and get_json_object(value,'$.keyword') in ('北京','兰州')
and get_json_object(value,'$.wordid') = 2482205
