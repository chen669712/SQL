
select get_json_object(value,'$.keyword'),get_json_object(value,'$.value')
from dw_mobdb.factmbtracelog_hybrid
where d >= '2018-06-01'
	and d <= '2018-06-10'
	and key = 'c_bnb_inn_city_search_result_app'
	and get_json_object(value,'$.value') like '[]'