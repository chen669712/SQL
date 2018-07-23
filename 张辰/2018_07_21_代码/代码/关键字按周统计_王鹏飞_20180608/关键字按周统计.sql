--关键字按周统计
select  mocktable.keywordOfvalue
,  mocktable.cityname
, COUNT(  mocktable.uid )  as a
, (sum(if(typeOfValue='keywords',1,0) )) as b
, (sum(if(typeOfValue <> 'keywords' and typeOfValue<>'cancel',1,0) )) as c
, (sum(if(typeOfValue='cancel',1,0) )) as d  

from ( select c.cityname
	, newvalue.data['env_city'] as envcityname
	, get_json_object(value,'$.keyword') as keywordOfvalue
	, get_json_object(value,'$.value') as valueOfvalue
	, get_json_object(value,'$.type') as typeOfvalue
	, log.d as day
	, log.* from dw_mobdb.factmbtracelog_hybrid log 
	inner join ods_htl_groupwormholedb.bnb_city c 
	on get_json_object(value,'$.cityid')=c.cityid and c.d='2018-06-07' )  as mocktable  

where  ( mocktable.key='c_bnb_inn_searching_list_app' )  
and  ( ( mocktable.vid  !=  '12001082310059243704') )  
and  ( mocktable.d>='2018-06-01' and mocktable.d<='2018-06-07' )   
group by  mocktable.keywordOfvalue,  mocktable.cityname   
order by b desc 	
limit 500