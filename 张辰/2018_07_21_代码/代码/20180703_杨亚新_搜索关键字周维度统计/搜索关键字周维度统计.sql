select log.keywordofvalue as `关键字`  	
	, c.cityname as `城市`  	
	, COUNT(log.uid) as `搜索用户数`  	
	, (sum(if( log.typeofvalue= 200 ,1,0))) as `直搜次数`  	
	, (sum(if(log.typeofvalue = 100 and typeid != 100, 1, 0))) as `点选次数`  	
	, (sum(if(log.typeofvalue= 101,1,0))) as `取消次数` 
	, (sum(if(log.typeofvalue= 102,1,0))) as `删除次数`
	, cast(round((sum(if( log.typeofvalue= 200 ,1,0)))/(sum(if(log.typeofvalue = 100 and typeid != 100, 1, 0))),2) as string) as `直搜次数/点选次数` 	
	from (select get_json_object(value,'$.keyword') as keywordofvalue 		
		, get_json_object(value,'$.operation') as typeofvalue 		
		, get_json_object(get_json_object(value,'$.item'),'$.typeid') as typeid 		 		
		, get_json_object(value,'$.cityid') as cityid 	
		, uid 
		from dw_mobdb.factmbtracelog_hybrid 		 	 		
		where key = 'c_bnb_inn_search_list_app' 	 		 	 		
		and d >= "$effectdate('yyyy-MM-dd',-9)" and d <= "$effectdate('yyyy-MM-dd',-1)" 
		and ts >= "$effectdate('yyyy-MM-dd',-8)" and ts <= "$effectdate('yyyy-MM-dd',-2)") log	 	 	
	inner join ( select cityid 			 		
		, cityname  		 		
		from ods_htl_groupwormholedb.bnb_city where d = "$effectdate('yyyy-MM-dd',0)" ) c  	
	on log.cityid=c.cityid  group by log.keywordOfvalue,c.cityname having  `直搜次数` >=12