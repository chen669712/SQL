--按商圈统计房源数量（城市维度）
select   mocktable.cityname,  mocktable.zonename, ( COUNT(distinct  mocktable.spaceid ) ) as a  
from ( select space.spaceid
			,space.d
			,zone.zonename
			,CASE  WHEN zone.zonename IS NOT NULL THEN '有商圈' WHEN zone.zonename IS NULL THEN '无商圈' ELSE zone.zonename END hasZone
			,city.cityname 
		from ods_htl_groupwormholedb.bnb_space space 
		left join ods_htl_groupwormholedb.bnb_space_zone zone 
		on zone.spaceid=space.spaceid and zone.d='2018-06-07' 
		left join ods_htl_groupwormholedb.bnb_space_address address on address.spaceid=space.spaceid and address.d='2018-06-07' 
		left join ods_htl_groupwormholedb.bnb_city city on city.cityid=address.cityid and city.d='2018-06-07' 
		where address.cityid>0 and space.statusid=2 and space.d='2018-06-07' )  as mocktable  
where  ( mocktable.cityname in ('三亚','上海','厦门','成都','北京','重庆','丽江','南京','杭州','广州') )   
group by  mocktable.cityname,  mocktable.zonename   
order by  mocktable.cityname asc,  mocktable.zonename asc 

--按商圈统计房源数量（城市维度）--海外
select   mocktable.cityname,  mocktable.zonename, ( COUNT(distinct  mocktable.spaceid ) ) as a  
from ( 
	select space.spaceid,space.d,zone.zonename,CASE  WHEN zone.zonename IS NOT NULL THEN '有商圈' WHEN zone.zonename IS NULL THEN '无商圈' ELSE zone.zonename END hasZone, city.cityname
	from ods_htl_groupwormholedb.bnb_space space 
	left join ods_htl_groupwormholedb.bnb_space_zone zone on zone.spaceid=space.spaceid and zone.d='2018-06-07' 
	left join ods_htl_groupwormholedb.bnb_space_address address on address.spaceid=space.spaceid and address.d='2018-06-07' 
	left join ods_htl_groupwormholedb.bnb_city city on city.cityid=address.cityid and city.d='2018-06-07' 
	where address.cityid>0 and space.statusid=2 and space.d='2018-06-07' )  as mocktable  
where  ( mocktable.cityname in ('曼谷','普吉岛','清迈','台北','屏东','花莲','高雄','首尔','济州市','河内','胡志明市','巴厘岛','吉隆坡','大阪','东京','暹粒') )   
group by  mocktable.cityname,  mocktable.zonename   order by  mocktable.cityname asc,  mocktable.zonename asc 

--按是否关联商圈统计房源数量
select   ( case  when tb.zonename is not null  then '有商圈' when tb.zonename is null  then '无商圈' else tb.zonename end ) , ( COUNT(distinct ta.spaceid ) ) as a  
from `ods_htl_groupwormholedb`.`bnb_space` ta    
left join `ods_htl_groupwormholedb`.`bnb_space_zone` tb on ta.spaceid = tb.spaceid  and ta.d = tb.d  
where  (  ta.d='2017-12-15' )  and  (  ta.statusid=2 )   
group by  ( case  when tb.zonename is not null  then '有商圈' when tb.zonename is null  then '无商圈' else tb.zonename end )    
order by  ( case  when tb.zonename is not null  then '有商圈' when tb.zonename is null  then '无商圈' else tb.zonename end )  desc

--按城市统计房源数量
select   mocktable.cityname,  mocktable.zonename, ( COUNT(distinct  mocktable.spaceid ) ) as a  
from ( select space.spaceid
			,space.d
			,zone.zonename
			,CASE  WHEN zone.zonename IS NOT NULL THEN '有商圈' WHEN zone.zonename IS NULL THEN '无商圈' ELSE zone.zonename END hasZone
			,city.cityname 
		from ods_htl_groupwormholedb.bnb_space space 
		left join ods_htl_groupwormholedb.bnb_space_zone zone 
		on zone.spaceid=space.spaceid and zone.d='2018-06-07' 
		left join ods_htl_groupwormholedb.bnb_space_address address on address.spaceid=space.spaceid and address.d='2018-06-07' 
		left join ods_htl_groupwormholedb.bnb_city city on city.cityid=address.cityid and city.d='2018-06-07' 
		where address.cityid>0 and space.statusid=2 and space.d='2018-06-07' )  as mocktable  
where  ( mocktable.cityname in ('三亚','上海','厦门','成都','北京','重庆','丽江','南京','杭州','广州') )   
group by  mocktable.cityname,  mocktable.zonename   
order by  mocktable.cityname asc,  mocktable.zonename asc 

--按行政区统计房源数量（城市维度）
select   mocktable.cityname,  mocktable.locationname, ( COUNT(distinct  mocktable.spaceid ) ) as a  
from ( select space.spaceid
	,location.locationname
	,city.cityname
	,CASE  WHEN location.locationname IS NOT NULL THEN '有行政区' WHEN location.locationname IS NULL THEN '无行政区' ELSE location.locationname END haslocation 
	from ods_htl_groupwormholedb.bnb_space space 
	left join bnb_hive_db.bnb_space_location location 
	on location.spaceid=space.spaceid and location.d='2018-06-08' 
	left join ods_htl_groupwormholedb.bnb_space_address address 
	on address.spaceid=space.spaceid and address.d='2018-06-08' 
	left join ods_htl_groupwormholedb.bnb_city city 
	on city.cityid=address.cityid and city.d='2018-06-08' 
	where address.cityid>0 and space.statusid=2 and space.d='2018-06-08' )  as mocktable    
group by  mocktable.cityname,  mocktable.locationname   
order by  mocktable.cityname asc,  mocktable.locationname asc