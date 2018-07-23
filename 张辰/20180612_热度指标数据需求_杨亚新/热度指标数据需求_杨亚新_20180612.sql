    
--途家在线房源数量和房东数量
select count(distinct c1.spaceid),count(distinct ownerid)
from ods_htl_bnborderdb.order_item a1 
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d = '2018-06-13' and a1.vendorid = 115 and a1.d = '2018-06-13'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-06-13'


select count(DISTINCT b.productid)as SumS,count(DISTINCT c.ownerid)as SumO
from
	(select spaceid
	from ods_htl_groupwormholedb.bnb_space_source
	where d = '2018-06-13'
	And vendorid <> '106') a 
	left join 
	(select productid
	from bnb_hive_db.bnb_product_trace
	where type = 101 
	AND DateDiff('2018-06-13',d)<=60) b 
	on a.spaceid = b.productid
	left join 
	(select spaceid,ownerid
	from ods_htl_groupwormholedb.bnb_space
	where d='2018-06-13')c
	on c.spaceid = b.productid


select * from ods_htl_groupwormholedb.bnb_city_data where d = '2018-06-13' limit 10

--行政区总量
select count(distinct locationid) from bnb_hive_db.bnb_space_location where d = '2018-06-13'

select count(distinct zoneid) from ods_htl_groupwormholedb.bnb_space_zone where d = '2018-06-13'

select count(distinct cityid) from ods_htl_groupwormholedb.bnb_city_data where parentcityid != 0 and d = '2018-06-13';  

select get_json_object(value,'$.position') from dw_mobdb.factmbtracelog_hybrid where d = '2018-06-13' limit 10

select 
