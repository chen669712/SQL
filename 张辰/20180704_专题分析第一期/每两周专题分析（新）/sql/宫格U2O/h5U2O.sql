--h5U2O
select users.d as `日期`   
	, users.uv as `DAU`   
	, submit.ois as `提交单`   
	, pay.ois as `支付单`   
	, concat(cast(100*((pay.ois)/users.uv) as decimal(5,2)),'%') as `U2O`  
	, concat(cast(100*((pay.ois)/submit.ois) as decimal(5,2)),'%') as `提交2有效` 
	from (select d   
			, count(distinct vid) as uv 
			from dw_ubtdb.pageview  
			where d>= '2018-06-01'
			and d <= '2018-06-29'    
			and originalpageid='600003543' 
		group by d) users 
	join (select distinct substring(b1.createdtime, 0, 10) as d     
		, count(distinct a1.orderid) as ois 
		from ods_htl_bnborderdb.order_item a1 
		left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="2018-06-30" 
		left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="2018-06-30" 
		where substring(b1.createdtime,0,10)>="2018-06-01"    
		and substring(b1.createdtime,0,10)<="2018-06-29"   
		and b1.terminalType=20   
		and a1.saleamount>=20 
		and a1.d="2018-06-30" 
		and b1.sellerid=0 
		group by substring(b1.createdtime, 0, 10)) submit 
	on submit.d = users.d 
	join (select distinct substring(b1.createdtime, 0, 10) as d     
		, count(distinct a1.orderid) as ois 
		from ods_htl_bnborderdb.order_item a1 
		left join ods_htl_bnborderdb.order_header_v2 b1 
		on a1.orderid=b1.orderid and b1.d="2018-06-30" 
		left join ods_htl_bnborderdb.order_item_space c1 
		on c1.orderitemid=a1.orderitemid and c1.d="2018-06-30" 
		where substring(b1.createdtime,0,10)>="2018-06-01"    
		and substring(b1.createdtime,0,10)<="2018-06-29"   
		and b1.terminalType=20   
		and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')   
		and a1.saleamount>=20 and a1.d="2018-06-30" and b1.sellerid=0 
		group by substring(b1.createdtime, 0, 10)) pay on submit.d = pay.d