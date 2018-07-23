--酒店提交单口径
select oi.d, count (distinct oi.orderid) as ois from   
	(select substring(orderdate,0,10) as d     
				, orderid   
	from dw_htlmaindb.facthotelorder   
	where d>="2017-06-01" and d<="2018-06-30") oi   
group by oi.d

--酒店UV
select  d
,count(distinct  clientcode) as htluvuv 
from DW_MobDB.factmbpageview 
where d>="2017-06-01"  and d<="2018-06-30"   
and pagecode in ('hotel_inland_inquire', 'hotel_oversea_inquire')    
group by d

--窄口径业务报表（对比酒店）
select   mocktable.d
,  mocktable.`宫格dau`
,  mocktable.`民宿订单`
,  mocktable.`u2o`
,  mocktable.`大住宿内宫格订单占比`
,  mocktable.`大住宿内宫格途家订单占比`
,  mocktable.`大住宿内宫格用户占比`
,  mocktable.`app内宫格用户占比`  
from ( select bnboi.d   
	, bnboi.ois as `民宿订单`   
	, bnbuvuv as `宫格DAU`   
	, concat(cast(100*((bnboi.ois)/bnbuvuv) as decimal(5,2)),'%') as `U2O`   
	, concat(cast(100*((bnboi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格订单占比`   
	, concat(cast(100*((tujiaoi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格途家订单占比`   
	, concat(cast(100*((bnbuvuv)/htluvuv) as decimal(5,2)),'%') as `大住宿内宫格用户占比`   
	, concat(cast(100*((bnbuvuv)/appuvuv) as decimal(5,2)),'%') as `App内宫格用户占比` 
	from (select a.d   
		, sum (a.ois) as ois 
		from (select a.d   ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))       
			, sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois 
		from (select distinct substring(b1.createdtime, 0, 10) as d     
			, b1.applicationType     
			, b1.terminalType     
			, a1.orderid 
			from ods_htl_bnborderdb.order_item a1 
			left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="2018-07-03" 
			left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="2018-07-03" 
			where substring(b1.createdtime,0,10)>="2018-06-17"   and substring(b1.createdtime,0,10)<="2018-07-02"   
			and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)   
			and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')   
			and a1.saleamount>=20 and a1.d="2018-07-03" and b1.sellerid=0) a group by a.d union all select b.d   , 
		round(count(distinct b.orderid)*0.85,0) as ois from (select substring(orderdate, 0, 10) as d  
		,orderid from dw_htlmaindb.FactHotelOrder_All_Inn 
		where substring(orderdate,0,10)>="2018-06-17"   and substring(orderdate,0,10)<="2018-07-02"   and d ="2018-07-03" ) b   
		group by b.d)a group by a.d) bnboi 
	join ( select a.d   , if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))       
		, sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois from (select substring(b1.createdtime, 0, 10) as d     
		, b1.applicationType     , b1.terminalType     , a1.orderid from ods_htl_bnborderdb.order_item a1 
		left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="2018-07-03" 
		left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="2018-07-03" where substring(b1.createdtime,0,10)>="2018-06-17"   
		and substring(b1.createdtime,0,10)<="2018-07-02"   and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)   
		and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')   
		and a1.vendorid in (105,115)   and a1.saleamount>=20 and a1.d="2018-07-03" and b1.sellerid=0) a group by a.d)tujiaoi 
		on tujiaoi.d = bnboi.d 
		join (select oi.d   , count (distinct oi.orderid) as ois from   
			(select substring(orderdate,0,10) as d     
				, orderid   
			from dw_htlmaindb.facthotelorder   where d>="2018-06-17"     and d<="2018-07-02") oi   
		inner join   
		(select d 
			, orderid    
			from ods_htl_orderdb.ord_ltp_paymentinfo   where d="2018-07-03"     and paymentstatus = 2) olp 
		on oi.orderid = olp.orderid 
		group by oi.d)htloi 
		on htloi.d = bnboi.d 
		join (select  d,   count(distinct  clientcode) as bnbuvuv from bnb_hive_db.bnb_pageview 
			where d>="2018-06-17"       and d<="2018-07-02" group by d) bnbuv on bnbuv.d = bnboi.d 
		join (select  d,   count(distinct  clientcode) as htluvuv from DW_MobDB.factmbpageview 
			where d>="2018-06-17"  and d<="2018-07-02"   and pagecode in ('hotel_inland_inquire', 'hotel_oversea_inquire')   
			and prepagecode in ('home', '0') group by d)htluv on htluv.d = bnboi.d 
		join (select  d,   count(distinct  clientcode) as appuvuv 
			from DW_MobDB.factmbpageview where d>="2018-06-17"  and d<="2018-07-02"   and pagecode in ('home') group by d)appuv on appuv.d = bnboi.d )  as mocktable    
order by  mocktable.d desc 
