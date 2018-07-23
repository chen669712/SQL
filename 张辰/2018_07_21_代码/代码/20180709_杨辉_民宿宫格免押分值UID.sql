--下单的UID和订单金额
select distinct b1.uid,b1.totalsaleamount           
from ods_htl_bnborderdb.order_item a1     
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid 
and b1.d="2018-07-09"     
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid 
and c1.d="2018-07-09"     
where substring(b1.createdtime,0,10)>="2018-06-01"       
and substring(b1.createdtime,0,10)<="2018-07-08"       
and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)       
and a1.saleamount>=20 and a1.d="2018-07-09" and b1.sellerid=0
union all
select distinct uid,ordamount as totalsaleamount       
from dw_htlmaindb.FactHotelOrder_All_Inn     
where substring(orderdate,0,10)>="2018-06-01"       
and substring(orderdate,0,10)<="2018-07-08"       
and d ="2018-07-09"

--进入民宿的UID
select distinct uid from bnb_hive_db.bnb_pageview where d >= '2018-06-01' and d <= '2018-07-08' order by uid limit 1000000
