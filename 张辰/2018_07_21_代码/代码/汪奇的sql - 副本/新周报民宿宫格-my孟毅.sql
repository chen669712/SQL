

select sum(bb.orderid) as `预定订单`
,sum(bb.output) as `预定间夜`
,sum(bb.gmv) as `预定GMV`
,sum(dd.orderid) as `离店订单`
,sum(dd.output) as `离店间夜`
,sum(dd.gmv) as `离店GMV`
,sum(aa.orderid) as `支付订单`
,sum(aa.output) as `支付间夜`
,sum(aa.gmv) as `支付GMV`
,sum(cc.orderid) as `提交订单`
from
(select a.day
  ,count (distinct a.orderid) as orderid
  ,sum (a.output) as output
  ,sum(a.saleamount) as gmv
from
(select			------------支付
    to_date(b1.createdtime) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from
  ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
  where to_date(b1.createdtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(b1.createdtime)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
    and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}'
    and b1.sellerid=0
    and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
  union all
  select
  to_date(fs.orderdate) as day,
  fs.orderid ,
  fs.ciiquantity as output,
  fs.ciireceivable as saleamount
  from
  dw_htlmaindb.FactHotelOrder_All_Inn fs
  where
  to_date(fs.orderdate) >='${zdt.addDay(-7).format("yyyy-MM-dd")}'	and to_date(fs.orderdate) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and fs.d ='${zdt.format("yyyy-MM-dd")}'
  and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )a
  group by a.day)aa--------------支付

left join
  (select b.day
    ,count (distinct b.orderid) as orderid
    ,sum (b.output) as output
    ,sum(b.saleamount) as gmv
  from
  (select			------------预定
    to_date(b1.createdtime) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
  where to_date(b1.createdtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(b1.createdtime)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
    and a1.saleamount>=20
    and a1.d='${zdt.format("yyyy-MM-dd")}'
    and b1.sellerid=0
    and a1.statusid in (1212,2212,2232,2233)

  union all
  select to_date(fs.orderdate) as day,
    fs.orderid ,
    fs.ciiquantity as output,
    fs.ciireceivable as saleamount
  from dw_htlmaindb.FactHotelOrder_All_Inn fs
  where to_date(fs.orderdate) >='${zdt.addDay(-7).format("yyyy-MM-dd")}'
    and to_date(fs.orderdate) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and fs.d ='${zdt.format("yyyy-MM-dd")}' and fs.orderstatus <> 'C'
    and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )b
  group by b.day)bb on aa.day=bb.day

  left join

  (select c.day
    ,count (distinct c.orderid) as orderid
    ,sum (c.output) as output
    ,sum(c.saleamount) as gmv
  from
  (select			------------提交
    to_date(b1.createdtime) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
  where to_date(b1.createdtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(b1.createdtime)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
    and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}' and b1.sellerid=0

  union all

  select to_date(fs.orderdate) as day,
    fs.orderid ,
    fs.ciiquantity as output,
    fs.ciireceivable as saleamount
  from dw_htlmaindb.FactHotelOrder_All_Inn fs
  where to_date(fs.orderdate) >='${zdt.addDay(-7).format("yyyy-MM-dd")}'
    and to_date(fs.orderdate) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and fs.d ='${zdt.format("yyyy-MM-dd")}'
    and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )c
  group by c.day)cc on aa.day=cc.day

  left join

  (select d.day
    ,count (distinct d.orderid) as orderid
    ,sum (d.output) as output
    ,sum(d.saleamount) as gmv
  from
  (select			------------离店
    to_date(c1.checkout) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
  where to_date(c1.checkout)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(c1.checkout)>='${zdt.addDay(-7).format("yyyy-MM-dd")}'
    and a1.statusid  in (1212,2212,2232,2233)
    and a1.saleamount>=20
    and a1.d='${zdt.format("yyyy-MM-dd")}'
    and b1.sellerid=0

  union all
  select  to_date(fs.etd) as day,
    fs.orderid ,
    fs.ciiquantity as output,
    fs.ciireceivable as saleamount
  from dw_htlmaindb.FactHotelOrder_All_Inn fs
  where to_date(fs.etd) >='${zdt.addDay(-7).format("yyyy-MM-dd")}'
    and to_date(fs.etd) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and fs.d ='${zdt.format("yyyy-MM-dd")}'
    and fs.orderstatus ='S'
    and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )d
  group by d.day)dd on aa.day=dd.day

----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
----------------------------------------------------------------------------------

set beginDay= '2018-03-01';
set endDay= '2018-03-31';
set calcDay= '2018-04-01';
select sum(bb.orderid) as `预定订单`
,sum(bb.output) as `预定间夜`
,sum(bb.gmv) as `预定GMV`
,sum(dd.orderid) as `离店订单`
,sum(dd.output) as `离店间夜`
,sum(dd.gmv) as `离店GMV`
,sum(aa.orderid) as `支付订单`
,sum(aa.output) as `支付间夜`
,sum(aa.gmv) as `支付GMV`
,sum(cc.orderid) as `提交订单`
from
(select a.day
  ,count (distinct a.orderid) as orderid
  ,sum (a.output) as output
  ,sum(a.saleamount) as gmv
from
(select			------------支付
    to_date(b1.createdtime) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from
  ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:calcDay}
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:calcDay}
  where to_date(b1.createdtime)<=${hiveconf:endDay}
    and to_date(b1.createdtime)>=${hiveconf:beginDay}
    and a1.saleamount>=20 and a1.d=${hiveconf:calcDay}
    and b1.sellerid=0
    and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
  union all
  select
  to_date(fs.orderdate) as day,
  fs.orderid ,
  fs.ciiquantity as output,
  fs.ciireceivable as saleamount
  from
  dw_htlmaindb.FactHotelOrder_All_Inn fs
  where
  to_date(fs.orderdate) >=${hiveconf:beginDay}	and to_date(fs.orderdate) <=${hiveconf:endDay}
  and fs.d =${hiveconf:calcDay}
  and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )a
  group by a.day)aa--------------支付

left join
  (select b.day
    ,count (distinct b.orderid) as orderid
    ,sum (b.output) as output
    ,sum(b.saleamount) as gmv
  from
  (select			------------预定
    to_date(b1.createdtime) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:calcDay}
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:calcDay}
  where to_date(b1.createdtime)<=${hiveconf:endDay}
    and to_date(b1.createdtime)>=${hiveconf:beginDay}
    and a1.saleamount>=20
    and a1.d=${hiveconf:calcDay}
    and b1.sellerid=0
    and a1.statusid in (1212,2212,2232,2233)

  union all
  select to_date(fs.orderdate) as day,
    fs.orderid ,
    fs.ciiquantity as output,
    fs.ciireceivable as saleamount
  from dw_htlmaindb.FactHotelOrder_All_Inn fs
  where to_date(fs.orderdate) >=${hiveconf:beginDay}
    and to_date(fs.orderdate) <=${hiveconf:endDay}
    and fs.d =${hiveconf:calcDay} and fs.orderstatus <> 'C'
    and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )b
  group by b.day)bb on aa.day=bb.day

  left join

  (select c.day
    ,count (distinct c.orderid) as orderid
    ,sum (c.output) as output
    ,sum(c.saleamount) as gmv
  from
  (select			------------提交
    to_date(b1.createdtime) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:calcDay}
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:calcDay}
  where to_date(b1.createdtime)<=${hiveconf:endDay}
    and to_date(b1.createdtime)>=${hiveconf:beginDay}
    and a1.saleamount>=20 and a1.d=${hiveconf:calcDay} and b1.sellerid=0

  union all

  select to_date(fs.orderdate) as day,
    fs.orderid ,
    fs.ciiquantity as output,
    fs.ciireceivable as saleamount
  from dw_htlmaindb.FactHotelOrder_All_Inn fs
  where to_date(fs.orderdate) >=${hiveconf:beginDay}
    and to_date(fs.orderdate) <=${hiveconf:endDay}
    and fs.d =${hiveconf:calcDay}
    and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )c
  group by c.day)cc on aa.day=cc.day

  left join

  (select d.day
    ,count (distinct d.orderid) as orderid
    ,sum (d.output) as output
    ,sum(d.saleamount) as gmv
  from
  (select			------------离店
    to_date(c1.checkout) as day
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    ,a1.saleamount
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:calcDay}
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:calcDay}
  where to_date(c1.checkout)<=${hiveconf:endDay}
    and to_date(c1.checkout)>=${hiveconf:beginDay}
    and a1.statusid  in (1212,2212,2232,2233)
    and a1.saleamount>=20
    and a1.d=${hiveconf:calcDay}
    and b1.sellerid=0

  union all
  select  to_date(fs.etd) as day,
    fs.orderid ,
    fs.ciiquantity as output,
    fs.ciireceivable as saleamount
  from dw_htlmaindb.FactHotelOrder_All_Inn fs
  where to_date(fs.etd) >=${hiveconf:beginDay}
    and to_date(fs.etd) <=${hiveconf:endDay}
    and fs.d =${hiveconf:calcDay}
    and fs.orderstatus ='S'
    and (fs.ordertype_ubt not in ('hotel_order') or fs.ordertype_ubt is null) )d
  group by d.day)dd on aa.day=dd.day
