select c.day,sum(c.directorder) as `宫格支付直接订单`
from
(select to_date(a.orderdate) as day,round(count(distinct a.orderid)*0.85,0) as directorder
from
dw_htlmaindb.FactHotelOrder_All_Inn a
where to_date(a.orderdate)>='2017-06-01' and to_date(a.orderdate)<='2018-06-30'and a.d='2018-07-02' 
group by to_date(a.orderdate)
union all
select b2.day,count(distinct b2.ctriporderid) as directorder
from
(select to_date(b1.createdtime) as day,a1.orderid as ctriporderid,a1.saleamount,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output 
from
ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-06-28'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-06-28'

where to_date(b1.createdtime)>='2017-06-01'and to_date(b1.createdtime)<='2018-06-30'
and a1.saleamount>=20 and a1.d='2018-07-02' and uid not in('$seller-agent') and
b1.visitsource not in (13,14,18) and
(a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%') ) b2 
group by b2.day)c
group by c.day

--支付单口径
select c.day
,sum(c.output) as `间夜数`
,sum(c.directorder) as `宫格支付直接订单`
, sum(c.totalamount) as `订单金额`
from
(select to_date(a.orderdate) as day
  ,round(sum(ciiquantity) *0.85,0) as output
  ,round(count(distinct a.orderid)*0.85,0) as directorder
  ,round(count(distinct a.ciireceivable)*0.85,0) as totalamount
from
dw_htlmaindb.FactHotelOrder_All_Inn a
where to_date(a.orderdate)>='2017-06-01' and to_date(a.orderdate)<='2018-06-30'and a.d='2018-07-02'
group by to_date(a.orderdate)
union all
select b2.day,sum(b2.output) as output, count(distinct b2.ctriporderid) as directorder, sum(b2.saleamount) as totalamount
from
(select to_date(b1.createdtime) as day,a1.orderid as ctriporderid,a1.saleamount,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
from
ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-02'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-02'

where to_date(b1.createdtime)<='2018-06-30'and to_date(b1.createdtime)>='2017-06-01'
and a1.saleamount>=20 and a1.d='2018-07-02' and uid not in('$seller-agent') and
b1.visitsource not in (13,14,18) and
(a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%') ) b2
group by b2.day)c
group by c.day


