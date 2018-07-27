select a.day
  ,a.totalorder as `总订单量`
  ,concat(cast(100*(b.tujiaorder/a.totalorder) as decimal(5,2)),'%') as `途家占比`
  ,concat(cast(100*(c.mayiorder/a.totalorder) as decimal(5,2)),'%') as `蚂蚁占比`
  ,concat(cast(100*(d.qitaorder/a.totalorder) as decimal(5,2)),'%') as `其他占比`
  ,concat(cast(100*(e.guoneiorder/a.totalorder) as decimal(5,2)),'%') as `国内接入占比`
from 
(select c.day
  ,sum(c.directorder) as totalorder
  from
  (select to_date(a.orderdate) as day
    ,count(distinct a.orderid) as directorder
  from dw_htlmaindb.FactHotelOrder_All_Inn a
  where to_date(a.orderdate)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
    and to_date(a.orderdate)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and a.d='${zdt.format("yyyy-MM-dd")}'
    and (a.ordertype_ubt not in ('hotel_order') or a.ordertype_ubt is null)
  group by to_date(a.orderdate)

  union all

  select b1.day
    ,count(distinct b1.ctriporderid) as directorder
  from
  (select to_date(a.createtime) as day
    ,a.ctriporderid
    ,datediff(to_date(a.checkout),to_date(a.checkin))*(a.quantity) as output
    ,a.totalamount
    ,case when a.statusId in (11,19) 
               OR (a.statusId = 13 AND a.paymentStatusId = 3) then '预订成功'
          when (a.statusId = 12 AND a.vendorId != 108) 
               OR (a.vendorId = 108 AND a.paymentStatusId = 2) 
               OR (a.vendorid !=108 and a.statusid=7) then '房东拒单'
          when a.statusid in (11,10,19) 
               OR (a.paymentstatusid in (2,3))
               OR (a.statusid=12 and a.vendorid!=108)
               OR (a.statusId = 13  AND a.paymentStatusId = 3) then '支付成功'
          when a.statusid=7 then '房东拒绝'
          when a.statusid=8 then '未支付未确认'
          when a.statusid=10 then '已支付未确认'                   
          when a.statusid=12 then '其他原因关闭'
          when a.statusid=13 then '用户关闭'
          when a.statusid=19 then '已完成'
          when a.statusid=9 then '已确认未支付'
     else '其他' end as orderstatus            
  from ods_htl_groupwormholedb.bnb_order a
  where a.d='${zdt.format("yyyy-MM-dd")}'
    and to_date(a.createtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(a.createtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
    and a.saleamount>=20 ) b1 where b1.orderstatus in('预订成功','房东拒单','支付成功')
  group by b1.day--------老订单支付成功订单-----------

  union all

  select b2.day
    ,count(distinct b2.ctriporderid) as directorder
  from 
  (select to_date(b1.createdtime) as day
    ,a1.orderid as ctriporderid
    ,a1.saleamount
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output 
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
  where to_date(b1.createdtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(b1.createdtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
    and a1.saleamount>=20 
    and a1.d='${zdt.format("yyyy-MM-dd")}'
    and b1.sellerid = 0 
    and (a1.statusid like '12%' 
          or a1.statusid like '20%' 
          or a1.statusid like '22%' 
          or a1.statusid like '23%') ) b2 
  group by b2.day)c
group by c.day)a  
 
left join
(select --------途家
  c.day
  ,sum(c.directorder) as tujiaorder
  from
  (select b1.day
    ,count(distinct b1.ctriporderid) as directorder
  from
  (select to_date(a.createtime) as day
    ,a.ctriporderid
    ,datediff(to_date(a.checkout),to_date(a.checkin))*(a.quantity) as output
    ,a.totalamount
    ,case when a.statusId in (11,19) OR (a.statusId = 13 AND a.paymentStatusId = 3)  then '预订成功'
          when (a.statusId = 12 AND a.vendorId != 108) OR (a.vendorId = 108 AND a.paymentStatusId = 2) or (a.vendorid !=108 and a.statusid=7) then '房东拒单'
          when a.statusid in (11,10,19) 
                            or (a.paymentstatusid in (2,3))
                               or (a.statusid=12 and a.vendorid!=108)
                              or (a.statusId = 13  AND a.paymentStatusId = 3) then '支付成功'
          when a.statusid=7 then '房东拒绝'
          when  a.statusid=8 then '未支付未确认'
          when  a.statusid=10 then '已支付未确认'                   
          when  a.statusid=12 then '其他原因关闭'
          when  a.statusid=13 then '用户关闭'
          when  a.statusid=19 then '已完成'
          when  a.statusid=9 then '已确认未支付'
      else '其他' end as orderstatus            
  from ods_htl_groupwormholedb.bnb_order a
  left join ods_htl_groupwormholedb.bnb_space_source b on a.productid=b.spaceid and b.d='${zdt.format("yyyy-MM-dd")}'
  where a.d='${zdt.format("yyyy-MM-dd")}'
    and to_date(a.createtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(a.createtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
    and b.vendorid in (105,115) 
    and a.saleamount>=20 )b1 where b1.orderstatus in('预订成功','房东拒单','支付成功')
  group by b1.day--------老订单支付成功订单-----------
  
  union all

  select b2.day
  ,count(distinct b2.ctriporderid) as directorder
  from
  (select to_date(b1.createdtime) as day
    ,a1.orderid as ctriporderid
    ,a1.saleamount
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output 
  from ods_htl_bnborderdb.order_item a1
  left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
  left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
  where to_date(b1.createdtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and to_date(b1.createdtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
    and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}' 
    and b1.sellerid =0 
    and a1.vendorid in (105,115)
    and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%') ) b2 
  group by b2.day)c
group by c.day)b on a.day=b.day

left join

(select -----------蚂蚁
  c.day
  ,sum(c.directorder) as mayiorder
from (select b1.day
    ,count(distinct b1.ctriporderid) as directorder
  from
  (select to_date(a.createtime) as day
    ,a.ctriporderid,datediff(to_date(a.checkout),to_date(a.checkin))*(a.quantity) as output,a.totalamount,	
	   case when  a.statusId in  (11,19) OR (a.statusId = 13 AND a.paymentStatusId = 3)  then '预订成功'
               when    (a.statusId = 12 AND a.vendorId != 108) OR (a.vendorId = 108 AND a.paymentStatusId = 2) or (a.vendorid !=108 and a.statusid=7) then '房东拒单'
              when  a.statusid in (11,10,19) 
                            or (a.paymentstatusid in (2,3))
                               or (a.statusid=12 and a.vendorid!=108)
                              or (a.statusId = 13  AND a.paymentStatusId = 3) then '支付成功'
                               when a.statusid=7 then '房东拒绝'
             when  a.statusid=8 then '未支付未确认'
             when  a.statusid=10 then '已支付未确认'                   
             when  a.statusid=12 then '其他原因关闭'
             when  a.statusid=13 then '用户关闭'
             when  a.statusid=19 then '已完成'
             when  a.statusid=9 then '已确认未支付'
             else '其他' end as orderstatus            
from 
ods_htl_groupwormholedb.bnb_order a
left join ods_htl_groupwormholedb.bnb_space_source b on a.productid=b.spaceid and b.d='${zdt.format("yyyy-MM-dd")}'
where
a.d='${zdt.format("yyyy-MM-dd")}' and to_date(a.createtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'  and to_date(a.createtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
and b.vendorid =104
and a.saleamount>=20 )b1 where b1.orderstatus in('预订成功','房东拒单','支付成功')
group by b1.day--------老订单支付成功订单-----------
union all
select b2.day,count(distinct b2.ctriporderid) as directorder
from
(select to_date(b1.createdtime) as day,a1.orderid as ctriporderid,a1.saleamount,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output 
from
ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'

where to_date(b1.createdtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}' and to_date(b1.createdtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}' and  b1.sellerid =0 and a1.vendorid =104
and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%') ) b2 
  group by b2.day)c
  group by c.day)c   on a.day=c.day
left join
 (select ---------其他
 c.day,sum(c.directorder) as qitaorder
from
(select b1.day,count(distinct b1.ctriporderid) as directorder
from
(select to_date(a.createtime) as day,a.ctriporderid,datediff(to_date(a.checkout),to_date(a.checkin))*(a.quantity) as output,a.totalamount,	
	 
     case when  a.statusId in  (11,19) OR (a.statusId = 13 AND a.paymentStatusId = 3)  then '预订成功'
               when    (a.statusId = 12 AND a.vendorId != 108) OR (a.vendorId = 108 AND a.paymentStatusId = 2) or (a.vendorid !=108 and a.statusid=7) then '房东拒单'
              when  a.statusid in (11,10,19) 
                            or (a.paymentstatusid in (2,3))
                               or (a.statusid=12 and a.vendorid!=108)
                              or (a.statusId = 13  AND a.paymentStatusId = 3) then '支付成功'
                               when a.statusid=7 then '房东拒绝'
             when  a.statusid=8 then '未支付未确认'
             when  a.statusid=10 then '已支付未确认'                   
             when  a.statusid=12 then '其他原因关闭'
             when  a.statusid=13 then '用户关闭'
             when  a.statusid=19 then '已完成'
             when  a.statusid=9 then '已确认未支付'
             else '其他' end as orderstatus            
from 
ods_htl_groupwormholedb.bnb_order a
left join ods_htl_groupwormholedb.bnb_space_source b on a.productid=b.spaceid and b.d='${zdt.format("yyyy-MM-dd")}'
where
a.d='${zdt.format("yyyy-MM-dd")}' and to_date(a.createtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}'  and to_date(a.createtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
and b.vendorid not in (105,115,104) 
and a.saleamount>=20 )b1 where b1.orderstatus in('预订成功','房东拒单','支付成功')
group by b1.day--------老订单支付成功订单-----------
union all
select b2.day,count(distinct b2.ctriporderid) as directorder
from
(select to_date(b1.createdtime) as day,a1.orderid as ctriporderid,a1.saleamount,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output 
from
ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'

where to_date(b1.createdtime)<='${zdt.addDay(-1).format("yyyy-MM-dd")}' and to_date(b1.createdtime)>='${zdt.addDay(-8).format("yyyy-MM-dd")}'
and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}' and  b1.sellerid =0 and a1.vendorid not in (105,115,104)
and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%') ) b2 
  group by b2.day)c
  group by c.day)d  on  a.day=d.day

left join 

 (select ----国内接入
 to_date(a.orderdate) as day,
count (distinct a.orderid) as guoneiorder
from
dw_htlmaindb.FactHotelOrder_All_Inn a 
where
to_date(a.orderdate) >='${zdt.addDay(-8).format("yyyy-MM-dd")}'	and to_date(a.orderdate) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
and a.d ='${zdt.format("yyyy-MM-dd")}'  and (a.ordertype_ubt not in ('hotel_order') or a.ordertype_ubt is null)
group by
to_date(a.orderdate))e on  a.day=e.day  
sort by a.day desc