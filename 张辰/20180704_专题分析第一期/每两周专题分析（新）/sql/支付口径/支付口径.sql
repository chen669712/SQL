select c.day,sum(c.directorder) as `宫格支付直接订单`
from
(select to_date(a.orderdate) as day,round(count(distinct a.orderid)*0.85,0) as directorder
from
dw_htlmaindb.FactHotelOrder_All_Inn a
where to_date(a.orderdate)>='2017-06-01' and to_date(a.orderdate)<='2018-06-28'and a.d='2018-06-28' 
group by to_date(a.orderdate)
union all
select b1.day,count(distinct b1.ctriporderid) as directorder
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
where
a.d='2018-06-28' and to_date(a.createtime)<='2018-06-28' and to_date(a.createtime)>='2018-06-28'-8
and a.saleamount>=20 )b1 where b1.orderstatus in('预订成功','房东拒单','支付成功')
group by b1.day--------老订单支付成功订单-----------
union all
select b2.day,count(distinct b2.ctriporderid) as directorder
from
(select to_date(b1.createdtime) as day,a1.orderid as ctriporderid,a1.saleamount,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output 
from
ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-06-28'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-06-28'

where to_date(b1.createdtime)<='2018-06-28'and to_date(b1.createdtime)>='2018-06-28'
and a1.saleamount>=20 and a1.d='2018-06-28' and uid not in('$seller-agent') and
b1.visitsource not in (13,14,18) and
(a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%') ) b2 
group by b2.day)c
group by c.day