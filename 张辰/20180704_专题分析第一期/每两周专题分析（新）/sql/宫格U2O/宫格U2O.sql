----昨天UV
select user_d.cityid from 
(select get_json_object(value, '$.cityid') as cityid
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d = '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
GROUP BY get_json_object( VALUE, '$.cityid')) user_d

--昨天民宿，客栈的订单量
inner join
(select a.cityid
  , sum (a.ois) as ois
from
(select a.cityid
  ,sum(if(a.terminalType=10, 1, 0)) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.terminalType
    , a1.orderid
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d= '2018-06-30'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d= '2018-06-30'
where substring(b1.createdtime,0,10) ='2018-06-29'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-30' and b1.sellerid=0) a
group by a.cityid
union all
select b.cityid
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) ='2018-06-29'
  and d ='2018-06-30' ) b
group by b.cityid)a group by a.cityid) oi_d
on user_d.cityid = oi_d.cityid

--昨天民宿的订单量
inner join
(select a.cityid
  ,sum(if(a.terminalType=10, 1, 0)) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.terminalType
    , a1.orderid
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d= '2018-06-30'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d= '2018-06-30'
where substring(b1.createdtime,0,10) ='2018-06-29'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-30' and b1.sellerid=0) a
group by a.cityid) bnb_oi
on user_d.cityid = bnb_oi.cityid

--昨天客栈的订单量
inner join
(select b.cityid
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) ='2018-06-29'
  and d ='2018-06-30' ) b
group by b.cityid)a group by a.cityid) hoteloi_d
on user_d.cityid = hoteloi_d.cityid

----一周的UV
select user_d.cityid from 
(select get_json_object(value, '$.cityid') as cityid
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d >= '2018-06-23'
and d <= '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
GROUP BY get_json_object( VALUE, '$.cityid')) user_d

--一周的民宿，客栈的订单量
inner join
(select a.cityid
  , sum (a.ois) as ois
from
(select a.cityid
  ,sum(if(a.terminalType=10, 1, 0)) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.terminalType
    , a1.orderid
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d= '2018-06-30'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d= '2018-06-30'
where substring(b1.createdtime,0,10) >= '2018-06-23'
  and substring(b1.createdtime,0,10) <= '2018-06-29'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-30' and b1.sellerid=0) a
group by a.cityid
union all
select b.cityid
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) >= '2018-06-23'
  and substring(orderdate,0,10) <= '2018-06-29'
  and d ='2018-06-30' ) b
group by b.cityid)a group by a.cityid) oi_d
on user_d.cityid = oi_d.cityid

--昨天民宿的订单量
inner join
(select a.cityid
  ,sum(if(a.terminalType=10, 1, 0)) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.terminalType
    , a1.orderid
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d= '2018-06-30'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d= '2018-06-30'
where substring(b1.createdtime,0,10) >= '2018-06-23'
  and substring(b1.createdtime,0,10) <= '2018-06-29'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-30' and b1.sellerid=0) a
group by a.cityid) bnb_oi
on user_d.cityid = bnb_oi.cityid

--昨天客栈的订单量
inner join
(select b.cityid
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) >= '2018-06-23'
  and substring(orderdate,0,10) <= '2018-06-29'
  and d ='2018-06-30' ) b
group by b.cityid)a group by a.cityid) hoteloi_d
on user_d.cityid = hoteloi_d.cityid