select  city.cityname as `城市名称`
        , user_d.visitNumber as `DAU`
        , oi_d.ois as `日支付订单数`
        , concat(round(oi_d.ois/user_d.visitNumber*100,2),'%') as `日转化率`
        , bnboi_d.ois as `日民宿订单数`
        , hoteloi_d.ois as `日酒店订单数`
        , user_w.visitNumber as `WAU`
        , oi_w.ois as `周支付订单数`
        , concat(round(oi_w.ois/user_w.visitNumber*100,2),'%') as `周转化率`
        , bnboi_d.ois as `周民宿订单数`
        , hoteloi_d.ois as `周酒店订单数`
        , user_m.visitNumber as `MAU`
        , oi_m.ois as `月支付订单数`
        , concat(round(oi_m.ois/user_m.visitNumber*100,2),'%') as `月转化率`
        , bnboi_m.ois as `月民宿订单数`
        , hoteloi_m.ois as `月酒店订单数`
from 
(select get_json_object(value, '$.cityid') as cityid
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d = '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
GROUP BY get_json_object( VALUE, '$.cityid')) user_d
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
group by a.cityid) bnboi_d
on user_d.cityid = bnboi_d.cityid
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
group by b.cityid) hoteloi_d
on user_d.cityid = hoteloi_d.cityid
inner join
(select get_json_object(value, '$.cityid') as cityid
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d >= '2018-06-23'
and d <= '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
GROUP BY get_json_object( VALUE, '$.cityid')) user_w
on user_d.cityid = user_w.cityid
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
group by b.cityid)a group by a.cityid) oi_w
on user_d.cityid = oi_w.cityid
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
group by a.cityid) bnboi_w
on user_d.cityid = bnboi_w.cityid
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
group by b.cityid) hoteloi_w
on user_d.cityid = hoteloi_w.cityid
inner join
(select get_json_object(value, '$.cityid') as cityid
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d >= '2018-05-31'
and d <= '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
GROUP BY get_json_object( VALUE, '$.cityid')) user_m
on user_d.cityid = user_m.cityid
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
where substring(b1.createdtime,0,10) >= '2018-05-31'
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
where substring(orderdate,0,10) >= '2018-05-31'
  and substring(orderdate,0,10) <= '2018-06-29'
  and d ='2018-06-30' ) b
group by b.cityid)a group by a.cityid) oi_m
on user_d.cityid = oi_m.cityid
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
where substring(b1.createdtime,0,10) >= '2018-05-31'
  and substring(b1.createdtime,0,10) <= '2018-06-29'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-30' and b1.sellerid=0) a
group by a.cityid) bnboi_m
on user_d.cityid = bnboi_m.cityid
inner join
(select b.cityid
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) >= '2018-05-31'
  and substring(orderdate,0,10) <= '2018-06-29'
  and d ='2018-06-30' ) b
group by b.cityid) hoteloi_m
on user_d.cityid = hoteloi_m.cityid
inner join ods_htl_groupwormholedb.bnb_city as city on user_d.cityid = city.cityid and city.d = '2018-06-30'
union all
select  '汇总' as `城市名称`
        , user_d.visitNumber as `DAU`
        , oi_d.ois as `日支付订单数`
        , concat(round(oi_d.ois/user_d.visitNumber*100,2),'%') as `日转化率`
        , bnboi_d.ois as `日民宿订单数`
        , hoteloi_d.ois as `日酒店订单数`
        , user_w.visitNumber as `WAU`
        , oi_w.ois as `周支付订单数`
        , concat(round(oi_w.ois/user_w.visitNumber*100,2),'%') as `周转化率`
        , bnboi_d.ois as `周民宿订单数`
        , hoteloi_d.ois as `周酒店订单数`
        , user_m.visitNumber as `MAU`
        , oi_m.ois as `月支付订单数`
        , concat(round(oi_m.ois/user_m.visitNumber*100,2),'%') as `月转化率`
        , bnboi_m.ois as `月民宿订单数`
        , hoteloi_m.ois as `月酒店订单数`
from 
(select 1 as t
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d = '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
) user_d
inner join
(select 1 as t
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
group by b.cityid)a ) oi_d
on user_d.t= oi_d.t
inner join
(select 1 as t
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
) bnboi_d
on user_d.t = bnboi_d.t
inner join
(select 1 as t
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) ='2018-06-29'
  and d ='2018-06-30' ) b
) hoteloi_d
on user_d.t = hoteloi_d.t
inner join
(select 1 as t
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d >= '2018-06-23'
and d <= '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
) user_w
on user_d.t = user_w.t
inner join
(select 1 as t
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
group by b.cityid)a) oi_w
on user_d.t = oi_w.t
inner join
(select 1 as t
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
) bnboi_w
on user_d.t = bnboi_w.t
inner join
(select 1 as t
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) >= '2018-06-23'
  and substring(orderdate,0,10) <= '2018-06-29'
  and d ='2018-06-30' ) b
) hoteloi_w
on user_d.t = hoteloi_w.t
inner join
(select 1 as t
         , count(distinct newvalue.data['env_clientcode']) as visitNumber
from dw_mobdb.factmbtracelog_hybrid
where d >= '2018-05-31'
and d <= '2018-06-29' 
  AND KEY in ('100641','bnb_inn_list_app_basic')
) user_m
on user_d.t = user_m.t
inner join
(select 1 as t
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
where substring(b1.createdtime,0,10) >= '2018-05-31'
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
where substring(orderdate,0,10) >= '2018-05-31'
  and substring(orderdate,0,10) <= '2018-06-29'
  and d ='2018-06-30' ) b
group by b.cityid)a ) oi_m
on user_d.t = oi_m.t
inner join
(select 1 as t
  ,sum(if(a.terminalType=10, 1, 0)) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.terminalType
    , a1.orderid
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d= '2018-06-30'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d= '2018-06-30'
where substring(b1.createdtime,0,10) >= '2018-05-31'
  and substring(b1.createdtime,0,10) <= '2018-06-29'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-30' and b1.sellerid=0) a
) bnboi_m
on user_d.t= bnboi_m.t
inner join
(select 1 as t
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10) >= '2018-05-31'
  and substring(orderdate,0,10) <= '2018-06-29'
  and d ='2018-06-30' ) b
) hoteloi_m
on user_d.t = hoteloi_m.t
