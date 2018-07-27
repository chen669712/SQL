select city.cityname
  , count (distinct search_d.cid ) as DAU
  , count (distinct pay_d.orderid) as order_d
  , concat(round(count (distinct pay_d.orderid)/count (distinct search_d.cid )*100,2),'%') as conversion_d 
  , count (distinct bnbpay_d.orderid ) as bnborder_d
  , count (distinct hotelpay_d.orderid) as hotelorder_d
from
--城市DAU
(select distinct d
    ,cid
    ,get_json_object(value,'$.cityid') as cityid
from bnb_hive_db.bnb_tracelog
where d = '2018-07-22'
  and key = 'bnb_inn_list_app_basic')search_d
left outer join
--日城市民宿客栈订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
        , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-23'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-23'
where substring(b1.createdtime,0,10) ='2018-07-22'
  and a1.d='2018-07-23'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
  , cityid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) ='2018-07-22'
  and subchannel='h5_kezhan'
  and d ='2018-07-22'
  and orderstatus not in ('W')) pay_d 
on pay_d.d = search_d.d and pay_d.cid=search_d.cid and pay_d.cityid=search_d.cityid
left outer join 
--日城市民宿订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
        , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-23'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-23'
where substring(b1.createdtime,0,10) ='2018-07-22'
  and a1.d='2018-07-23'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')) bnbpay_d
on bnbpay_d.d = search_d.d and bnbpay_d.cid=search_d.cid and bnbpay_d.cityid=search_d.cityid
left outer join
--日城市客栈订单
(select to_date(orderdate) as d
  , orderid
  , clientid as cid
  , cityid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) ='2018-07-22'
  and subchannel='h5_kezhan'
  and d ='2018-07-22'
  and orderstatus not in ('W')) hotelpay_d 
on hotelpay_d.d = search_d.d and hotelpay_d.cid=search_d.cid and hotelpay_d.cityid=search_d.cityid
left outer join ods_htl_groupwormholedb.bnb_city city on search_d.cityid = city.cityid and city.d = '2018-07-23'
group by city.cityname
union all
select  '汇总' as cityname
  , count (distinct search_d.cid ) as DAU
  , count (distinct pay_d.orderid) as order_d
  , concat(round(count (distinct pay_d.orderid)/count (distinct search_d.cid )*100,2),'%') as conversion_d 
  , count (distinct bnbpay_d.orderid ) as bnborder_d
  , count (distinct hotelpay_d.orderid) as hotelorder_d
from
--日汇总DAU
(select distinct d
    ,cid
from bnb_hive_db.bnb_tracelog
where d = '2018-07-22'
  and key = 'bnb_inn_list_app_basic')search_d
left outer join
--日汇总民宿客栈订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-23'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-23'
where substring(b1.createdtime,0,10) ='2018-07-22'
  and a1.d='2018-07-23'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) ='2018-07-22'
  and subchannel='h5_kezhan'
  and d ='2018-07-22'
  and orderstatus not in ('W')) pay_d 
on pay_d.d = search_d.d and pay_d.cid=search_d.cid
left outer join 
--日汇总民宿订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-23'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-23'
where substring(b1.createdtime,0,10) ='2018-07-22'
  and a1.d='2018-07-23'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')) bnbpay_d
on bnbpay_d.d = search_d.d and bnbpay_d.cid=search_d.cid
left outer join
--日汇总客栈订单
(select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) ='2018-07-22'
  and subchannel='h5_kezhan'
  and d ='2018-07-22'
  and orderstatus not in ('W')) hotelpay_d 
on hotelpay_d.d = search_d.d and hotelpay_d.cid=search_d.cid
group by '汇总'

