--adhoc使用
select city.cityname
  , count (distinct search_w.cid ) as WAU
  , count (distinct pay_w.orderid) as order_w
  , concat(round(count (distinct pay_w.orderid)/count (distinct search_w.cid )*100,2),'%') as conversion_w 
  , count (distinct bnbpay_w.orderid ) as bnborder_w
  , count (distinct hotelpay_w.orderid) as hotelorder_w
from
--WAU
(select distinct d
    ,cid
    ,get_json_object(value,'$.cityid') as cityid
from bnb_hive_db.bnb_tracelog
where d >= '2018-07-13'
  and d <= '2018-07-19'
  and key = 'bnb_inn_list_app_basic')search_w
left outer join
--周城市民宿客栈订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
        , c1.cityid
from  ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-20'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-20'
where substring(b1.createdtime,0,10) >='2018-07-13'
  and substring(b1.createdtime,0,10) <='2018-07-19'
  and a1.d='2018-07-20'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
  , cityid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >='2018-07-13'
  and to_date(orderdate) <='2018-07-19'
  and subchannel='h5_kezhan'
  and d >='2018-07-13'
  and d <='2018-07-19'
  and orderstatus not in ('W')) pay_w 
on pay_w.d = search_w.d and pay_w.cid=search_w.cid and pay_w.cityid=search_w.cityid
left outer join 
--周城市民宿订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
        , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-20'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-20'
where substring(b1.createdtime,0,10) >='2018-07-13'
  and substring(b1.createdtime,0,10) <='2018-07-19'
  and a1.d='2018-07-20'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')) bnbpay_w
on bnbpay_w.d = search_w.d and bnbpay_w.cid=search_w.cid and bnbpay_w.cityid=search_w.cityid
left outer join
--周城市客栈订单
(select to_date(orderdate) as d
  , orderid
  , clientid as cid
  , cityid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >='2018-07-13'
  and to_date(orderdate) <='2018-07-19'
  and subchannel='h5_kezhan'
  and d >='2018-07-13'
  and d <='2018-07-19'
  and orderstatus not in ('W')) hotelpay_w 
on hotelpay_w.d = search_w.d and hotelpay_w.cid=search_w.cid and hotelpay_w.cityid=search_w.cityid
left outer join ods_htl_groupwormholedb.bnb_city city on search_w.cityid = city.cityid and city.d = '2018-07-20'
group by city.cityname
union all
select  '汇总' as cityname
  , count (distinct search_w.cid ) as WAU
  , count (distinct pay_w.orderid) as order_w
  , concat(round(count (distinct pay_w.orderid)/count (distinct search_w.cid )*100,2),'%') as conversion_w 
  , count (distinct bnbpay_w.orderid ) as bnborder_w
  , count (distinct hotelpay_w.orderid) as hotelorder_w
from
--周汇总WAU
(select distinct d
    ,cid
from bnb_hive_db.bnb_tracelog
where d >= '2018-07-13'
  and d <= '2018-07-19'
  and key = 'bnb_inn_list_app_basic')search_w
left outer join
--周汇总民宿客栈订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-20'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-20'
where substring(b1.createdtime,0,10) >= '2018-07-13'
  and substring(b1.createdtime,0,10) <= '2018-07-19'
  and a1.d='2018-07-20'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >= '2018-07-13'
  and to_date(orderdate) <= '2018-07-19'
  and subchannel='h5_kezhan'
  and d >= '2018-07-13'
  and d <= '2018-07-19'
  and orderstatus not in ('W')) pay_w 
on pay_w.d = search_w.d and pay_w.cid=search_w.cid
left outer join 
--周汇总民宿订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-20'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-20'
where substring(b1.createdtime,0,10) >='2018-07-13'
  and substring(b1.createdtime,0,10) <='2018-07-19'
  and a1.d='2018-07-20'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')) bnbpay_w
on bnbpay_w.d = search_w.d and bnbpay_w.cid=search_w.cid
left outer join
--周汇总客栈订单
(select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >='2018-07-13'
  and to_date(orderdate) <='2018-07-19'
  and subchannel='h5_kezhan'
  and d >= '2018-07-13'
  and d <= '2018-07-19'
  and orderstatus not in ('W')) hotelpay_w 
on hotelpay_w.d = search_w.d and hotelpay_w.cid=search_w.cid
group by '汇总'