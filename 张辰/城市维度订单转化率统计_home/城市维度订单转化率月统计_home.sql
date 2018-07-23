use bnb_hive_db;
insert overwrite table bnb_data_order_conversion_rate_m
partition(d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')
select city.cityname
  , count (distinct search_m.cid ) as MAU
  , count (distinct pay_m.orderid) as order_m
  , concat(round(count (distinct pay_m.orderid)/count (distinct search_m.cid )*100,2),'%') as conversion_m 
  , count (distinct bnbpay_m.orderid ) as bnborder_m
  , count (distinct hotelpay_m.orderid) as hotelorder_m
from
--MAU
(select distinct d
    ,cid
    ,get_json_object(value,'$.cityid') as cityid
from bnb_hive_db.bnb_tracelog
where d >= '${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and key = 'bnb_inn_list_app_basic')search_m
left outer join
--月民宿客栈订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
        , c1.cityid
from  ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10) >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and substring(b1.createdtime,0,10) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and a1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
  , cityid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and to_date(orderdate) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and subchannel='h5_kezhan'
  and d >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and d <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and orderstatus not in ('W')) pay_m 
on pay_m.d = search_m.d and pay_m.cid=search_m.cid and pay_m.cityid=search_m.cityid
left outer join 
--月民宿订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
        , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10) >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and substring(b1.createdtime,0,10) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and a1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')) bnbpay_m
on bnbpay_m.d = search_m.d and bnbpay_m.cid=search_m.cid and bnbpay_m.cityid=search_m.cityid
left outer join
--月客栈订单
(select to_date(orderdate) as d
  , orderid
  , clientid as cid
  , cityid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and to_date(orderdate) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and subchannel='h5_kezhan'
  and d >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and d <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and orderstatus not in ('W')) hotelpay_m 
on hotelpay_m.d = search_m.d and hotelpay_m.cid=search_m.cid and hotelpay_m.cityid=search_m.cityid
left outer join ods_htl_groupwormholedb.bnb_city city on search_m.cityid = city.cityid and city.d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
group by city.cityname
union all
select  '汇总' as cityname
  , count (distinct search_m.cid ) as MAU
  , count (distinct pay_m.orderid) as order_m
  , concat(round(count (distinct pay_m.orderid)/count (distinct search_m.cid )*100,2),'%') as conversion_m 
  , count (distinct bnbpay_m.orderid ) as bnborder_m
  , count (distinct hotelpay_m.orderid) as hotelorder_m
from
--月汇总MAU
(select distinct d
    ,cid
from bnb_hive_db.bnb_tracelog
where d >= '${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and key = 'bnb_inn_list_app_basic')search_m
left outer join
--月汇总民宿客栈订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10) >= '${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and substring(b1.createdtime,0,10) <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and a1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >= '${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and to_date(orderdate) <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and subchannel='h5_kezhan'
  and d >= '${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and orderstatus not in ('W')) pay_m 
on pay_m.d = search_m.d and pay_m.cid=search_m.cid
left outer join 
--月汇总民宿订单
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10) >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and substring(b1.createdtime,0,10) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and a1.d='${zdt.addDay(0).format("yyyy-MM-dd")}'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')) bnbpay_m
on bnbpay_m.d = search_m.d and bnbpay_m.cid=search_m.cid
left outer join
--月汇总客栈订单
(select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate) >='${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and to_date(orderdate) <='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and subchannel='h5_kezhan'
  and d >= '${zdt.addDay(-30).format("yyyy-MM-dd")}'
  and d <= '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and orderstatus not in ('W')) hotelpay_m 
on hotelpay_m.d = search_m.d and hotelpay_m.cid=search_m.cid
group by '汇总'
