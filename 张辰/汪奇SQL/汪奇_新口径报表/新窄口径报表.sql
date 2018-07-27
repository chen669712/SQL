select d
  , count (distinct clientcode ) as uv
from dw_mobdb.factmbpageview
where d >= '2018-07-10'
  and d <= '2018-07-16'
  and pagecode = '600003560'
group by d

select search.d
  , count (distinct search.cid ) as `完成搜索`
  , count (distinct list.cid) as `列表页`
  , count (distinct detail.cid) as `详情页`
  , count (distinct booking.cid) as `填写页`
  , count (distinct submit.orderid) as `提交单`
  , count (distinct pay.orderid) as `支付单`
from
(select distinct d
    ,newvalue.data['env_clientcode'] as cid
from dw_mobdb.factmbtracelog_hybrid
where d >= '2018-07-11'
  and d <= '2018-07-17'
  and key = 'bnb_inn_list_app_basic')search
left outer join
(select distinct d
  ,clientcode as cid
from dw_mobdb.factmbpageview
where d >= '2018-07-11'
  and d <= '2018-07-17'
  and pagecode = '600003563') list on list.d=search.d and list.cid=search.cid
left outer join
(select distinct d
  ,clientcode as cid
from dw_mobdb.factmbpageview
where d >= '2018-07-11'
  and d <= '2018-07-17'
  and pagecode in ('600003564','10320677404')) detail on detail.d=search.d and detail.cid=search.cid
left outer join
(select distinct d
  ,clientcode as cid
from dw_mobdb.factmbpageview
where d >= '2018-07-11'
  and d <= '2018-07-17'
  and pagecode in ('600003570','10320677405')) booking on booking.d=search.d and booking.cid=search.cid
left outer join
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-18'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-18'
where substring(b1.createdtime,0,10)>='2018-07-11'
  and substring(b1.createdtime,0,10)<='2018-07-17'
  and a1.d='2018-07-18'
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate)>='2018-07-11'
  and to_date(orderdate)<='2018-07-17'
  and subchannel='h5_kezhan'
  and d>='2018-07-11'
  and d<='2018-07-17') submit on submit.d=search.d and submit.cid=search.cid
left outer join
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-18'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-18'
where substring(b1.createdtime,0,10)>='2018-07-11'
  and substring(b1.createdtime,0,10)<='2018-07-17'
  and a1.d='2018-07-18'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate)>='2018-07-11'
  and to_date(orderdate)<='2018-07-17'
  and subchannel='h5_kezhan'
  and d>='2018-07-11'
  and d<='2018-07-17'
  and orderstatus not in ('W')) pay on pay.d = search.d and pay.cid=search.cid
group by search.d;
