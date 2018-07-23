--新口径U2O
select search.d
  , count (distinct search.cid ) as `完成搜索`
  , count (distinct pay.orderid) as `支付单`
from
(select distinct d
    ,cid
from bnb_hive_db.bnb_tracelog
where d >= '2018-01-01'
  and d <= '2018-07-19'
  and key = 'bnb_inn_list_app_basic')search
left outer join
(select substring(b1.createdtime, 0, 10) as d
        , a1.orderid
        , b1.clientid as cid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-20'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-20'
where substring(b1.createdtime,0,10)>='2018-01-01'
  and substring(b1.createdtime,0,10)<='2018-07-19'
  and a1.d='2018-07-20'
  and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
union all
select to_date(orderdate) as d
  , orderid
  , clientid as cid
from dwhtl.edw_htl_order_all_orderdate
where to_date(orderdate)>='2018-01-01'
  and to_date(orderdate)<='2018-07-19'
  and subchannel='h5_kezhan'
  and d>='2018-01-01'
  and d<='2018-07-19'
  and orderstatus not in ('W')) pay on pay.d = search.d and pay.cid=search.cid
group by search.d;
--------------------------------------------------------------------------------------------------------------
--老口径U2O
select home.d,concat(cast(100*(pay.ois/home.uv) as decimal(5,2)),'%') as `U2O`
from
(select d,count(distinct clientcode) uv 
  from bnb_hive_db.bnb_pageview where d >= '2018-01-01' and d<= '2018-07-19' group by d) home
join
(select a.d                -----支付单
  , sum (a.ois) as ois
from
(select a.d
  ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
      , sum(if(a.applicationType=10 or a.applicationType is null or a.applicationType=0, 1, 0))) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.applicationType
    , b1.terminalType
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-07-20'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-20'
where substring(b1.createdtime,0,10)>='2018-01-01'
  and substring(b1.createdtime,0,10)<='2018-07-19'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-07-20' and b1.sellerid=0) a
group by a.d
union all
select b.d
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10)>='2018-01-01'
  and substring(orderdate,0,10)<='2018-07-19'
  and d ='2018-07-20') b
  group by b.d)a group by a.d) pay on home.d = pay.d
group by home.d