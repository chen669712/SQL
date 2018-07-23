SELECT DATE_FORMAT(oh.createdTime, '%H') AS '时间'
  , count(*) AS '支付成功数'
FROM order_header oh
WHERE date_format(oh.createdTime, '%Y-%m-%d') >= date_format('2018-04-18', '%Y-%m-%d')
    AND date_format(oh.createdTime, '%Y-%m-%d') < date_format('2018-04-19', '%Y-%m-%d')
    AND oh.payStatusId IN ( 12, 20, 22, 23 )
    AND oh.salesChannel = 1
GROUP BY DATE_FORMAT(oh.createdTime, '%H')
Order by DATE_FORMAT(oh.createdTime, '%H') desc


-- 1. U2O = 宫格曾经支付成功过的订单量 / 宫格DAU
-- 2. 大住宿内宫格订单占比 = 宫格曾经支付成功过的订单量 / 大住宿曾经支付成功过的订单量
-- 3. 大住宿内宫格途家订单占比 = 宫格曾经支付成功过的途家订单量 / 大住宿曾经支付成功过的订单量
-- 4. 大住宿内宫格用户占比 = 宫格DAU / 大住宿DAU
-- 5. APP内宫格用户占比 = 宫格DAU / APP整体DAU
-- 6. 途家内宫格订单占比 = 宫格曾经支付成功过的途家订单量 / 途家所有渠道曾经支付成功过的订单总量



select bnboi.d
  , bnboi.ois as `民宿订单`
  , htloi.ois as `酒店订单`
--  , bnbuv.uv as `民宿DAU`
--  , htluv.uv as `酒店DAU`
--  , appuv.uv as `App整体DAU`
  , concat(cast(100*((bnboi.ois)/bnbuv.uv) as decimal(5,2)),'%') as `U2O`
  , concat(cast(100*((bnboi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格订单占比`
  , concat(cast(100*((tujiaoi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格途家订单占比`
  , concat(cast(100*((bnbuv.uv)/htluv.uv) as decimal(5,2)),'%') as `大住宿内宫格用户占比`
  , concat(cast(100*((bnbuv.uv)/appuv.uv) as decimal(5,2)),'%') as `App内宫格用户占比`
from
(select to_date(ordertime) as d
  , count (distinct orderid) as ois
from bnb_hive_db.bnb_orderinfo
where d = $effectdate('yyyy-MM-dd',-1)
  and to_date(ordertime)>=$effectdate('yyyy-MM-dd',-8)
  and to_date(ordertime)<=$effectdate('yyyy-MM-dd',-1)
  and source in('100','101')
  and (visitsource is null or visitsource not in('13','14', '18'))
  and (sellerid is null or sellerid = '0')
group by to_date(ordertime))bnboi
left join
(select to_date(ordertime) as d
  , count (distinct orderid) as ois
from bnb_hive_db.bnb_orderinfo
where d = $effectdate('yyyy-MM-dd',-1)
  and to_date(ordertime)>=$effectdate('yyyy-MM-dd',-8)
  and to_date(ordertime)<=$effectdate('yyyy-MM-dd',-1)
  and source in('100','101')
  and (visitsource is null or visitsource not in('13','14','18'))
  and (sellerid is null or sellerid = '0')
  and vendorid in (105,115)
group by to_date(ordertime))tujiaoi on tujiaoi.d = bnboi.d
left join
(select oi.d
  , count (distinct oi.orderid) as ois
from
  (select to_date(orderdate) as d
    , orderid
  from dw_htlmaindb.facthotelorder
  where d>=$effectdate('yyyy-MM-dd',-8)
    and d<=$effectdate('yyyy-MM-dd',-1)) oi
  inner join
  (select d
    , orderid
   from ods_htl_orderdb.ord_ltp_paymentinfo
  where d=$effectdate('yyyy-MM-dd',0)
    and paymentstatus = 2) olp on oi.orderid = olp.orderid
group by oi.d)htloi on htloi.d = bnboi.d  ---大住宿订单
left join
(select  d,
  count(distinct  clientcode) as uv
from DW_MobDB.factmbpageview
where d>=$effectdate('yyyy-MM-dd',-8) and d<=$effectdate('yyyy-MM-dd',-1)
  and pagecode in ('600003560', '10320675332')
  and prepagecode in ('home', '215019', '0')
group by d) bnbuv on bnbuv.d = bnboi.d
left join
(select  d,
  count(distinct  clientcode) as uv
from DW_MobDB.factmbpageview
where d>=$effectdate('yyyy-MM-dd',-8)  and d<=$effectdate('yyyy-MM-dd',-1)
  and pagecode in ('hotel_inland_inquire', 'hotel_oversea_inquire')
  and prepagecode in ('home', '0')
group by d)htluv on htluv.d = bnboi.d
left join
(select  d,
  count(distinct  clientcode) as uv
from DW_MobDB.factmbpageview
where d>=$effectdate('yyyy-MM-dd',-8)  and d<=$effectdate('yyyy-MM-dd',-1)
  and pagecode in ('home')
group by d)appuv on appuv.d = bnboi.d



select bnboi.d
  , bnboi.ois as `民宿订单`
  , concat(cast(100*((bnboi.ois)/bnbuvuv) as decimal(5,2)),'%') as `U2O`
  , concat(cast(100*((bnboi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格订单占比`
  , concat(cast(100*((tujiaoi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格途家订单占比`
  , concat(cast(100*((bnbuvuv)/htluvuv) as decimal(5,2)),'%') as `大住宿内宫格用户占比`
  , concat(cast(100*((bnbuvuv)/appuvuv) as decimal(5,2)),'%') as `App内宫格用户占比`
from
(select substring(ordertime,0,10) as d
  , count (distinct orderid) as ois
from bnb_hive_db.bnb_orderinfo
where d = "$effectdate('yyyy-MM-dd',-1)"
  and substring(ordertime,0,10)>="$effectdate('yyyy-MM-dd',-8)"
  and substring(ordertime,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and source in('100','101')
  and (visitsource is null or visitsource not in('13','14', '18'))
  and (sellerid is null or sellerid = '0')
group by substring(ordertime,0,10)) bnboi
join
(select substring(ordertime,0,10) as d
  , count (distinct orderid) as ois
from bnb_hive_db.bnb_orderinfo
where d = "$effectdate('yyyy-MM-dd',-1)"
  and substring(ordertime,0,10)>="$effectdate('yyyy-MM-dd',-8)"
  and substring(ordertime,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and source in('100','101')
  and (visitsource is null or visitsource not in('13','14','18'))
  and (sellerid is null or sellerid = '0')
  and vendorid in (105,115)
group by substring(ordertime,0,10))tujiaoi on tujiaoi.d = bnboi.d
join
(select oi.d
  , count (distinct oi.orderid) as ois
from
  (select substring(orderdate,0,10) as d
    , orderid
  from dw_htlmaindb.facthotelorder
  where d>="$effectdate('yyyy-MM-dd',-8)"
    and d<="$effectdate('yyyy-MM-dd',-1)") oi
  inner join
  (select d
    , orderid
   from ods_htl_orderdb.ord_ltp_paymentinfo
  where d="$effectdate('yyyy-MM-dd',-1)"
    and paymentstatus = 2) olp on oi.orderid = olp.orderid
group by oi.d)htloi on htloi.d = bnboi.d
join
(select  d,
  count(distinct  clientcode) as bnbuvuv
from DW_MobDB.factmbpageview
where d>="$effectdate('yyyy-MM-dd',-8)" and d<="$effectdate('yyyy-MM-dd',-1)"
  and pagecode in ('600003560', '10320675332')
  and prepagecode in ('home', '215019', '0')
group by d) bnbuv on bnbuv.d = bnboi.d
join
(select  d,
  count(distinct  clientcode) as htluvuv
from DW_MobDB.factmbpageview
where d>="$effectdate('yyyy-MM-dd',-8)"  and d<="$effectdate('yyyy-MM-dd',-1)"
  and pagecode in ('hotel_inland_inquire', 'hotel_oversea_inquire')
  and prepagecode in ('home', '0')
group by d)htluv on htluv.d = bnboi.d
join
(select  d,
  count(distinct  clientcode) as appuvuv
from DW_MobDB.factmbpageview
where d>="$effectdate('yyyy-MM-dd',-8)"  and d<="$effectdate('yyyy-MM-dd',-1)"
  and pagecode in ('home')
group by d)appuv on appuv.d = bnboi.d



select bnboi.d
  , bnboi.ois as `民宿订单`
  , htloi.ois as `酒店订单`
--  , bnbuv.uv as `民宿DAU`
--  , htluv.uv as `酒店DAU`
--  , appuv.uv as `App整体DAU`
  , concat(cast(100*((bnboi.ois)/bnbuv.uv) as decimal(5,2)),'%') as `U2O`
  , concat(cast(100*((bnboi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格订单占比`
  , concat(cast(100*((tujiaoi.ois)/htloi.ois) as decimal(5,2)),'%') as `大住宿内宫格途家订单占比`
  , concat(cast(100*((bnbuv.uv)/htluv.uv) as decimal(5,2)),'%') as `大住宿内宫格用户占比`
  , concat(cast(100*((bnbuv.uv)/appuv.uv) as decimal(5,2)),'%') as `App内宫格用户占比`
from
(select to_date(ordertime) as d
  , count (distinct orderid) as ois
from bnb_hive_db.bnb_orderinfo
where d = '2018-04-22'
  and to_date(ordertime)>='2018-04-08' and to_date(ordertime)<='2018-04-14'
  and source in('100','101')
  and (visitsource is null or visitsource not in('13','14', '18'))
  and (sellerid is null or sellerid = '0')
group by to_date(ordertime))bnboi
left join
(select to_date(ordertime) as d
  , count (distinct orderid) as ois
from bnb_hive_db.bnb_orderinfo
where d = '2018-04-22'
  and to_date(ordertime)>='2018-04-08' and to_date(ordertime)<='2018-04-14'
  and source in('100','101')
  and (visitsource is null or visitsource not in('13','14','18'))
  and (sellerid is null or sellerid = '0')
  and vendorid in (105,115)
group by to_date(ordertime))tujiaoi on tujiaoi.d = bnboi.d
left join
(select oi.d
  , count (distinct oi.orderid) as ois
from
  (select to_date(orderdate) as d
    , orderid
  from dw_htlmaindb.facthotelorder
  where d>='2018-04-08'
    and d<='2018-04-14') oi
  inner join
  (select d
    , orderid
   from ods_htl_orderdb.ord_ltp_paymentinfo
  where d='2018-04-22'
    and paymentstatus = 2) olp on oi.orderid = olp.orderid
group by oi.d)htloi on htloi.d = bnboi.d  ---大住宿订单
left join
(select  d,
  count(distinct  clientcode) as uv
from DW_MobDB.factmbpageview
where d>='2018-04-08' and d<='2018-04-14'
  and pagecode in ('600003560', '10320675332')
  and prepagecode in ('home', '215019', '0')
group by d) bnbuv on bnbuv.d = bnboi.d
left join
(select  d,
  count(distinct  clientcode) as uv
from DW_MobDB.factmbpageview
where d>='2018-04-08'  and d<='2018-04-14'
  and pagecode in ('hotel_inland_inquire', 'hotel_oversea_inquire')
  and prepagecode in ('home', '0')
group by d)htluv on htluv.d = bnboi.d
left join
(select  d,
  count(distinct  clientcode) as uv
from DW_MobDB.factmbpageview
where d>='2018-04-08'  and d<='2018-04-14'
  and pagecode in ('home')
group by d)appuv on appuv.d = bnboi.d



----计算DAU，订单数，转化率
set beginDay= '2018-04-01';
set calcDay= '2018-05-01';
select bnboi.d
  , bnboi.ois as `民宿订单`
  , bnbuv.uv as `民宿DAU`
  , concat(cast(100*((bnboi.ois)/bnbuv.uv) as decimal(5,2)),'%') as `U2O`
from
(select to_date(ordertime) as d
  , count (distinct orderid) as ois
from bnb_hive_db.bnb_orderinfo
where d = ${hiveconf:calcDay}
  and to_date(ordertime)>=${hiveconf:beginDay}
  and source in('100','101')
  and (visitsource is null or visitsource not in('13','14', '18'))
  and (sellerid is null or sellerid = '0')
group by to_date(ordertime))bnboi
left join
(select  d,
  count(distinct  clientcode) as uv
from bnb_hive_db.bnb_pageview
where d>=${hiveconf:beginDay}
group by d) bnbuv on bnbuv.d = bnboi.d



