use bnb_hive_db;
CREATE TABLE bnb_user_distribution(
  cid string COMMENT '用户的cid'
  , uid string COMMENT '用户的uid'
	, homeFlag string COMMENT '宫格首页UV'
	, listFlag string COMMENT '宫格列表页UV'
	, detailFlag string COMMENT '宫格详情页UV'
	, fillFlag string COMMENT '宫格填写页UV'
)COMMENT '无线民宿订单表'
PARTITIONED BY (`d` string COMMENT 'date')


use bnb_hive_db;
insert overwrite table bnb_user_distribution
partition(d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')
select home.clientcode
  , null as uid
  , if(home.clientcode is null, 0, 1) as homeFlag
  , if(list.clientcode is null, 0, 1) as listFlag
  , if(detail.clientcode is null, 0, 1) as detailFlag
  , if(fill.clientcode is null, 0, 1) as fillFlag
from
(select distinct clientcode
from bnb_pageview
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')home
left outer join
(select distinct clientcode
from bnb_pageview
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and pagecode ='600003563'
  and prepagecode ='600003560')list on home.clientcode = list.clientcode
left outer join
(select a.clientcode
  from
  (select distinct clientcode
  from bnb_pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and pagecode in ('hotel_inland_detail')    -- 国内酒店老详情页
    and prepagecode IN ('600003563')
  union all
  select distinct clientcode
  from bnb_pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and pagecode in ('10320677404')    -- 国内酒店新详情页
    and prepagecode IN ('600003563')
  union all
  select distinct clientcode
  from bnb_pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and pagecode in ('600003564')    -- 600003564 民宿产品详情页
    and prepagecode IN ('600003563')) a)detail on list.clientcode = detail.clientcode
left outer join
(select a.clientcode
 from
 (select distinct clientcode
  from bnb_pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and pagecode in ('hotel_inland_order')    -- 客栈订单填写页
    and prepagecode IN ('hotel_inland_detail')
  union all
  select distinct clientcode
  from bnb_pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and pagecode in ('10320677405')    -- 新客栈订单填写页
    and prepagecode IN ('10320677404')
  union all
  select distinct clientcode
  from bnb_pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and pagecode in ('600003570')    -- 600003564 订单填写页
    and prepagecode IN ('600003564')) a)fill on detail.clientcode = fill.clientcode

----------------------------------------------------------------------------
-- 基于流量分布表计算宫格的转换
----------------------------------------------------------------------------
select uv.d
  ,uv.home as `首页UV`
  ,pay.ois as `支付订单`
  ,concat(cast(100*(pay.ois/uv.home) as decimal(5,2)),'%') as `U2O`
	,concat(cast(100*(uv.list/uv.home) as decimal(5,2)),'%') as `S2L`
	,concat(cast(100*(uv.detail/uv.list) as decimal(5,2)),'%') as `L2D`
	,concat(cast(100*(uv.fill/uv.detail) as decimal(5,2)),'%') as `D2B`
	,concat(cast(100*(submit.ois/uv.fill) as decimal(5,2)),'%') as `B2提交`
	,concat(cast(100*(pay.ois/submit.ois) as decimal(5,2)),'%') as `提交2有效`
from
(select a.d
  , sum(if(homeFlag=1 , num, 0))as home
  , sum(if(listFlag=1, num, 0)) as list
  , sum(if(detailFlag=1, num, 0)) as detail
  , sum(if(fillFlag=1, num, 0)) as fill
  from
  (select d
    , homeFlag
    , listFlag
    , detailFlag
    , fillFlag
    , count(distinct cid) as num
  from bnb_hive_db.bnb_user_distribution
  where d>="$effectdate('yyyy-MM-dd',-16)"
    and d<="$effectdate('yyyy-MM-dd',-1)"
  group by d, homeFlag, listFlag, detailFlag, fillFlag)a
  group by a.d) uv
join
(select a.d                     -----提交单
  , sum (a.ois) as ois
  from
  (select a.d
      ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
          , sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois
  from
    (select distinct substring(b1.createdtime, 0, 10) as d
        , b1.applicationType
        , b1.terminalType
        , a1.orderid
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)"
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"
    where substring(b1.createdtime,0,10)>="$effectdate('yyyy-MM-dd',-16)"
      and substring(b1.createdtime,0,10)<="$effectdate('yyyy-MM-dd',-1)"
      and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
      and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)" and b1.sellerid=0) a
    group by a.d

    union all

    select b.d
      , count(distinct b.orderid) as ois
    from
    (select substring(orderdate, 0, 10) as d
      ,orderid
    from dw_htlmaindb.FactHotelOrder_All_Inn
    where substring(orderdate,0,10)>="$effectdate('yyyy-MM-dd',-16)"
      and substring(orderdate,0,10)<="$effectdate('yyyy-MM-dd',-1)"
      and d ="$effectdate('yyyy-MM-dd', 0)" ) b
    group by b.d
  )a group by a.d) submit on uv.d = submit.d
join
(select a.d                -----支付单
  , sum (a.ois) as ois
from
(select a.d
  ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
      , sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.applicationType
    , b1.terminalType
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)"
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"
where substring(b1.createdtime,0,10)>="$effectdate('yyyy-MM-dd',-16)"
  and substring(b1.createdtime,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)" and b1.sellerid=0) a
group by a.d
union all
select b.d
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10)>="$effectdate('yyyy-MM-dd',-16)"
  and substring(orderdate,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and d ="$effectdate('yyyy-MM-dd', 0)" ) b
  group by b.d)a group by a.d) pay on uv.d = pay.d



------------------------------------------------------------------------------------
-- Hive取数逻辑
------------------------------------------------------------------------------------
select uv.d
  ,uv.home as `首页UV`
  ,pay.ois as `支付订单`
  ,concat(cast(100*(pay.ois/uv.home) as decimal(5,2)),'%') as `U2O`
	,concat(cast(100*(uv.list/uv.home) as decimal(5,2)),'%') as `S2L`
	,concat(cast(100*(uv.detail/uv.list) as decimal(5,2)),'%') as `L2D`
	,concat(cast(100*(uv.fill/uv.detail) as decimal(5,2)),'%') as `D2B`
	,concat(cast(100*(submit.ois/uv.fill) as decimal(5,2)),'%') as `B2提交`
	,concat(cast(100*(pay.ois/submit.ois) as decimal(5,2)),'%') as `提交2有效`
from
(select a.d
  , sum(if(homeFlag=1 , num, 0))as home
  , sum(if(listFlag=1, num, 0)) as list
  , sum(if(detailFlag=1, num, 0)) as detail
  , sum(if(fillFlag=1, num, 0)) as fill
  from
  (select d
    , homeFlag
    , listFlag
    , detailFlag
    , fillFlag
    , count(distinct cid) as num
  from bnb_hive_db.bnb_user_distribution
  where d>="$effectdate('yyyy-MM-dd',-16)"
    and d<="$effectdate('yyyy-MM-dd',-1)"
  group by d, homeFlag, listFlag, detailFlag, fillFlag)a
  group by a.d) uv
join
(select a.d                     -----提交单
  , sum (a.ois) as ois
  from
  (select a.d
      ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
          , sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois
  from
    (select distinct substring(b1.createdtime, 0, 10) as d
        , b1.applicationType
        , b1.terminalType
        , a1.orderid
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)"
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"
    where substring(b1.createdtime,0,10)>="$effectdate('yyyy-MM-dd',-16)"
      and substring(b1.createdtime,0,10)<="$effectdate('yyyy-MM-dd',-1)"
      and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
      and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)" and b1.sellerid=0) a
    group by a.d

    union all

    select b.d
      , count(distinct b.orderid) as ois
    from
    (select substring(orderdate, 0, 10) as d
      ,orderid
    from dw_htlmaindb.FactHotelOrder_All_Inn
    where substring(orderdate,0,10)>="$effectdate('yyyy-MM-dd',-16)"
      and substring(orderdate,0,10)<="$effectdate('yyyy-MM-dd',-1)"
      and d ="$effectdate('yyyy-MM-dd', 0)" ) b
    group by b.d
  )a group by a.d) submit on uv.d = submit.d
join
(select a.d                -----支付单
  , sum (a.ois) as ois
from
(select a.d
  ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
      , sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.applicationType
    , b1.terminalType
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)"
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"
where substring(b1.createdtime,0,10)>="$effectdate('yyyy-MM-dd',-16)"
  and substring(b1.createdtime,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)" and b1.sellerid=0) a
group by a.d
union all
select b.d
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10)>="$effectdate('yyyy-MM-dd',-16)"
  and substring(orderdate,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and d ="$effectdate('yyyy-MM-dd', 0)" ) b
  group by b.d)a group by a.d) pay on uv.d = pay.d