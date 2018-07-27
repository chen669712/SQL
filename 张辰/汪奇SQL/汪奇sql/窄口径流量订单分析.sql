use bnb_hive_db;
drop table if exists tmp_wq_bnb_page_list;
create table tmp_wq_bnb_page_list as
select home.d
  , home.clientcode
from
(select distinct d
  , clientcode
from bnb_pageview
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')home
left outer join
(select distinct clientcode
from bnb_pageview
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and pagecode ='600003563'
  and prepagecode ='600003560')list on home.clientcode = list.clientcode
where list.clientcode is not null;


drop table if exists tmp_wq_bnb_page_detail;
create table tmp_wq_bnb_page_detail as
select list.d
  , list.clientcode
from
(select distinct d
  , clientcode
from tmp_wq_bnb_page_list
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')list
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
where detail.clientcode is not null;


drop table if exists tmp_wq_bnb_page_fill;
create table tmp_wq_bnb_page_fill as
select detail.d
  , detail.clientcode
from
(select distinct d
  , clientcode
from tmp_wq_bnb_page_detail
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')detail
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
where fill.clientcode is not null;


drop table if exists tmp_wq_bnb_oi_submit;  -----提交单
create table tmp_wq_bnb_oi_submit as
select a.d
  ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
      , sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois
from
(select to_date(b1.createdtime) as d
    , a1.orderid
    , b1.applicationType
    , b1.terminalType
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
where a1.d='${zdt.format("yyyy-MM-dd")}'
  and to_date(b1.createdtime)='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and a1.saleamount>=20 and b1.sellerid=0) a
group by a.d
union all
select b.d
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select to_date(orderdate) as d
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where to_date(orderdate) ='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and d ='${zdt.format("yyyy-MM-dd")}')b
group by b.d;

drop table if exists tmp_wq_bnb_oi_pay;  -----支付单
create table tmp_wq_bnb_oi_pay as
select to_date(b1.createdtime) as d
    , a1.orderid
    , b1.applicationType
    , b1.terminalType
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
where to_date(b1.createdtime)='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}' and b1.sellerid=0
union all
select to_date(orderdate) as d
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where to_date(orderdate) ='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and d ='${zdt.format("yyyy-MM-dd")}' and orderstatus <> 'C'
  and (ordertype_ubt not in ('hotel_order') or ordertype_ubt is null);


drop table if exists tmp_wq_bnb_oi_gmv;
create table tmp_wq_bnb_oi_gmv as
select	to_date(c1.checkout) as d
    ,a1.orderid
    ,datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as days
    ,a1.saleamount
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
where to_date(c1.checkout)='${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (b1.applicationType is null or b1.applicationType = 10) ---App端售卖
  and a1.statusid in (1212,2212,2232,2233)
  and a1.saleamount>=20
  and a1.d='${zdt.format("yyyy-MM-dd")}'
  and b1.sellerid=0
union all
select  to_date(etd) as d,
  orderid ,
  ciiquantity as days,
  ciireceivable as saleamount
from dw_htlmaindb.FactHotelOrder_All_Inn
where to_date(etd) = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and d ='${zdt.format("yyyy-MM-dd")}' and orderstatus ='S'
  and (ordertype_ubt not in ('hotel_order') or ordertype_ubt is null);


use bnb_hive_db;
insert overwrite table bnb_product_out_kpi
partition(d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')
select home.uv as home
  ,list.uv as list
	,detail.uv as detail
	,fill.uv as fill
	,submit.num as submit
	,pay.num as pay
	,oi.num as checkoutNum
	,oi.days as checkoutDays
	,oi.gmv as checkoutGMV
from
(select d
    , count(distinct clientcode) as uv
  from bnb_pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  group by d) home
left outer join
(select d
  , count (distinct clientcode) as uv
  from tmp_wq_bnb_page_list
  group by d) list on home.d = list.d
join
(select d
  , count (distinct clientcode) as uv
  from tmp_wq_bnb_page_detail
  group by d) detail on home.d = detail.d
join
(select d
  , count (distinct clientcode) as uv
  from tmp_wq_bnb_page_fill
  group by d) fill on home.d = fill.d
join
(select d
  , count (distinct orderid) as num
  from tmp_wq_bnb_oi_submit
  group by d) submit on home.d = submit.d
join
(select d
  , count (distinct orderid) as num
  from tmp_wq_bnb_oi_pay
  group by d) pay on home.d = pay.d
join
(select d
  , count (distinct orderid) as num
  , sum(days) as days
  , sum(saleamount) as gmv
  from tmp_wq_bnb_oi_gmv
  group by d) oi on home.d = oi.d;



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
  , sum(if(listFlag=1 , num, 0)) as list
  , sum(if(detailFlag=1 , num, 0)) as detail
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
(select a.d
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
(select a.d
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






select home.uv as home
  ,list.uv as list
	,detail.uv as detail
	,fill.uv as fill
	,submit.num as submit
	,pay.num as pay
	,oi.num as checkoutNum
	,oi.days as checkoutDays
	,oi.gmv as checkoutGMV
from tmp_wq_bnb_page_home home
join tmp_wq_bnb_page_list list on home.d = list.d
join tmp_wq_bnb_page_detail detail on home.d = detail.d
join tmp_wq_bnb_page_fill fill on home.d = fill.d
join tmp_wq_bnb_oi_submit submit on home.d = submit.d
join tmp_wq_bnb_oi_pay pay on home.d = pay.d
join tmp_wq_bnb_oi_gmv oi on home.d = oi.d


use bnb_hive_db;
CREATE TABLE bnb_product_out_kpi(
	home string COMMENT '宫格首页UV'
	, list string COMMENT '宫格列表页UV'
	, detail string COMMENT '宫格详情页UV'
	, fill string COMMENT '宫格填写页UV'
	, submit string COMMENT '宫格提交订单量'
	, pay string COMMENT '宫格支付订单量'
	, checkoutNum string COMMENT '当日离店订单数'
	, checkoutDays string COMMENT '当日离店间夜数'
	, checkoutGMV string COMMENT '当日离店GMV'
)COMMENT '无线民宿订单表'
PARTITIONED BY (`d` string COMMENT 'date')



	,concat(cast(100*((pay.num)/home.uv) as decimal(5,2)),'%') as U2O
	,concat(cast(100*((list.uv)/home.uv) as decimal(5,2)),'%') as S2L
	,concat(cast(100*((detail.uv)/list.uv) as decimal(5,2)),'%') as L2D
	,concat(cast(100*((fill.uv)/detail.uv) as decimal(5,2)),'%') as D2B
	,concat(cast(100*((submit.num)/fill.uv) as decimal(5,2)),'%') as B2提交
	,concat(cast(100*((pay.num)/submit.num) as decimal(5,2)),'%') as 提交2有效

