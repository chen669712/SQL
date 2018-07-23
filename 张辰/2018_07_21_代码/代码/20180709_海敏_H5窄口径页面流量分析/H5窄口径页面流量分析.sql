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