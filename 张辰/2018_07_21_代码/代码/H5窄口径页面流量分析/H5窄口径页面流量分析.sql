--建表bnb_data_h5_user_distribution
use bnb_hive_db;
CREATE TABLE bnb_data_h5_user_distribution(
  vid string COMMENT '用户的vid'
  , uid string COMMENT '用户的uid'
	, homeFlag string COMMENT '宫格首页UV'
	, listFlag string COMMENT '宫格列表页UV'
	, detailFlag string COMMENT '宫格详情页UV'
	, fillFlag string COMMENT '宫格填写页UV'
)COMMENT '无线民宿订单表'
PARTITIONED BY (`d` string COMMENT 'date')


use bnb_hive_db;
insert overwrite table bnb_data_h5_user_distribution
partition(d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')
select homevid
  , null as uid
  , if(homevid is null, 0, 1) as homeFlag
  , if(listvid is null, 0, 1) as listFlag
  , if(detailvid is null, 0, 1) as detailFlag
  , if(fillvid is null, 0, 1) as fillFlag
from
(select distinct home.vid as homevid ,list.vid as listvid,detail.vid as detailvid,fill.vid as fillvid  from
(select distinct vid,sid
from dw_ubtdb.pageview
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
and originalpageid ='600003543')home
left outer join
(select distinct vid,sid
from dw_ubtdb.pageview
where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
  and originalpageid ='600003546')list on home.vid = list.vid and home.sid = list.sid
left outer join
(select a.vid,a.sid
  from
  (select distinct vid,sid
  from dw_ubtdb.pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and originalpageid in ('600003547'))a)detail on list.vid = detail.vid and list.sid = detail.sid
left outer join
(select a.vid,a.sid
 from
 (select distinct vid,sid
  from dw_ubtdb.pageview
  where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
    and originalpageid in ('600003553'))a)fill on detail.vid = fill.vid and detail.sid = fill.sid
) a


--H5窄口径页面流量分析
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
    , count(distinct vid) as num
 from bnb_hive_db.bnb_data_h5_user_distribution
 where d >= "$effectdate('yyyy-MM-dd',-16)"
    and d <= "$effectdate('yyyy-MM-dd',-1)"
 group by d, homeFlag, listFlag, detailFlag, fillFlag)a
 group by a.d) uv
join
(select substring(b1.createdtime, 0, 10) as d     
, count(distinct a1.orderid) as ois 
from ods_htl_bnborderdb.order_item a1 
left join ods_htl_bnborderdb.order_header_v2 b1 
on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)" 
left join ods_htl_bnborderdb.order_item_space c1 
on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)" 
where substring(b1.createdtime,0,10)>= "$effectdate('yyyy-MM-dd',-16)" 
  and substring(b1.createdtime,0,10)<= "$effectdate('yyyy-MM-dd',-1)"  
and b1.terminalType=20   
and a1.saleamount>=20 
and a1.d="$effectdate('yyyy-MM-dd',0)" 
and b1.sellerid=0 
group by substring(b1.createdtime, 0, 10)
) submit on uv.d = submit.d
join
(select a.d                
  , sum (a.ois) as ois
from
(select substring(b1.createdtime, 0, 10) as d     
, count(distinct a1.orderid) as ois 
from ods_htl_bnborderdb.order_item a1 
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)" 
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"  
where substring(b1.createdtime,0,10) >= "$effectdate('yyyy-MM-dd',-16)"
  and substring(b1.createdtime,0,10) <= "$effectdate('yyyy-MM-dd',-1)"        
and b1.terminalType=20   and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')   
and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)"  and b1.sellerid=0 
group by substring(b1.createdtime, 0, 10)
)a
group by a.d) pay on uv.d = pay.d