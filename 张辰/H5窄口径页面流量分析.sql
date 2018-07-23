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
select count(distinct homevid) as homeuv
  , count(distinct listvid) as listuv
  , count(distinct detailvid) as detailuv
  , count(distinct fillvid) as filluv
from
(select distinct d,home.vid as homevid ,list.vid as listvid,detail.vid as detailvid,fill.vid as fillvid  from
(select distinct d,vid,sid
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
group by a.d

--H5窄口径页面流量分析
select uv.d as `日期`
  ,uv.homeuv as `首页UV`
  ,pay.ois as `支付订单`
  ,concat(cast(100*(pay.ois/uv.homeuv) as decimal(5,2)),'%') as `U2O`
  ,concat(cast(100*(uv.listuv/uv.homeuv) as decimal(5,2)),'%') as `S2L`
  ,concat(cast(100*(uv.detailuv/uv.listuv) as decimal(5,2)),'%') as `L2D`
  ,concat(cast(100*(uv.filluv/uv.detailuv) as decimal(5,2)),'%') as `D2B`
  ,concat(cast(100*(submit.ois/uv.filluv) as decimal(5,2)),'%') as `B2提交`
  ,concat(cast(100*(pay.ois/submit.ois) as decimal(5,2)),'%') as `提交2有效`
from
( select d
    ,homeuv
    ,listuv
    ,detailuv
    ,filluv 
  from bnb_hive_db.bnb_data_h5_user_distribution 
  where d >= "$effectdate('yyyy-MM-dd',-16)"
    and d <= "$effectdate('yyyy-MM-dd',-1)"
) uv
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