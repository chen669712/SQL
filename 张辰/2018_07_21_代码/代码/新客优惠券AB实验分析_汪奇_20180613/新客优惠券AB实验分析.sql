
use bnb_hive_db;
set beginDay='2018-06-08';
set endDay='2018-06-11';
set calDay='2018-06-11';
set abCode='180531_bnbHybrid_syyhq';


-----------------------取分流人群-------------------------------------
drop table if exists tmp_wq_bnb_ticket_ab;
create table tmp_wq_bnb_ticket_ab as
select d
  , userid as uid
  , clientcode
  , abversion
  , min(starttime) as starttime
  , min(unix_timestamp(substring(starttime,1,19))) as unix_time
from dw_abtestdb.factabtestingserver
where expcode = ${hiveconf:abCode}
  and d>=${hiveconf:beginDay}
  and d<=${hiveconf:endDay}
group by d
  , userid
  , clientcode
  , abversion;



-----------------------过滤异常人群------------------------------------
drop table if exists tmp_wq_bnb_ticket_ab_cid;
create table tmp_wq_bnb_ticket_ab_cid as
select a.d
  , a.uid
  , a.clientcode
  , a.abversion
  , a.starttime
  , a.unix_time
from tmp_wq_bnb_ticket_ab a
left outer join
(select d
    , clientcode
    , count(distinct abversion) as abcnt
  from tmp_wq_bnb_ticket_ab
  group by d
         , clientcode
  having count(distinct abversion)>1) b on a.clientcode=b.clientcode and a.d=b.d
left outer join
(select clientcode
  from olap_abtestdb.abAbnormalUser
  where d>=${hiveconf:beginDay}
    and d<=${hiveconf:endDay}
    and clienttype = 'app'
    and distance>std
    and distance>1
 group by clientcode ) c on a.clientcode=c.clientcode
where b.clientcode is null
  and c.clientcode is null;



use bnb_hive_db;
select d
  , abversion
  , count(distinct clientcode) as uv
from tmp_wq_bnb_ticket_ab_cid
group by d
  , abversion



-----------------------看分流人群的下单数，使用券数------------------------------------
use bnb_hive_db;
set beginDay='2018-06-08';
set endDay='2018-06-11';
set oiCalDay='2018-06-12';
drop table if exists tmp_wq_bnb_ticket_oi;
create table tmp_wq_bnb_ticket_oi as
select btc.d
  , btc.abversion
  , oi.clientid
  , oi.orderid
  , oi.paychannel
  , oi.cardparentno
  , oi.payamount
from
tmp_wq_bnb_ticket_ab_cid btc
left outer join
(select oi.d
  , oi.clientid
  , oi.orderid
  , ticket.paychannel
  , ticket.cardparentno
  , ticket.payamount
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.clientid
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:oiCalDay}
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:oiCalDay}
where substring(b1.createdtime,0,10)>=${hiveconf:beginDay}
  and substring(b1.createdtime,0,10)<=${hiveconf:endDay}
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d=${hiveconf:oiCalDay} and b1.sellerid=0) oi
inner join
(select distinct substring(createdtime,0,10) as d
  , orderid
  , paychannel
  , cardparentno
  , payamount
from ods_htl_bnborderdb.order_pay
where d = ${hiveconf:oiCalDay}
  and substring(createdtime,0,10)>=${hiveconf:beginDay}
  and substring(createdtime,0,10)<=${hiveconf:endDay}
  and paychannel in(11, 10)
  and (cardparentno is null or cardparentno in('73291', '73292'))
  and reverseflag=1) ticket on ticket.d = oi.d and ticket.orderid=oi.orderid) oi on btc.d=oi.d and btc.clientcode = oi.clientid




use bnb_hive_db;
select d
  , abversion
  , paychannel
  , cardparentno
  , count(distinct orderid) as ois
  , count(distinct clientid) as uv
  , sum(payamount) as amount
from tmp_wq_bnb_ticket_oi
where payamount is not null
group by d
  , abversion
  , paychannel
  , cardparentno




-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
    -----------------------------下面忽略-------------------------------
    -----------------------------下面忽略-------------------------------
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------
----------------------------查看分流人群的下单数和用户数--------------------------------
use bnb_hive_db;
set beginDay='2018-06-08';
set endDay='2018-06-11';
set oiCalDay='2018-06-12';

select oi.*
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.clientid
    , b1.saleschannel
    , b1.visitsource
    , b1.paystatusid
    , b1.createdtime
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:oiCalDay}
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:oiCalDay}
where substring(b1.createdtime,0,10)>=${hiveconf:beginDay}
  and substring(b1.createdtime,0,10)<=${hiveconf:endDay}
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and b1.terminalType=10
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d=${hiveconf:oiCalDay} and b1.sellerid=0) oi
left outer join
(select btc.d
	, oi.orderid
from
(select d
 	, cid
from bnb_user_distribution
where d>= ${hiveconf:beginDay})btc
left outer join
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.clientid
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:oiCalDay}
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:oiCalDay}
where substring(b1.createdtime,0,10)>=${hiveconf:beginDay}
  and substring(b1.createdtime,0,10)<=${hiveconf:endDay}
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and b1.terminalType=10
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d=${hiveconf:oiCalDay} and b1.sellerid=0) oi on btc.d = oi.d and btc.cid=oi.clientid
where oi.clientid is not null) bdoi on bdoi.d = oi.d and bdoi.orderid = oi.orderid
where bdoi.orderid is null
limit 100


set beginDay='2018-06-08';
set endDay='2018-06-11';
set oiCalDay='2018-06-12';
select b1.*
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=${hiveconf:oiCalDay}
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=${hiveconf:oiCalDay}
where substring(b1.createdtime,0,10)>=${hiveconf:beginDay}
  and substring(b1.createdtime,0,10)<=${hiveconf:endDay}
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d=${hiveconf:oiCalDay} and b1.sellerid=0
  and a1.orderid in('6415700669', '6415700841', '6415846102', '6415839282', '6415802333', '6415773471', '6415689774', '6415702731' )







-----------------------查看领券的用户-----------------------------------
drop table if exists tmp_wq_bnb_ticket_ab_info;
create table tmp_wq_bnb_ticket_ab_info as
select tac.*
  , fcb.promotionid
  , case when tac.unix_time>=fcb.claimtime then 'y' end as isvalid
from tmp_wq_bnb_ticket_ab_cid tac
left outer join
(select uid
  , promotionid
  , unix_timestamp(substring(claimtime,1,19)) as claimtime
from Dw_pubsharedb.factcoupon_BNB
where promotionid in ('73291', '73292')
  and uid is not null) fcb on lower(tac.uid) = lower(fcb.uid)



use bnb_hive_db;
select d
  , abversion
  , promotionid
  , count(distinct clientcode) as uv
from tmp_wq_bnb_ticket_ab_info
group by d
  , isvalid
  , promotionid


-------------------------------------------------------------
---------只看从首页进入民宿的用户
-------------------------------------------------------------
use bnb_hive_db;
set beginDay='2018-06-08';
drop table if exists tmp_wq_bnb_ticket_home_cid;
create table tmp_wq_bnb_ticket_home_cid as
select tac.*
from
tmp_wq_bnb_ticket_ab_cid tac
left outer join
(select *
from bnb_user_distribution
where d>= ${hiveconf:beginDay}) bud on tac.d = bud.d and tac.clientcode = bud.cid
where bud.cid is not null;