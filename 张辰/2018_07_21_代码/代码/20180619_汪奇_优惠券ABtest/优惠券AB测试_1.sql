use bnb_hive_db;
set beginDay='2018-05-26';
set endDay='2018-05-30';
set calDay='2018-05-30';
set abCode='180511_bnbHybrid_msxsy';

--分流人群
drop table if exists tmp_zc_bnb_ticket_ab_01;
create table tmp_zc_bnb_ticket_ab_01 as
select d
  , clientcode
  , abversion
  , min(starttime) as starttime
  , min(unix_timestamp(substring(starttime,1,19))) as unix_time
from dw_abtestdb.factabtestingserver
where expcode = ${hiveconf:abCode}
  and d>=${hiveconf:beginDay}
  and d<=${hiveconf:endDay}
group by d
  , clientcode
  , abversion;

--过滤人群
drop table if exists tmp_zc_bnb_ticket_ab_cid_01;
create table tmp_zc_bnb_ticket_ab_cid_01 as
select a.d
  , a.uid
  , a.clientcode
  , a.abversion
  , a.starttime
  , a.unix_time
from tmp_zc_bnb_ticket_ab_01 a
left outer join
(select d
    , clientcode
    , count(distinct abversion) as abcnt
  from tmp_zc_bnb_ticket_ab_01
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

--人群优惠券使用情况
use bnb_hive_db;
set beginDay='2018-05-26';
set endDay='2018-05-30';
set calDay='2018-05-30';

drop table if exists tmp_zc_bnb_ticket_ab_ticketdate_01;
create table tmp_zc_bnb_ticket_ab_ticketdate_01 as
select oi.d
  , oi.clientid
  , sum(if(ticket.paychannel=10, 1, 0)) as oinum
  , sum(if(ticket.paychannel=11, 1, 0)) as ticketnum
  , sum(if(ticket.paychannel=10, ticket.payamount, 0)) as oiamount
  , sum(if(ticket.paychannel=11, ticket.payamount, 0)) as ticketamount
from
(select distinct substring(b1.createdtime, 0, 10) as d
  , b1.clientid
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-06-18'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-06-18'
where substring(b1.createdtime,0,10)>=${hiveconf:beginDay}
  and substring(b1.createdtime,0,10)<${hiveconf:endDay}
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-18' and b1.sellerid=0) oi
inner join
(select distinct substring(createdtime,0,10) as d
  , orderid
  , paychannel
  , payamount
from ods_htl_bnborderdb.order_pay
where d = '2018-06-18'
  and substring(createdtime,0,10)>=${hiveconf:beginDay}
  and substring(createdtime,0,10)<${hiveconf:endDay}
  and paychannel in (10,11)
  and reverseflag=1) ticket on ticket.d = oi.d and ticket.orderid=oi.orderid
group by oi.d,oi.clientid

--B版本9.2 VS CD版本9.0,需要过滤出从主页进入的UV
use bnb_hive_db;
set beginDay='2018-05-26';
set endDay='2018-05-30';
set calDay='2018-05-30';
set abCode='180511_bnbHybrid_msxsy';

select a.d,a.abversion,count(distinct home.clientcode),sum(b.oinum),sum(b.ticketnum),sum(b.oiamount),sum(b.ticketamount) from bnb_hive_db.tmp_zc_bnb_ticket_ab_cid_01 a
left outer join bnb_hive_db.tmp_zc_bnb_ticket_ab_ticketdate_01 b
on a.clientcode = b.clientid and a.d = b.d
left outer join
--从宫格首页进入的用户
(select d,clientcode 
 from bnb_hive_db.bnb_pageview 
 where d >= ${hiveconf:beginDay}
 and d <= ${hiveconf:endDay}
 group by d,clientcode) home 
on a.d=home.d and a.clientcode=home.clientcode
group by a.d,a.abversion