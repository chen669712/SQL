select * from dw_abtestdb.factabtestingserver

AB实验查看数据
- CR：人均订单数量
- GP：人均订单利润
- GP-C：人均订单利润-人均订单优惠（包含不判断是否使用的优惠券和消费券）
- GMB：人均订单金额
- Quantity：人均订单间夜数
- PV：人均访问页面数

汪奇:
Dw_pubsharedb.factpromotion_BNB
Dw_pubsharedb.factcoupon_BNB
Dw_pubsharedb.FactCouponOrdDetail_BNB
汪奇:
这三张表是关于优惠券的

--任务
新首页：180511_bnbHybrid_msxsy
- 5月26~5月29 分流 20:10:10，查看B版本9.2 vs CD版本9.0
- 5月31~6月03 分流20:10:10，查看B版本800-80 vs CD版本无800-80
- 6月8日~6月10 分流50（50:25:25）:25:25，两个实验（180531_bnbHybrid_syyhq） 8.8vs8.5vs9.0

AB的实验分流表dw_abtestdb.factabtestingserver

--订单相关
select oi.d
  , 0 as type
  , sum(if(ticket.paychannel=10, 1, 0)) as oinum
  , sum(if(ticket.paychannel=11, 1, 0)) as ticketnum
  , sum(if(ticket.paychannel=10, ticket.payamount, 0)) as oiamount
  , sum(if(ticket.paychannel=11, ticket.payamount, 0)) as ticketamount
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-06-18'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-06-18'
where substring(b1.createdtime,0,10)>='2018-06-10'
  and substring(b1.createdtime,0,10)<'2018-06-18'
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-06-18' and b1.sellerid=0) oi
inner join
(select distinct substring(createdtime,0,10) as d
  , orderid
  , paychannel
  , payamount
from ods_htl_bnborderdb.order_pay
where d = '2018-06-18'
  and substring(createdtime,0,10)>='2018-06-10'
  and substring(createdtime,0,10)<'2018-06-18'
  and paychannel in(10, 11)
  and reverseflag=1) ticket on ticket.d = oi.d and ticket.orderid=oi.orderid
group by oi.d
paychannel =11 优惠券

--获取UV
use bnb_hive_db;
set beginDay='2018-06-05';
set endDay='2018-06-07';
set calDay='2018-06-07';
set abCode='180511_bnbHybrid_msxsy';

drop table if exists tmp_wq_bnb_home_ab;
create table tmp_wq_bnb_home_ab as
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

--过滤UV
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