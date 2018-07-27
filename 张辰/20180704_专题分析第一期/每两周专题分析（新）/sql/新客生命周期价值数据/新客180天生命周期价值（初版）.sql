--将用户按照创建订单时间降序排序,获取用户最早的下单时间,并且位于2017年10月1日至11月30日之间
use bnb_hive_db;
drop table if exists tmp_zc_uid_createdtime;
create table tmp_zc_uid_createdtime as 
select uid,orderid,createdtime from 
(select uid,orderid,min(createdtime) over(partition by uid order by createdtime desc) as createdtime from ods_htl_bnborderdb.order_header_v2 where d = '2018-06-20') a
where createdtime >= '2017-10-01'
and createdtime <= '2018-11-30'

--计算首单用户在下单时及接下来半年内的累积消费
select sum(b.payamount)/sum(distinct a.uid) from 
(select uid,orderid,createdtime from tmp_zc_uid_createdtime) a 
join (select orderid,payamount ods_htl_bnborderdb.order_pay
      where reverseflag = 1
      and payamounttype = 1
      and paychannel = 10
      and statusid = 12
      and substr(createdtime,1,10) >= '2017-10-01'
      and substr(createdtime,1,10) <= '2018-05-31') b
on a.orderid = b.orderid