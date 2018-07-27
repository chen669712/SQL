-----------------------------------新客180天生命周期价值---------------------------------------
--2017年10月1日至10月31日首单用户，下单时间及消费额度
use bnb_hive_db;
drop table if exists tmp_zc_newusers_value;
create table  tmp_zc_newusers_value as
select substr(ordertime,1,10) as ordertime,uid,orderamount from 	 
  (select uid
	 ,orderamount,ordertime
	 ,row_number() over(partition by uid order by ordertime) as rn  
	from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
      and agent = 0) a	
	where rn = 1
	  and substr(ordertime,1,10) >= '2017-10-01'
	  and substr(ordertime,1,10) <= '2017-10-31'
	group by substr(ordertime,1,10),uid,orderamount

--2018年10月1日至今下单用户，下单时间及消费额度
use bnb_hive_db;
drop table if exists tmp_zc_repurchase_value;
create table  tmp_zc_repurchase_value as
select uid
    ,substr(ordertime,1,10) as ordertime 
	,orderamount 
from bnb_hive_db.bnb_orderinfo
where d = '2018-06-21'
  and agent = 0
  and substr(ordertime,1,10) >= '2017-10-01'

--新客180天生命周期价值
select a.ordertime,sum(b.orderamount)/count(distinct b.uid) from bnb_hive_db.tmp_zc_newusers_value a
inner join (select uid,ordertime,orderamount from bnb_hive_db.tmp_zc_repurchase_value) b
on a.uid = b.uid
where datediff(b.ordertime,a.ordertime) <= 180
group by a.ordertime