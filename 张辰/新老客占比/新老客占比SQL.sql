-------------------------------下单用户------------------------------------------
--2017.10.01 - 2017.10.31下单的用户
select substr(ordertime,1,10),count(distinct uid) from bnb_hive_db.bnb_orderinfo
where d = '2018-06-21'
	and substr(ordertime,1,10) >= '2017-10-01'
	and substr(ordertime,1,10) <= '2017-10-31'
	and agent = 0
group by substr(ordertime,1,10)

-------------------------------下单新客------------------------------------------
--2017.10.01-2017.10.31下单的新客	
select ordertime,count(distinct uid) from 
	(select uid,ordertime,row_number() over(partition by uid order by ordertime) as rn from 
		(select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
		where d = '2018-06-21'
        	and agent = 0
        ) a
	) b
where rn = 1
	and substr(ordertime,1,10) >= '2017-10-01'
	and substr(ordertime,1,10) <= '2017-10-31'
group by ordertime
-------------------------------下单老客（全量跑）---------------------------------------
--2018-07-24 新代码
select users.d,count(distinct users.uid) from 
	(select substr(ordertime,1,10) as d,uid from bnb_hive_db.bnb_orderinfo
	where d = '2018-07-23'
		and substr(ordertime,1,10) >= '2017-10-01'
		and substr(ordertime,1,10) <= '2017-10-31'
		and agent = 0
	group by substr(ordertime,1,10),uid) users
	left outer join
	(select ordertime as d,uid from 
		(select uid,ordertime,row_number() over(partition by uid order by ordertime) as rn from 
			(select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
			where d = '2018-07-23'
            and agent = 0) a
		) b
	where rn = 1
	and substr(ordertime,1,10) >= '2017-10-01'
	and substr(ordertime,1,10) <= '2017-10-31'
	group by ordertime,uid) newusers
	on users.uid = newusers.uid and users.d = newusers.d 
where newusers.uid is null
group by users.d

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

--2018年10月1日至11月01日+180日下单用户，下单时间及消费额度
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
  and substr(ordertime,1,10) <= date_add('2017-11-01',180)

--复购价值
select a.ordertime,sum(b.orderamount)/count(distinct uid) from bnb_hive_db.tmp_zc_newusers_value a
inner join (select uid,ordertime,orderamount from bnb_hive_db.tmp_zc_repurchase_value) b
on a.uid = b.uid
where datediff(b.ordertime,a.ordertime) <= 180
group by a.ordertime
-------------------------------下单老客（每天跑）-------------------------------------------
--2017.10.01-2017.10.31下单的老客
	select a.ordertime,count(a.uid) from
--2017.10.01下单的客户 
	(select substr(ordertime,1,10) ordertime,uid from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
		and substr(ordertime,1,10) = '2017-10-01'
--		and substr(ordertime,1,10) <= '2017-10-31'
		and agent = 0
	group by substr(ordertime,1,10),uid
	) a 
	inner join
--2017.10.01之前下单的客户 
	(select uid from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
		and substr(ordertime,1,10) < '2017-10-01'
		and agent = 0
	group by uid
	) b
	on a.uid = b.uid 
	group by a.ordertime

-------------------------------新客30日，90日，180天复购人数-------------------------
--新客180天复购人数
--2018年10月1日至10月31日首单用户，下单时间
	use bnb_hive_db;
	drop table if exists tmp_zc_newusers;
	create table  tmp_zc_newusers as
	select ordertime,uid from 
		(select uid,ordertime,row_number() over(partition by uid order by ordertime) as rn from 
			(select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
			where d = '2018-06-21'
            and agent = 0) a
		) b
	where rn = 1
	and substr(ordertime,1,10) >= '2017-10-01'
	and substr(ordertime,1,10) <= '2017-10-31'
	group by ordertime,uid

--2018年10月2日至11月01日+180日下单用户，下单时间
	use bnb_hive_db;
	drop table if exists tmp_zc_buyusers;
	create table  tmp_zc_buyusers as
	select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
	and agent = 0
	and substr(ordertime,1,10) >= '2017-10-02'
	and substr(ordertime,1,10) <= date_add('2017-11-01',180)
	group by substr(ordertime,1,10),uid

--180日复购用户
	select a.ordertime,count(*) from 
	bnb_hive_db.tmp_zc_newusers a 
	inner join bnb_hive_db.tmp_zc_buyusers b
	on a.uid = b.uid
	where 
	datediff(b.ordertime,a.ordertime) <= 180
	and datediff(b.ordertime,a.ordertime) > 0 
	group by a.ordertime
--------------------------------新客30日，90日，180天复购人数（每行1跑）--------------------------
--新客180天复购人数
--2018年10月1日首单用户，下单时间
	use bnb_hive_db;
	drop table if exists tmp_zc_newusers;
	create table  tmp_zc_newusers as
	select ordertime,uid from 
		(select uid,ordertime,row_number() over(partition by uid order by ordertime) as rn from 
			(select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
			where d = '2018-06-21'
              and agent = 0) a
		) b
	where rn = 1
	  and substr(ordertime,1,10) = '2017-10-01'
--	and substr(ordertime,1,10) <= '2017-10-31'
	group by ordertime,uid

--2018年10月2日至10月02日+180日下单用户，下单时间
	use bnb_hive_db;
	drop table if exists tmp_zc_repurchase_users;
	create table  tmp_zc_repurchase_users as
	select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
	  where d = '2018-06-21'
	  and agent = 0
	  and substr(ordertime,1,10) >= '2017-10-02'
	  and substr(ordertime,1,10) <= date_add('2017-10-02',180)
	group by substr(ordertime,1,10),uid

--180日复购用户
	select a.ordertime,count(*) from 
	  bnb_hive_db.tmp_zc_newusers a 
	inner join bnb_hive_db.tmp_zc_repurchase_users b
	on a.uid = b.uid
	where 
	  datediff(b.ordertime,a.ordertime) <= 180
	  and datediff(b.ordertime,a.ordertime) > 0 
	group by a.ordertime

/*------------------------------------新客生命180天周期价值---------------------------------------
--2018年10月1日至10月31日首单用户，及消费额度
	use bnb_hive_db;
	drop table if exists tmp_zc_newusers;
	create table  tmp_zc_newusers as
	select ordertime,uid,orderamount from 
		(select uid,ordertime,orderamount,row_number() over(partition by uid order by ordertime) as rn from 
			(select uid,orderamount,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
			where d = '2018-06-21'
            and agent = 0) a
		) b
	where rn = 1
	and substr(ordertime,1,10) >= '2017-10-01'
	and substr(ordertime,1,10) <= '2017-10-31'
	group by ordertime,uid,orderamount

--2018年10月2日至11月01日+180日下单用户，及消费额度
	use bnb_hive_db;
	drop table if exists tmp_zc_buyusers;
	create table  tmp_zc_buyusers as
	select uid,substr(ordertime,1,10),orderamount as ordertime from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
	and agent = 0
	and substr(ordertime,1,10) >= '2017-10-02'
	and substr(ordertime,1,10) <= date_add('2017-11-01',180)
	group by substr(ordertime,1,10),uid

--180日复购用户人均累计消费
	select a.ordertime,sum(a.orderamount)/count(distinct a.uid) from 
	bnb_hive_db.tmp_zc_newusers a 
	inner join bnb_hive_db.tmp_zc_buyusers b
	on a.uid = b.uid
	where 
	datediff(b.ordertime,a.ordertime) <= 180
--	and datediff(b.ordertime,a.ordertime) > 0 
	group by a.ordertime*/


------------------------------------新客180天生命周期价值(每天1跑)---------------------------------------
--2018年10月1日首单用户，下单时间及消费额度
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
	  and substr(ordertime,1,10) = '2017-10-01'
--	and substr(ordertime,1,10) <= '2017-10-31'
	group by substr(ordertime,1,10),uid,orderamount

--2018年10月1日至10月01日+180日下单用户，下单时间及消费额度
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
  and substr(ordertime,1,10) <= date_add('2017-10-01',180)

--180日复购用户人均累计消费
select a.ordertime
	,sum(a.orderamount)/count(distinct a.uid) 
from bnb_hive_db.tmp_zc_newusers a 
inner join bnb_hive_db.tmp_zc_repurchase_value b
on a.uid = b.uid
where 
datediff(b.ordertime,a.ordertime) <= 180
--	and datediff(b.ordertime,a.ordertime) > 0 
group by a.ordertime

--180日复购用户人均累计消费
select sum(a.orderamount)/count(distinct a.uid) from
(select uid,orderamount,ordertime from bnb_hive_db.tmp_zc_repurchase_value) a
left semi join
(select uid from bnb_hive_db.tmp_zc_newusers_value) b
on a.uid = b.uid

-----------------------------------新客180天生命周期价值(1次全量跑)---------------------------------------
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

--180日复购用户人均累计消费
select a.ordertime
	,sum(a.orderamount)/count(distinct a.uid) 
from bnb_hive_db.tmp_zc_newusers a 
inner join bnb_hive_db.tmp_zc_repurchase_value b
on a.uid = b.uid
where 
datediff(b.ordertime,a.ordertime) <= 180
--	and datediff(b.ordertime,a.ordertime) > 0 
group by a.ordertime

--180日复购用户人均累计消费
select sum(a.orderamount)/count(distinct a.uid) from
(select uid,orderamount,ordertime from bnb_hive_db.tmp_zc_repurchase_value) a
left semi join
(select uid from bnb_hive_db.tmp_zc_newusers_value) b
on a.uid = b.uid

-------------------------------老客30日，90日，180天复购人数-------------------------
--老客180天复购人数
--2018年10月1日至10月31日下单老客，下单时间
use bnb_hive_db;
drop table if exists tmp_zc_oldusers;
create table tmp_zc_oldusers as
select users.d,users.uid from 
	(select substr(ordertime,1,10) as d,uid from bnb_hive_db.bnb_orderinfo
	where d = '2018-07-23'
		and substr(ordertime,1,10) >= '2017-10-01'
		and substr(ordertime,1,10) <= '2017-10-31'
		and agent = 0
	group by substr(ordertime,1,10),uid) users
	left outer join
	(select ordertime as d,uid from 
		(select uid,ordertime,row_number() over(partition by uid order by ordertime) as rn from 
			(select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
			where d = '2018-07-23'
            and agent = 0) a
		) b
	where rn = 1
	and substr(ordertime,1,10) >= '2017-10-01'
	and substr(ordertime,1,10) <= '2017-10-31'
	group by ordertime,uid) newusers
	on users.uid = newusers.uid and users.d = newusers.d 
where newusers.uid is null
group by users.d,users.uid


--2018年10月2日至11月01日+180日下单用户，下单时间
	use bnb_hive_db;
	drop table if exists tmp_zc_buyusers;
	create table  tmp_zc_buyusers as
	select uid,substr(ordertime,1,10) as ordertime from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
	and agent = 0
	and substr(ordertime,1,10) >= '2017-10-02'
	and substr(ordertime,1,10) <= date_add('2017-11-01',180)
	group by substr(ordertime,1,10),uid

--180日复购用户
	select a.ordertime,count(*) from 
	bnb_hive_db.tmp_zc_oldusers a 
	inner join bnb_hive_db.tmp_zc_buyusers b
	on a.uid = b.uid
	where 
	datediff(b.ordertime,a.ordertime) <= 180
	and datediff(b.ordertime,a.ordertime) > 0 
	group by a.ordertime







