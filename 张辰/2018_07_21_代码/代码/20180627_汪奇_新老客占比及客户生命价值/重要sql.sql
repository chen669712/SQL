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
            and agent = 0) a
		) b
	where rn = 1
	and substr(ordertime,1,10) >= '2017-10-01'
	and substr(ordertime,1,10) <= '2017-10-31'
	group by ordertime
-------------------------------下单老客-------------------------------------------
--2017.10.01-2017.10.31下单的老客
	select a.ordertime,count(a.uid) from
--2017.10.01下单的客户 
	(select substr(ordertime,1,10) ordertime,uid from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
	and substr(ordertime,1,10) = '2017-10-07'
--	and substr(ordertime,1,10) <= '2017-10-31'
	and agent = 0
	group by substr(ordertime,1,10),uid) a 
	inner join
--2017.10.01之前下单的客户 
	(select uid from bnb_hive_db.bnb_orderinfo
	where d = '2018-06-21'
	and substr(ordertime,1,10) < '2017-10-07'
	and agent = 0
	group by uid) b
	on a.uid = b.uid 
	group by a.ordertime

-------------------------------新客30日，90日，180天复购人数-------------------------
--新客180天复购人数（最终版）
--2018年10月1日至10月31日首单用户
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

--2018年10月2日至11月01日+180日下单用户
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

------------------------------------新客生命180天周期价值---------------------------------------
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
	group by a.ordertime











