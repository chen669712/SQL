--2017年10月1日首单用户
use bnb_hive_db;
drop table if exists tmp_zc_ordertime;
create table tmp_zc_ordertime as
select uid from 
  (select uid,ordertime,row_number() over(partition by uid order by ordertime) rn from 
    (select uid,
        substr(ordertime,1,10) as ordertime 
    from bnb_hive_db.bnb_orderinfo 
    where d = '2018-06-20'
    and agent = 0) a 
  ) b
where rn = 1
and ordertime = '2017-10-01'
group by uid

--10月1日首单用户汇总
select count(distinct uid) from bnb_hive_db.tmp_zc_ordertime

--2017年10月2日至2018年4月1日之间仍旧下单的用户
select count(distinct a.uid) from bnb_hive_db.tmp_zc_ordertime a
join (select uid from bnb_hive_db.bnb_orderinfo
     	where d = '2018-06-20'
      	and substr(ordertime,1,10) >= '2017-11-02'
		and substr(ordertime,1,10) <= '2018-05-01'
      group by uid
     ) b
on a.uid = b.uid