use bnb_hive_db;
drop table if exists tmp_zc_hotel_order;
create table tmp_zc_hotel_order
select substr(createdtime,1,10) as createdtime,a.orderid,c.spaceid,a.allianceid,a.alliancesid from 
(select * from ods_htl_bnborderdb.order_header_v2
where d = '2018-07-26'
and substr(createdtime,1,10) >= '2018-07-19'
and substr(createdtime,1,10) <= '2018-07-25'
and saleschannel = 2) a
inner join 
(select * from ods_htl_bnborderdb.order_item
where d = '2018-07-26') b
on a.orderid = b.orderid
inner join
(select* from ods_htl_groupwormholedb.bnb_space_product where d= '2018-07-26') c
on b.productid = c.productid
group by substr(createdtime,1,10),a.orderid,c.spaceid,a.allianceid,a.alliancesid

select * from bnb_hive_db.tmp_zc_hotel_order a
inner join
(select * from ods_htl_groupwormholedb.bnb_space where d = '2018-07-26') b
on a.spaceid = b.spaceid