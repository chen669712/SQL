	--订单产量前20的城市
	use bnb_hive_db;
	drop table if exists tmp_zc_top20city;
	create table tmp_zc_top20city as
	select d1.cityname,c1.cityid,count(a1.orderid) cn_order
	from ods_htl_bnborderdb.order_item a1
	left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='2018-06-11'
	left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-06-11'
	left join ods_htl_groupwormholedb.bnb_city d1 on c1.cityid = d1.cityid and d1.d = '2018-06-11'
	where a1.d='2018-06-11'
	  and to_date(b1.createdtime) >= '2018-01-01'
	  and to_date(b1.createdtime) <= '2018-06-11'
	  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
	  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
	  and a1.saleamount>=20 and b1.sellerid=0 and a1.vendorid = 115 
	group by d1.cityname,c1.cityid
	order by cn_order desc
	limit 20

	--各城市前100列表页面
	use bnb_hive_db;
	drop table if exists tmp_zc_recommend;
	create table tmp_zc_recommend as
	select t.cityid,t.spaceid,t.rn from (
		select b.cityid,a.spaceid,row_number() over(partition by cityid order by recommendscore desc) as rn from 
			(select spaceid
					, recommendscore
			from ods_htl_groupwormholedb.bnb_space_sortscore
			where isHidden = 'F'
			  and d = '2018-06-11'
			group by spaceid,recommendscore) a 
			left outer join (
				select spaceid
						, cityid
				from ods_htl_groupwormholedb.bnb_space_zone
				where d = '2018-06-11'
				group by spaceid,cityid) b
			on a.spaceid = b.spaceid
			left outer join (
				select spaceid from ods_htl_groupwormholedb.bnb_space
				where statusid = 2
				and d = '2018-06-11'
				group by spaceid) c
			on a.spaceid = c.spaceid
		) t
	where t.rn >= 1
	and t.rn <= 100

	--订单产量前20的城市前100列表页面对应的城市，途家roomid和携程spaceid
		select a.cityname,c.spaceid,c.productid 
		from bnb_hive_db.tmp_zc_top20city a
		left outer join bnb_hive_db.tmp_zc_recommend b
		on a.cityid = b.cityid
		left outer join ods_htl_groupwormholedb.bnb_space_source c
		on b.spaceid = c.spaceid and c.d = '2018-06-11'





