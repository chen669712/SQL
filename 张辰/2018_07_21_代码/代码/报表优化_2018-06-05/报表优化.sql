--单日，7日，30日UV
use bnb_hive_db;
drop table if exists tmp_zc_uv;
create table tmp_zc_uv as
select a1.cityid,a1.dau,a2.wau,a3.mau from (
      select
          get_json_object(value, '$.cityid')              as cityid,
          count(distinct newvalue.data['env_clientcode']) as dau
      from dw_mobdb.factmbtracelog_hybrid
      where d = date_add('${zdt.format("yyyy-MM-dd")}',-1) and KEY in ("100641","bnb_inn_list_app_basic")
      group by get_json_object( VALUE, '$.cityid')	
    ) a1
    join (
          select
          get_json_object(value, '$.cityid')              as cityid,
          count(DISTINCT newvalue.data['env_clientcode']) as wau
        from dw_mobdb.factmbtracelog_hybrid
        where d < '${zdt.format("yyyy-MM-dd")}' and d >= date_add('${zdt.format("yyyy-MM-dd")}',-7) and KEY in ("100641","bnb_inn_list_app_basic")
        group by get_json_object( VALUE, '$.cityid')  
   ) a2
  on a1.cityid = a2.cityid
  join (
  		select
         get_json_object(value, '$.cityid')              as cityid,
         count(DISTINCT newvalue.data['env_clientcode']) as mau
      from dw_mobdb.factmbtracelog_hybrid
      where d < '${zdt.format("yyyy-MM-dd")}' and d >= date_add('${zdt.format("yyyy-MM-dd")}',-30) 
    	and KEY in ("100641","bnb_inn_list_app_basic")
      group by get_json_object( VALUE, '$.cityid')
  ) a3
  on a1.cityid = a3.cityid

select * from bnb_hive_db.tmp_zc_uv

--单日，7日，30日民宿订单量
use bnb_hive_db; 
drop table if exists tmp_zc_homestay_order;
create table tmp_zc_homestay_order AS


(select a.cityid
  ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
      , sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois_past_day
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.applicationType
    , b1.terminalType
    , a1.orderid
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10)>=date_add('${zdt.format("yyyy-MM-dd")}',-1)
  and substring(b1.createdtime,0,10)<=date_add('${zdt.format("yyyy-MM-dd")}',-1)
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}' and b1.sellerid=0) a
group by a.cityid
)
join 
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.applicationType
    , b1.terminalType
    , a1.orderid
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d='${zdt.format("yyyy-MM-dd")}'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='${zdt.format("yyyy-MM-dd")}'
where substring(b1.createdtime,0,10)>=date_add('${zdt.format("yyyy-MM-dd")}',-1)
  and substring(b1.createdtime,0,10)<=date_add('${zdt.format("yyyy-MM-dd")}',-1)
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='${zdt.format("yyyy-MM-dd")}' and b1.sellerid=0
)








--单日，7日，30日酒店订单量


(select a.d
  , sum (a.ois) as ois
from
(select a.d
  ,if(a.d>='2018-05-26', sum(if(a.terminalType=10, 1, 0))
      , sum(if(a.applicationType=10 or a.applicationType is null, 1, 0))) as ois
from
(select distinct substring(b1.createdtime, 0, 10) as d
    , b1.applicationType
    , b1.terminalType
    , a1.orderid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)"
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"
where substring(b1.createdtime,0,10)>="$effectdate('yyyy-MM-dd',-16)"
  and substring(b1.createdtime,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)" and b1.sellerid=0) a
group by a.d
union all
select b.d
  , round(count(distinct b.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn
where substring(orderdate,0,10)>="$effectdate('yyyy-MM-dd',-16)"
  and substring(orderdate,0,10)<="$effectdate('yyyy-MM-dd',-1)"
  and d ="$effectdate('yyyy-MM-dd', 0)" ) b
  group by b.d)a group by a.d) 










use bnb_hive_db; 
drop table if exists tmp_zc_hotel_order;
create table tmp_zc_hotel_order as
	select c1.cityid,c1.ordernumber_hotel_past_day,c2.ordernumber_hotel_past_7_days,c3.ordernumber_hotel_past_30_days from (
      
        select om.cityid,count(om.orderid) as ordernumber_hotel_past_day
        from dw_htlmaindb.facthotelorder_all_inn om left join Dim_HtlDB.dimhtlhotel dhh on dhh.hotel = om.hotel
        where om.ordertype_ubt = '直接订单' and om.orderstatus != 'C' 
        --and om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-1) and om.orderdate<'${zdt.format("yyyy-MM-dd")}' 
      	and om.d ='${zdt.format("yyyy-MM-dd")}'
        group by om.cityid
      
	) c1
	join (
        select om.cityid,count(om.orderid) as ordernumber_hotel_past_7_days
        from dw_htlmaindb.facthotelorder_all_inn om left join Dim_HtlDB.dimhtlhotel dhh on dhh.hotel = om.hotel
        where om.ordertype_ubt = '直接订单' and om.orderstatus != 'C' 
        and om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-7) and om.orderdate<'${zdt.format("yyyy-MM-dd")}' and om.d ='${zdt.format("yyyy-MM-dd")}'
        group by om.cityid
	) c2
	on c1.cityid = c2.cityid
	join(
        select om.cityid,count(om.orderid) as ordernumber_hotel_past_30_days
        from dw_htlmaindb.facthotelorder_all_inn om left join Dim_HtlDB.dimhtlhotel dhh on dhh.hotel = om.hotel
        where om.ordertype_ubt = '直接订单' and om.orderstatus != 'C' 
        and om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-30) and om.orderdate<'${zdt.format("yyyy-MM-dd")}' and om.d ='${zdt.format("yyyy-MM-dd")}'
        group by om.cityid
    ) c3
	on c1.cityid = c3.cityid

         SELECT
           om.cityId,
           count(om.orderid) AS orderNumber
         FROM dw_htlmaindb.facthotelorder_all_inn om LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
         WHERE om.ordertype_ubt = '直接订单' AND om.orderstatus != 'C' AND
               om.d ='${zdt.format("yyyy-MM-dd")}'
         GROUP BY om.cityId
		
		select * from dw_htlmaindb.facthotelorder_all_inn where d = '2018-06-04' and ordertype_ubt = '直接订单'


--城市名称，DAU，支付总订单，转化率，民宿支付订单，客栈支付订单，WAU，支付总订单，转化率，民宿支付订单，客栈支付订单，MAU，支付总订单，转化率，民宿支付订单，客栈支付订单
select a.dau, b.ordernumber_homestay_past_day + b.
from tmp_zc_uv a
join tmp_zc_homestay_order b
on a.cityid = b.cityid
join tmp_zc_hotel_order c
on a.cityid = c.cityid


select a.cityid,a.ordernumber_homestay_past_day+b.orderNumber_hotel_past_day as ordernumber_total_past_day from bnb_hive_db.tmp_zc_homestay_order a
join bnb_hive_db.tmp_zc_hotel_order b
on a.cityid = b.cityid
















--当日，7日，30日订单量
use bnb_hive_db; 
drop table if exists tmp_zc_order;
create table tmp_zc_order as
select * from 
    (
	select t1.cityid,sum(t1.orderNumber) as ordernumber_past_day from (
          select ois.cityid,count(oh.orderid) as ordernumber
          from ods_htl_bnborderdb.order_header_v2 oh,
               ods_htl_bnborderdb.order_item oi,
               ods_htl_bnborderdb.order_item_space ois
          where	oh.orderId = oi.orderId and oi.orderItemId = ois.orderItemId and oh.paystatusId in (12, 20, 22, 23)
                and ois.cityId is not null
                and oh.createdTime >= date_add('${zdt.format("yyyy-MM-dd")}',-1)  and oh.createdTime <'${zdt.format("yyyy-MM-dd")}'  
      			and oh.salesChannel = 1 
      			and oh.visitsource not in (14, 18)
                and oh.d = '${zdt.format("yyyy-MM-dd")}'  AND oi.d = '${zdt.format("yyyy-MM-dd")}'  AND ois.d = '${zdt.format("yyyy-MM-dd")}' 
          group by ois.cityId

          union all

          select om.cityid,count(om.orderid) as ordernumber_hotel_past_day
          from dw_htlmaindb.facthotelorder_all_inn om left join Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
          where om.ordertype_ubt = '直接订单' and om.orderstatus != 'C' 
      			and om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-1) and om.orderdate<'${zdt.format("yyyy-MM-dd")}' and om.d ='${zdt.format("yyyy-MM-dd")}'
          group by om.cityId
        ) t1
        group by t1.cityId
	) b1
	join
    (
	select t2.cityid,sum(t2.ordernumber) as ordernumber_past_7_days from (
          select ois.cityid,count(oh.orderid) as ordernumber
          from ods_htl_bnborderdb.order_header_v2 oh,
               ods_htl_bnborderdb.order_item oi,
               ods_htl_bnborderdb.order_item_space ois
          where	oh.orderid = oi.orderid and oi.orderitemid = ois.orderitemid and oh.paystatusid in (12, 20, 22, 23)
                and ois.cityid is not null
                and oh.createdtime >= date_add('${zdt.format("yyyy-MM-dd")}',-7)  AND oh.createdtime <'${zdt.format("yyyy-MM-dd")}'  
      			and oh.saleschannel = 1 and oh.visitsource not in (14, 18)
                and oh.d = '${zdt.format("yyyy-MM-dd")}'  
      			and oi.d = '${zdt.format("yyyy-MM-dd")}'  
      			and ois.d = '${zdt.format("yyyy-MM-dd")}' 
          GROUP BY ois.cityId

          UNION ALL

          SELECT om.cityId,count(om.orderid) AS orderNumber
          FROM dw_htlmaindb.facthotelorder_all_inn om LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
          WHERE om.ordertype_ubt = '直接订单' AND om.orderstatus != 'C' 
      			AND om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-7) AND
                om.orderdate<'${zdt.format("yyyy-MM-dd")}' AND om.d ='${zdt.format("yyyy-MM-dd")}'
          GROUP BY om.cityId
        ) t2
        group by t2.cityId
	) b2
	ON b1.cityId = b2.cityId
	JOIN
    (
	select t3.cityId,sum(t3.orderNumber) AS orderNumber from (
          SELECT ois.cityId,count(oh.orderId) AS orderNumber
          FROM ods_htl_bnborderdb.order_header_v2 oh,
               ods_htl_bnborderdb.order_item oi,
               ods_htl_bnborderdb.order_item_space ois
          WHERE	oh.orderId = oi.orderId AND oi.orderItemId = ois.orderItemId AND oh.payStatusId IN (12, 20, 22, 23)
                AND ois.cityId IS NOT NULL
                AND oh.createdTime >= date_add('${zdt.format("yyyy-MM-dd")}',-1)  AND oh.createdTime <'${zdt.format("yyyy-MM-dd")}'  AND oh.salesChannel = 1 AND
                oh.visitsource NOT IN (14, 18)
                AND oh.d = '${zdt.format("yyyy-MM-dd")}'  AND oi.d = '${zdt.format("yyyy-MM-dd")}'  AND ois.d = '${zdt.format("yyyy-MM-dd")}' 
          GROUP BY ois.cityId

          UNION ALL

          SELECT om.cityId,count(om.orderid) AS orderNumber
          FROM dw_htlmaindb.facthotelorder_all_inn om LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
          WHERE om.ordertype_ubt = '直接订单' AND om.orderstatus != 'C' AND om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-1) AND
                om.orderdate<'${zdt.format("yyyy-MM-dd")}' AND om.d ='${zdt.format("yyyy-MM-dd")}'
          GROUP BY om.cityId
        ) t3
        group by t3.cityId
	) b3
	ON b1.cityId = b3.cityId
















SELECT * FROM (
  SELECT
  c.cityName as `城市名称`,
 a.visitNumber  as `UV`,
b.orderNumber as `支付订单数`,
concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber,2) as string),'%')  as `转化率` 

select * 
FROM (
       SELECT
         get_json_object(value, '$.cityid')              AS cityid,
         count(DISTINCT newvalue.data['env_clientcode']) AS DAU
       FROM dw_mobdb.factmbtracelog_hybrid
       WHERE d = date_add('${zdt.format("yyyy-MM-dd")}',-1) AND KEY in ("100641","bnb_inn_list_app_basic")
                                      GROUP BY get_json_object( VALUE, '$.cityid')	
     ) a1
  JOIN (
        SELECT
         get_json_object(value, '$.cityid')              AS cityid,
         count(DISTINCT newvalue.data['env_clientcode']) AS WAU
       FROM dw_mobdb.factmbtracelog_hybrid
       WHERE d < '${zdt.format("yyyy-MM-dd")}' AND d >= date_add('${zdt.format("yyyy-MM-dd")}',-7) AND KEY in ("100641","bnb_inn_list_app_basic")
                                      GROUP BY get_json_object( VALUE, '$.cityid')  
  ) a2
  ON a1.cityid = a2.cityid
  JOIN (
  		SELECT
         get_json_object(value, '$.cityid')              AS cityid,
         count(DISTINCT newvalue.data['env_clientcode']) AS MAU
       FROM dw_mobdb.factmbtracelog_hybrid
       WHERE d < '${zdt.format("yyyy-MM-dd")}' AND d >= date_add('${zdt.format("yyyy-MM-dd")}',-30) AND KEY in ("100641","bnb_inn_list_app_basic")
                                      GROUP BY get_json_object( VALUE, '$.cityid')
  ) a3
  ON a1.cityid = a3.cityid

  
  JOIN (
 select b1.cityId,sum(b1.orderNumber) AS orderNumber from (
         SELECT
           ois.cityId,
           count(oh.orderId) AS orderNumber
         FROM ods_htl_bnborderdb.order_header_v2 oh,
           ods_htl_bnborderdb.order_item oi,
           ods_htl_bnborderdb.order_item_space ois
         WHERE
           oh.orderId = oi.orderId AND oi.orderItemId = ois.orderItemId AND oh.payStatusId IN (12, 20, 22, 23)
           AND ois.cityId IS NOT NULL
           AND oh.createdTime >= date_add('${zdt.format("yyyy-MM-dd")}',-1)  AND oh.createdTime <'${zdt.format("yyyy-MM-dd")}'  AND oh.salesChannel = 1 AND
           oh.visitsource NOT IN (14, 18)
           AND oh.d = '${zdt.format("yyyy-MM-dd")}'  AND oi.d = '${zdt.format("yyyy-MM-dd")}'  AND ois.d = '${zdt.format("yyyy-MM-dd")}' 
         GROUP BY ois.cityId

         UNION ALL

         SELECT
           om.cityId,
           count(om.orderid) AS orderNumber
         FROM dw_htlmaindb.facthotelorder_all_inn om LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
         WHERE om.ordertype_ubt = '直接订单' AND om.orderstatus != 'C' AND
               om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-1) and om.orderdate<'${zdt.format("yyyy-MM-dd")}' AND om.d ='${zdt.format("yyyy-MM-dd")}'
         GROUP BY om.cityId
	) b1
    group by b1.cityId
JOIN (
select b2.cityId,sum(b2.orderNumber) AS orderNumber from (
         SELECT
           ois.cityId,
           count(oh.orderId) AS orderNumber
         FROM ods_htl_bnborderdb.order_header_v2 oh,
           ods_htl_bnborderdb.order_item oi,
           ods_htl_bnborderdb.order_item_space ois
         WHERE
           oh.orderId = oi.orderId AND oi.orderItemId = ois.orderItemId AND oh.payStatusId IN (12, 20, 22, 23)
           AND ois.cityId IS NOT NULL
           AND oh.createdTime >= date_add('${zdt.format("yyyy-MM-dd")}',-1)  AND oh.createdTime <'${zdt.format("yyyy-MM-dd")}'  AND oh.salesChannel = 1 AND
           oh.visitsource NOT IN (14, 18)
           AND oh.d = '${zdt.format("yyyy-MM-dd")}'  AND oi.d = '${zdt.format("yyyy-MM-dd")}'  AND ois.d = '${zdt.format("yyyy-MM-dd")}' 
         GROUP BY ois.cityId

         UNION ALL

         SELECT
           om.cityId,
           count(om.orderid) AS orderNumber
         FROM dw_htlmaindb.facthotelorder_all_inn om LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
         WHERE om.ordertype_ubt = '直接订单' AND om.orderstatus != 'C' AND
               om.orderdate >=date_add('${zdt.format("yyyy-MM-dd")}',-1) and om.orderdate<'${zdt.format("yyyy-MM-dd")}' AND om.d ='${zdt.format("yyyy-MM-dd")}'
         GROUP BY om.cityId
		) b2    
		group by b2.cityId
  )
  		on 
JOIN (


)
       ) b ON a.cityId = b.cityId
  JOIN ods_htl_groupwormholedb.bnb_city c ON a.cityId = c.cityId AND c.d = :date

UNION ALL

SELECT
  '汇总' as `城市名称`,
 a.visitNumber  as `UV`,
b.orderNumber as `支付订单数`,
concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber,2) as string),'%')  as `转化率` 

FROM (
       SELECT
         'all' as `all`,
         count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
       FROM dw_mobdb.factmbtracelog_hybrid
       WHERE d = :date-1 AND KEY in ("100641","bnb_inn_list_app_basic")
     ) a
 LEFT JOIN (
 select 'all' as `all`, sum(e.orderNumber) AS orderNumber from (
         SELECT
           count(oh.orderId) AS orderNumber
         FROM ods_htl_bnborderdb.order_header_v2 oh,
           ods_htl_bnborderdb.order_item oi,
           ods_htl_bnborderdb.order_item_space ois
         WHERE
           oh.orderId = oi.orderId AND oi.orderItemId = ois.orderItemId AND oh.payStatusId IN (12, 20, 22, 23)
           AND ois.cityId IS NOT NULL
           AND oh.createdTime >= :date-1  AND oh.createdTime <:date  AND oh.salesChannel = 1 AND
           oh.visitsource NOT IN (14, 18)
           AND oh.d = :date  AND oi.d = :date  AND ois.d = :date 

         UNION ALL

         SELECT
           count(om.orderid) AS orderNumber
         FROM dw_htlmaindb.facthotelorder_all_inn om LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
         WHERE om.ordertype_ubt = '直接订单' AND om.orderstatus != 'C' AND
               om.orderdate >=:date-1 and om.orderdate<:date AND om.d =:date
) e 
       ) b ON a.all = b.all
 ) all

 
order by  `支付订单数`  desc , `转化率` desc limit 10000






desc ods_htl_groupwormholedb.bnb_city

select * from ods_htl_groupwormholedb.bnb_city where d = '2018-06-03' limit 10

desc dw_mobdb.factmbtracelog_hybrid

select * from dw_htlmaindb.facthotelorder_all_inn where d = '2018-06-03' 

desc dw_htlmaindb.facthotelorder_all_inn
