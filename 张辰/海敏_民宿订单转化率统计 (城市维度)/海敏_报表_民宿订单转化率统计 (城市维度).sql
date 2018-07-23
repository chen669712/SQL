SELECT * 
FROM (
SELECT 
    m.cityName as `城市名称`
  , a.visitNumber as `dau`
  , b.orderNumber as `日支付订单数`
  , concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber, 2) as string),'%') as `日转化率` 
  , c.orderNumber as `日民宿订单数`
  , d.orderNumber as `日客栈订单数`
  , e.visitNumber as `wau`
  , f.orderNumber as `周支付订单数`
  , concat_ws('',cast(round((f.orderNumber*100)/e.visitNumber, 2) as string),'%') as `周转化率`
  , g.orderNumber as `周民宿订单数`
  , h.orderNumber as `周客栈订单数`
  , i.visitNumber as `mau`
  , j.orderNumber as `月支付订单数`
  , concat_ws('',cast(round((j.orderNumber*100)/i.visitNumber, 2) as string),'%') as `月转化率`
  , k.orderNumber as `月民宿订单数`
  , l.orderNumber as `月客栈订单数`
FROM (
  SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d = :date-1 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) a        
JOIN 
( select t1.cityId
    ,sum(t1.orderNumber) AS orderNumber   
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime >= :date-1 
      AND oh.createdTime < :date 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = :date  
      AND oi.d = :date  
      AND ois.d = :date 
    GROUP BY ois.cityId    
    UNION ALL    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber  
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate >= :date-1 
      AND om.orderdate < :date 
      AND om.d = :date 
    GROUP BY om.cityId ) t1 
group by cityId ) b ON a.cityId = b.cityId
JOIN
( SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime >= :date-1 
      AND oh.createdTime < :date 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = :date  
      AND oi.d = :date  
      AND ois.d = :date 
    GROUP BY ois.cityId) c ON a.cityId = c.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate >= :date-1 
      AND om.orderdate< :date 
      AND om.d = :date
    GROUP BY om.cityId ) d ON a.cityId = d.cityId
JOIN
(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= :date-1
    AND d >= :date-8 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) e        
ON a.cityid = e.cityid
JOIN 
( select t2.cityId
    ,sum(t2.orderNumber) AS orderNumber
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= :date-1 
      AND oh.createdTime >= :date-8 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = :date  
      AND oi.d = :date  
      AND ois.d = :date 
    GROUP BY ois.cityId   
    UNION ALL  
    SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= :date-1 
      AND om.orderdate >= :date-8 
      AND om.d =:date 
    GROUP BY om.cityId ) t2
group by cityId ) f ON e.cityId = f.cityId
JOIN
( SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= :date-1 
      AND oh.createdTime >= :date-8 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = :date  
      AND oi.d = :date  
      AND ois.d = :date 
    GROUP BY ois.cityId) g ON e.cityId = g.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= :date-1 
      AND om.orderdate >= :date-8 
      AND om.d = :date
    GROUP BY om.cityId ) h ON e.cityId = h.cityId
JOIN
(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= :date-1
    AND d >= :date-31 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) i 
ON a.cityid = i.cityid
JOIN 
( select t3.cityId
    ,sum(t3.orderNumber) AS orderNumber
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= :date-1 
      AND oh.createdTime >= :date-31 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = :date  
      AND oi.d = :date  
      AND ois.d = :date 
    GROUP BY ois.cityId
    UNION ALL    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= :date-1 
      AND om.orderdate >= :date-31 
      AND om.d =:date 
    GROUP BY om.cityId ) t3 
group by cityId ) j ON i.cityId = j.cityId
JOIN
( SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= :date-1 
      AND oh.createdTime >= :date-31 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = :date  
      AND oi.d = :date  
      AND ois.d = :date 
    GROUP BY ois.cityId) k ON i.cityId = k.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= :date-1 
      AND om.orderdate >= :date-31 
      AND om.d = :date
    GROUP BY om.cityId ) l ON i.cityId = l.cityId
JOIN ods_htl_groupwormholedb.bnb_city m ON i.cityId = m.cityId AND m.d = '2018-06-26'
UNION ALL
SELECT '汇总' as `城市名称`
  , n.visitNumber  as `dau`
  , o.orderNumber as `日支付订单数`
  , concat_ws('',cast(round((o.orderNumber*100)/n.visitNumber,2) as string),'%') as `日转化率`
  , p.orderNumber as `日民宿订单数`
  , q.orderNumber as `日客栈订单数`
  , r.visitNumber  as `wau`
  , s.orderNumber as `周支付订单数`
  , concat_ws('',cast(round((s.orderNumber*100)/r.visitNumber,2) as string),'%') as `周转化率`
  , t.orderNumber as `周民宿订单数`
  , u.orderNumber as `周客栈订单数`
  , v.visitNumber  as `mau`
  , w.orderNumber as `月支付订单数`
  , concat_ws('',cast(round((w.orderNumber*100)/v.visitNumber,2) as string),'%') as `月转化率`
  , x.orderNumber as `月民宿订单数`
  , y.orderNumber as `月客栈订单数`
FROM (
  SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d = :date-1 
    AND KEY IN ('100641','bnb_inn_list_app_basic') ) n
JOIN (
    select 'all' as `all`
      , sum(t4.orderNumber) AS orderNumber 
    from (
      SELECT count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime >= :date-1
        AND oh.createdTime < :date 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = :date 
        AND oi.d = :date
        AND ois.d = :date 
      UNION ALL
      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= :date-1 
        AND om.orderdate<:date 
        AND om.d = :date ) t4 
  ) o ON n.all = o.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime >= :date-1
        AND oh.createdTime < :date 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = :date 
        AND oi.d = :date
        AND ois.d = :date 
) p ON n.all = p.all
JOIN (
      SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= :date-1 
        AND om.orderdate<:date 
        AND om.d = :date 
) q ON n.all = q.all 
JOIN 
(
  SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= :date-1
    AND d >= :date-8
    AND KEY IN ('100641','bnb_inn_list_app_basic') ) r ON n.all = r.all
JOIN (
    select 'all' as `all`
      , sum(t5.orderNumber) AS orderNumber 
    from (
      SELECT count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= :date-1
        AND oh.createdTime >= :date-8 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = :date 
        AND oi.d = :date
        AND ois.d = :date 
      UNION ALL
      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= :date-1
        AND om.orderdate >= :date-8 
        AND om.orderdate<:date 
        AND om.d = :date ) t5 
  ) s ON r.all = s.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= :date-1
        AND oh.createdTime >= :date-8 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = :date 
        AND oi.d = :date
        AND ois.d = :date 
) t ON r.all = t.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= :date-1
        AND om.orderdate >= :date-8 
        AND om.orderdate<:date 
        AND om.d = :date
) u ON r.all = u.all 
JOIN
( SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= :date-1
    AND d >= :date-31
    AND KEY IN ('100641','bnb_inn_list_app_basic') ) v ON n.all = v.all
JOIN (
    select 'all' as `all`
      , sum(t6.orderNumber) AS orderNumber 
    from (
      SELECT count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= :date-1
        AND oh.createdTime >= :date-31 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = :date 
        AND oi.d = :date
        AND ois.d = :date 
      UNION ALL
      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= :date-1
        AND om.orderdate >= :date-31 
        AND om.orderdate<:date 
        AND om.d = :date ) t6 
  ) w ON v.all = w.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= :date-1
        AND oh.createdTime >= :date-31 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = :date 
        AND oi.d = :date
        AND ois.d = :date 
) x ON v.all = x.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= :date-1
        AND om.orderdate >= :date-31 
        AND om.orderdate<:date 
        AND om.d = :date 
) y ON v.all = y.all 
) all
order by  `日支付订单数` desc,`周支付订单数`desc,`月支付订单数`desc,`日转化率` desc,`周转化率` desc，`月转化率` desc
limit 10000




------------------------------------报表-------------------------------------------------------
SELECT * 
FROM (
SELECT 
    m.cityName as `城市名称`
  , a.visitNumber as `dau`
  , b.orderNumber as `日支付订单数`
  , concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber, 2) as string),'%') as `日转化率` 
  , c.orderNumber as `日民宿订单数`
  , d.orderNumber as `日客栈订单数`
  , e.visitNumber as `wau`
  , f.orderNumber as `周支付订单数`
  , concat_ws('',cast(round((f.orderNumber*100)/e.visitNumber, 2) as string),'%') as `周转化率`
  , g.orderNumber as `周民宿订单数`
  , h.orderNumber as `周客栈订单数`
  , i.visitNumber as `mau`
  , j.orderNumber as `月支付订单数`
  , concat_ws('',cast(round((j.orderNumber*100)/i.visitNumber, 2) as string),'%') as `月转化率`
  , k.orderNumber as `月民宿订单数`
  , l.orderNumber as `月客栈订单数`

select a.cityid
from  (select get_json_object(value, '$.cityid') as cityid
         , count(distinct newvalue.data['env_clientcode']) as visitnumber
      from dw_mobdb.factmbtracelog_hybrid
      where d = "$effectdate('yyyy-MM-dd',-1)"
        and key in ('100641','bnb_inn_list_app_basic')
      group by get_json_object( VALUE, '$.cityid')
      ) a        
      inner join  
      (select t3.cityid
        , sum (t3.ois) as ois
      from
        (select t1.cityid,count(distinct t1.orderid) as ois from
          (select distinct a1.orderid
              , c1.cityid
          from ods_htl_bnborderdb.order_item a1
          left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)"
          left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"
          where substring(b1.createdtime,0,10)>="$effectdate('yyyy-MM-dd',-1)"
            and substring(b1.createdtime,0,10)<"$effectdate('yyyy-MM-dd',0)"
            and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
            and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
            and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)" and b1.sellerid=0 and b1.terminalType=10
          ) t1
        group by t1.cityid
        union all
        select t2.cityid
          , round(count(distinct t2.orderid)*0.85,0) as ois
        from  (select cityid
                ,orderid
              from dw_htlmaindb.FactHotelOrder_All_Inn
              where substring(orderdate,0,10)>="$effectdate('yyyy-MM-dd',-1)"
                and substring(orderdate,0,10)<"$effectdate('yyyy-MM-dd',0)"
                and d ="$effectdate('yyyy-MM-dd', 0)" 
              ) t2
        group by t2.cityid
        ) t3 
        group by t3.cityid
      ) b 
      on a.cityId = b.cityId
      inner join
      ( select t1.cityid,count(distinct t1.orderid) as ois from
          (select distinct a1.orderid
              , c1.cityid
          from ods_htl_bnborderdb.order_item a1
          left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="$effectdate('yyyy-MM-dd',0)"
          left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="$effectdate('yyyy-MM-dd',0)"
          where substring(b1.createdtime,0,10)>="$effectdate('yyyy-MM-dd',-1)"
            and substring(b1.createdtime,0,10)<"$effectdate('yyyy-MM-dd',0)"
            and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
            and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
            and a1.saleamount>=20 and a1.d="$effectdate('yyyy-MM-dd',0)" and b1.sellerid=0 and b1.terminalType=10
          ) t1
        group by t1.cityid
      ) c
      on a.cityid = c.cityid 
      inner join
      (select t2.cityid
          , round(count(distinct t2.orderid)*0.85,0) as ois
        from  (select cityid
                ,orderid
              from dw_htlmaindb.FactHotelOrder_All_Inn
              where substring(orderdate,0,10)>="$effectdate('yyyy-MM-dd',-1)"
                and substring(orderdate,0,10)<"$effectdate('yyyy-MM-dd',0)"
                and d ="$effectdate('yyyy-MM-dd', 0)" 
              ) t2
        group by t2.cityid
      ) d
      on a.cityid = d.cityid 
      inner join
      (select get_json_object(value, '$.cityid') as cityid
         , count(distinct newvalue.data['env_clientcode']) as visitnumber
      from dw_mobdb.factmbtracelog_hybrid
      where d >= "$effectdate('yyyy-MM-dd',-31)"
        and d <= 
        and key in ('100641','bnb_inn_list_app_basic')
      group by get_json_object( VALUE, '$.cityid')
      ) e ON a.cityId = e.cityId
JOIN




(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= "$effectdate('yyyy-MM-dd',-1)"
    AND d >= "$effectdate('yyyy-MM-dd',-8)" 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) e
ON a.cityid = e.cityid
JOIN 
( select t2.cityId
    ,sum(t2.orderNumber) AS orderNumber
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)" 
      AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-8)" 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = "$effectdate('yyyy-MM-dd',0)"  
      AND oi.d = "$effectdate('yyyy-MM-dd',0)"  
      AND ois.d = "$effectdate('yyyy-MM-dd',0)" 
    GROUP BY ois.cityId    
    UNION ALL    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)" 
      AND om.orderdate >= "$effectdate('yyyy-MM-dd',-8)" 
      AND om.d ="$effectdate('yyyy-MM-dd',0)" 
    GROUP BY om.cityId ) t2
group by cityId ) f ON e.cityId = f.cityId
JOIN
( SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)" 
      AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-8)" 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = "$effectdate('yyyy-MM-dd',0)"  
      AND oi.d = "$effectdate('yyyy-MM-dd',0)"  
      AND ois.d = "$effectdate('yyyy-MM-dd',0)" 
    GROUP BY ois.cityId) g ON e.cityId = g.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)" 
      AND om.orderdate >= "$effectdate('yyyy-MM-dd',-8)" 
      AND om.d = "$effectdate('yyyy-MM-dd',0)"
    GROUP BY om.cityId ) h ON e.cityId = h.cityId
JOIN
(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= "$effectdate('yyyy-MM-dd',-1)"
    AND d >= "$effectdate('yyyy-MM-dd',-31)" 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) i  
ON a.cityid = i.cityid
JOIN 
( select t3.cityId
    ,sum(t3.orderNumber) AS orderNumber
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)" 
      AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-31)" 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = "$effectdate('yyyy-MM-dd',0)"  
      AND oi.d = "$effectdate('yyyy-MM-dd',0)"  
      AND ois.d = "$effectdate('yyyy-MM-dd',0)" 
    GROUP BY ois.cityId    
    UNION ALL    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)" 
      AND om.orderdate >= "$effectdate('yyyy-MM-dd',-31)" 
      AND om.d ="$effectdate('yyyy-MM-dd',0)" 
    GROUP BY om.cityId ) t3 
group by cityId ) j ON i.cityId = j.cityId
JOIN
( SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)" 
      AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-31)" 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = "$effectdate('yyyy-MM-dd',0)"  
      AND oi.d = "$effectdate('yyyy-MM-dd',0)"  
      AND ois.d = "$effectdate('yyyy-MM-dd',0)" 
    GROUP BY ois.cityId) k ON i.cityId = k.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)" 
      AND om.orderdate >= "$effectdate('yyyy-MM-dd',-31)" 
      AND om.d = "$effectdate('yyyy-MM-dd',0)"
    GROUP BY om.cityId ) l ON i.cityId = l.cityId
JOIN ods_htl_groupwormholedb.bnb_city m ON i.cityId = m.cityId AND m.d = "$effectdate('yyyy-MM-dd',0)"
UNION ALL
SELECT '汇总' as `城市名称`
  , n.visitNumber  as `dau`
  , o.orderNumber as `日支付订单数`
  , p.orderNumber as `日民宿订单数`
  , q.orderNumber as `日客栈订单数`
  , concat_ws('',cast(round((o.orderNumber*100)/n.visitNumber,2) as string),'%') as `日转化率`
  , r.visitNumber  as `wau`
  , s.orderNumber as `周支付订单数`
  , t.orderNumber as `周民宿订单数`
  , u.orderNumber as `周客栈订单数`
  , concat_ws('',cast(round((s.orderNumber*100)/r.visitNumber,2) as string),'%') as `周转化率`
  , v.visitNumber  as `mau`
  , w.orderNumber as `月支付订单数`
  , x.orderNumber as `月民宿订单数`
  , y.orderNumber as `月客栈订单数`
  , concat_ws('',cast(round((w.orderNumber*100)/v.visitNumber,2) as string),'%') as `月转化率`
FROM (
  SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d = date_add('2018-06-26',-1) 
    AND KEY IN ('100641','bnb_inn_list_app_basic') ) n
JOIN (
    select 'all' as `all`
      , sum(t4.orderNumber) AS orderNumber 
    from (
      SELECT count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-1)"
        AND oh.createdTime < "$effectdate('yyyy-MM-dd',0)" 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = "$effectdate('yyyy-MM-dd',0)" 
        AND oi.d = "$effectdate('yyyy-MM-dd',0)"
        AND ois.d = "$effectdate('yyyy-MM-dd',0)" 
      UNION ALL
      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= "$effectdate('yyyy-MM-dd',-1)" 
        AND om.orderdate<"$effectdate('yyyy-MM-dd',0)" 
        AND om.d = "$effectdate('yyyy-MM-dd',0)" ) t4 
  ) o ON n.all = o.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-1)"
        AND oh.createdTime < "$effectdate('yyyy-MM-dd',0)" 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = "$effectdate('yyyy-MM-dd',0)"
        AND oi.d = "$effectdate('yyyy-MM-dd',0)"
        AND ois.d = "$effectdate('yyyy-MM-dd',0)" 
) p ON n.all = p.all
JOIN (
      SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= "$effectdate('yyyy-MM-dd',-1)" 
        AND om.orderdate<"$effectdate('yyyy-MM-dd',0)" 
        AND om.d = "$effectdate('yyyy-MM-dd',0)" 
) q ON n.all = q.all 
JOIN 
(
  SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= "$effectdate('yyyy-MM-dd',-1)"
    AND d >= "$effectdate('yyyy-MM-dd',-8)"
    AND KEY IN ('100641','bnb_inn_list_app_basic') ) r ON n.all = r.all
JOIN (
    select 'all' as `all`
      , sum(t5.orderNumber) AS orderNumber 
    from (
      SELECT count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)"
        AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-8)" 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = "$effectdate('yyyy-MM-dd',0)" 
        AND oi.d = "$effectdate('yyyy-MM-dd',0)"
        AND ois.d = "$effectdate('yyyy-MM-dd',0)" 
      UNION ALL
      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)"
        AND om.orderdate >= "$effectdate('yyyy-MM-dd',-8)" 
        AND om.d = "$effectdate('yyyy-MM-dd',-1)" ) t5 
  ) s ON r.all = s.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)"
        AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-8)" 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = "$effectdate('yyyy-MM-dd',0)" 
        AND oi.d = "$effectdate('yyyy-MM-dd',0)" 
        AND ois.d = "$effectdate('yyyy-MM-dd',0)"  
) t ON r.all = t.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)" 
        AND om.orderdate >= "$effectdate('yyyy-MM-dd',-8)"   
        AND om.d = "$effectdate('yyyy-MM-dd',0)"  
) u ON r.all = u.all 
JOIN
( SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= "$effectdate('yyyy-MM-dd',-1)" 
    AND d >= "$effectdate('yyyy-MM-dd',-31)" 
    AND KEY IN ('100641','bnb_inn_list_app_basic') ) v ON n.all = v.all
JOIN (
    select 'all' as `all`
      , sum(t6.orderNumber) AS orderNumber 
    from (
      SELECT count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)" 
        AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-31)"  
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = "$effectdate('yyyy-MM-dd',0)"  
        AND oi.d = "$effectdate('yyyy-MM-dd',0)" 
        AND ois.d = "$effectdate('yyyy-MM-dd',0)"  
      UNION ALL
      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)" 
        AND om.orderdate >= "$effectdate('yyyy-MM-dd',-31)"  
        AND om.d = "$effectdate('yyyy-MM-dd',0)"  ) t6 
  ) w ON v.all = w.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= "$effectdate('yyyy-MM-dd',-1)" 
        AND oh.createdTime >= "$effectdate('yyyy-MM-dd',-31)"  
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = "$effectdate('yyyy-MM-dd',0)"  
        AND oi.d = "$effectdate('yyyy-MM-dd',0)" 
        AND ois.d = "$effectdate('yyyy-MM-dd',0)"  
) x ON v.all = x.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= "$effectdate('yyyy-MM-dd',-1)" 
        AND om.orderdate >= "$effectdate('yyyy-MM-dd',-31)" 
        AND om.d = "$effectdate('yyyy-MM-dd',0)"  
) y ON v.all = y.all 
) all
order by  `日支付订单数` desc,`周支付订单数` desc,`月支付订单数` desc,`日转化率` desc，`周转化率` desc，`月转化率` desc
limit 10000

