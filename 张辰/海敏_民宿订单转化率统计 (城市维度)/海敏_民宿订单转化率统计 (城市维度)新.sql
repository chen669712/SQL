SELECT * 
FROM (
SELECT 
    m.cityName as `城市名称`
  , a.visitNumber as `dau`
  , b.orderNumber as `日支付订单数`
  , c.orderNumber as `日民宿订单数`
  , d.orderNumber as `日客栈订单数`
  , concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber, 2) as string),'%') as `日转化率` 
  , e.visitNumber as `wau`
  , f.orderNumber as `周支付订单数`
  , g.orderNumber as `周民宿订单数`
  , h.orderNumber as `周客栈订单数`
  , concat_ws('',cast(round((f.orderNumber*100)/e.visitNumber, 2) as string),'%') as `周转化率`
  , i.visitNumber as `mau`
  , j.orderNumber as `月支付订单数`
  , k.orderNumber as `月民宿订单数`
  , l.orderNumber as `月客栈订单数`
  , concat_ws('',cast(round((j.orderNumber*100)/i.visitNumber, 2) as string),'%') as `月转化率`
FROM (
  SELECT get_json_object(value, '$.cityid') AS cityid --- 搜索城市的用户集
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d = date_add('2018-06-26',-1) 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) a        
JOIN 
( select t1.cityId --- 民宿订单
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
      AND oh.createdTime >= date_add('2018-06-26',-1) 
      AND oh.createdTime < '2018-06-26' 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId
    
    UNION ALL
    
    SELECT om.cityId  --- 客栈订单
      , count(om.orderid) AS orderNumber  
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate >= date_add('2018-06-26',-1) 
      AND om.orderdate < '2018-06-26' 
      AND om.d ='2018-06-26' 
    GROUP BY om.cityId ) t1 
group by cityId ) b ON a.cityId = b.cityId
JOIN
( SELECT ois.cityId --- 民宿订单
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime >= date_add('2018-06-26',-1) 
      AND oh.createdTime < '2018-06-26' 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId) c ON a.cityId = c.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate >= date_add('2018-06-26',-1) 
      AND om.orderdate< '2018-06-26' 
      AND om.d = '2018-06-26'
    GROUP BY om.cityId ) d ON a.cityId = d.cityId
JOIN
----周数据
(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-8) 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) e        --- 搜索城市的用户集
ON a.cityid = e.cityid
JOIN 
( select t2.cityId
    ,sum(t2.orderNumber) AS orderNumber   --- 民宿订单
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-8) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId
    
    UNION ALL
    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-8) 
      AND om.d ='2018-06-26' 
    GROUP BY om.cityId ) t2
group by cityId ) f ON e.cityId = f.cityId
JOIN
( SELECT ois.cityId --- 民宿订单
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-8) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId) g ON e.cityId = g.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-8) 
      AND om.d = '2018-06-26'
    GROUP BY om.cityId ) h ON e.cityId = h.cityId
JOIN
----月数据
(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-31) 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) i        --- 搜索城市的用户集
ON a.cityid = i.cityid
JOIN 
( select t3.cityId
    ,sum(t3.orderNumber) AS orderNumber   --- 民宿订单
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-31) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId
    
    UNION ALL
    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-31) 
      AND om.d ='2018-06-26' 
    GROUP BY om.cityId ) t3 
group by cityId ) j ON i.cityId = j.cityId
JOIN
( SELECT ois.cityId --- 民宿订单
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-31) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId) k ON i.cityId = k.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-31) 
      AND om.d = '2018-06-26'
    GROUP BY om.cityId ) l ON i.cityId = l.cityId
JOIN ods_htl_groupwormholedb.bnb_city m ON i.cityId = m.cityId AND m.d = '2018-06-26'

--汇总数据
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
        AND oh.createdTime >= date_add('2018-06-26',-1)
        AND oh.createdTime < '2018-06-26' 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 

      UNION ALL

      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= date_add('2018-06-26',-1) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' ) t4 
  ) o ON n.all = o.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime >= date_add('2018-06-26',-1)
        AND oh.createdTime < '2018-06-26' 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 
) p ON n.all = p.all
JOIN (
      SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= date_add('2018-06-26',-1) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' 
) q ON n.all = q.all 
JOIN 
----周汇总数据
(
  SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-8)
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
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-8) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 

      UNION ALL

      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-8) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' ) t5 
  ) s ON r.all = s.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-8) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 
) t ON r.all = t.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-8) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' 
) u ON r.all = u.all 
JOIN
--月汇总数据
( SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-31)
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
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-31) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 

      UNION ALL

      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-31) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' ) t6 
  ) w ON v.all = w.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-31) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 
) x ON v.all = x.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-31) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' 
) y ON v.all = y.all 
) all
--order by  `支付订单数`  desc
--  ,`转化率` desc
limit 10000

------------------------------报表有数据----------------------------------------------------------
SELECT * 
FROM (
SELECT 
    m.cityName as `城市名称`
  , a.visitNumber as `dau`
  , b.orderNumber as `日支付订单数`
  , c.orderNumber as `日民宿订单数`
  , d.orderNumber as `日客栈订单数`
  , concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber, 2) as string),'%') as `日转化率` 
  , e.visitNumber as `wau`
  , f.orderNumber as `周支付订单数`
  , g.orderNumber as `周民宿订单数`
  , h.orderNumber as `周客栈订单数`
  , concat_ws('',cast(round((f.orderNumber*100)/e.visitNumber, 2) as string),'%') as `周转化率`
  , i.visitNumber as `mau`
  , j.orderNumber as `月支付订单数`
  , k.orderNumber as `月民宿订单数`
  , l.orderNumber as `月客栈订单数`
  , concat_ws('',cast(round((j.orderNumber*100)/i.visitNumber, 2) as string),'%') as `月转化率`
FROM (
  SELECT get_json_object(value, '$.cityid') AS cityid --- 搜索城市的用户集
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d = date_add('2018-06-26',-1) 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) a        
JOIN 
( select t1.cityId --- 民宿订单
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
      AND oh.createdTime >= date_add('2018-06-26',-1) 
      AND oh.createdTime < '2018-06-26' 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId
    
    UNION ALL
    
    SELECT om.cityId  --- 客栈订单
      , count(om.orderid) AS orderNumber  
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate >= date_add('2018-06-26',-1) 
      AND om.orderdate < '2018-06-26' 
      AND om.d ='2018-06-26' 
    GROUP BY om.cityId ) t1 
group by cityId ) b ON a.cityId = b.cityId
JOIN
( SELECT ois.cityId --- 民宿订单
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime >= date_add('2018-06-26',-1) 
      AND oh.createdTime < '2018-06-26' 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId) c ON a.cityId = c.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate >= date_add('2018-06-26',-1) 
      AND om.orderdate< '2018-06-26' 
      AND om.d = '2018-06-26'
    GROUP BY om.cityId ) d ON a.cityId = d.cityId
JOIN
----周数据
(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-8) 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) e        --- 搜索城市的用户集
ON a.cityid = e.cityid
JOIN 
( select t2.cityId
    ,sum(t2.orderNumber) AS orderNumber   --- 民宿订单
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-8) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId
    
    UNION ALL
    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-8) 
      AND om.d ='2018-06-26' 
    GROUP BY om.cityId ) t2
group by cityId ) f ON e.cityId = f.cityId
JOIN
( SELECT ois.cityId --- 民宿订单
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-8) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId) g ON e.cityId = g.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-8) 
      AND om.d = '2018-06-26'
    GROUP BY om.cityId ) h ON e.cityId = h.cityId
JOIN
----月数据
(SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-31) 
    AND KEY IN ('100641','bnb_inn_list_app_basic')
  GROUP BY get_json_object( VALUE, '$.cityid')) i        --- 搜索城市的用户集
ON a.cityid = i.cityid
JOIN 
( select t3.cityId
    ,sum(t3.orderNumber) AS orderNumber   --- 民宿订单
  from (
    SELECT ois.cityId
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-31) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId
    
    UNION ALL
    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-31) 
      AND om.d ='2018-06-26' 
    GROUP BY om.cityId ) t3 
group by cityId ) j ON i.cityId = j.cityId
JOIN
( SELECT ois.cityId --- 民宿订单
      , count(oh.orderId) AS orderNumber
    FROM ods_htl_bnborderdb.order_header_v2 oh,
      ods_htl_bnborderdb.order_item oi,
      ods_htl_bnborderdb.order_item_space ois
    WHERE oh.orderId = oi.orderId 
      AND oi.orderItemId = ois.orderItemId 
      AND oh.payStatusId IN (12, 20, 22, 23)
      AND ois.cityId IS NOT NULL
      AND oh.createdTime <= date_add('2018-06-26',-1) 
      AND oh.createdTime >= date_add('2018-06-26',-31) 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = '2018-06-26'  
      AND oi.d = '2018-06-26'  
      AND ois.d = '2018-06-26' 
    GROUP BY ois.cityId) k ON i.cityId = k.cityId
JOIN
( SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')  
      AND om.orderdate <= date_add('2018-06-26',-1) 
      AND om.orderdate >= date_add('2018-06-26',-31) 
      AND om.d = '2018-06-26'
    GROUP BY om.cityId ) l ON i.cityId = l.cityId
JOIN ods_htl_groupwormholedb.bnb_city m ON i.cityId = m.cityId AND m.d = '2018-06-26'

--汇总数据
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
        AND oh.createdTime >= date_add('2018-06-26',-1)
        AND oh.createdTime < '2018-06-26' 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 

      UNION ALL

      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= date_add('2018-06-26',-1) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' ) t4 
  ) o ON n.all = o.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime >= date_add('2018-06-26',-1)
        AND oh.createdTime < '2018-06-26' 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 
) p ON n.all = p.all
JOIN (
      SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate >= date_add('2018-06-26',-1) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' 
) q ON n.all = q.all 
JOIN 
----周汇总数据
(
  SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-8)
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
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-8) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 

      UNION ALL

      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-8) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' ) t5 
  ) s ON r.all = s.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-8) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 
) t ON r.all = t.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-8) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' 
) u ON r.all = u.all 
JOIN
--月汇总数据
( SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d <= date_add('2018-06-26',-1)
    AND d >= date_add('2018-06-26',-31)
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
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-31) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 

      UNION ALL

      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-31) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' ) t6 
  ) w ON v.all = w.all
JOIN (SELECT 'all' as `all`,count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime <= date_add('2018-06-26',-1)
        AND oh.createdTime >= date_add('2018-06-26',-31) 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = '2018-06-26' 
        AND oi.d = '2018-06-26'
        AND ois.d = '2018-06-26' 
) x ON v.all = x.all
JOIN (SELECT 'all' as `all`,count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE (om.ordertype_ubt IS NULL OR om.ordertype_ubt = '直接订单')
        AND om.orderdate <= date_add('2018-06-26',-1)
        AND om.orderdate >= date_add('2018-06-26',-31) 
        AND om.orderdate<'2018-06-26' 
        AND om.d = '2018-06-26' 
) y ON v.all = y.all 
) all
--order by  `支付订单数`  desc
--  ,`转化率` desc
limit 10000