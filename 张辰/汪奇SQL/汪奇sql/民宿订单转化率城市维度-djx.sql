SELECT * 
FROM (
SELECT c.cityName as `城市名称`
  , a.visitNumber as `UV`
  , b.orderNumber as `支付订单数`
  , concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber, 2) as string),'%') as `转化率` 
FROM (
  SELECT get_json_object(value, '$.cityid') AS cityid
         , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d = :Date-1 
    AND KEY ="100641"
  GROUP BY get_json_object( VALUE, '$.cityid')) a        --- 搜索城市的用户集
JOIN 
( select e.cityId
    ,sum(e.orderNumber) AS orderNumber   --- 民宿订单
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
      AND oh.createdTime >= :Date-1 
      AND oh.createdTime <:Date 
      AND oh.salesChannel = 1 
      AND oh.visitsource NOT IN (14, 18)
      AND oh.d = :Date  
      AND oi.d = :Date  
      AND ois.d = :Date 
    GROUP BY ois.cityId
    
    UNION ALL
    
    SELECT om.cityId
      , count(om.orderid) AS orderNumber   --- 客栈订单
    FROM dw_htlmaindb.facthotelorder_all_inn om
    LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
    WHERE om.ordertype_ubt = '直接订单' 
      AND om.orderstatus != 'C' 
      AND om.orderdate >=:Date-1 
      AND om.orderdate<:Date 
      AND om.d =:Date
    GROUP BY om.cityId )e 
group by cityId ) b ON a.cityId = b.cityId

JOIN ods_htl_groupwormholedb.bnb_city c ON a.cityId = c.cityId AND c.d = :Date

UNION ALL

SELECT '汇总' as `城市名称`
  , a.visitNumber  as `UV`
  , b.orderNumber as `支付订单数`
  , concat_ws('',cast(round((b.orderNumber*100)/a.visitNumber,2) as string),'%') as `转化率` 
FROM (
  SELECT 'all' as `all`
    , count(DISTINCT newvalue.data['env_clientcode']) AS visitNumber
  FROM dw_mobdb.factmbtracelog_hybrid
  WHERE d = :Date-1 
    AND KEY ="100641" ) a
  
  LEFT JOIN (
    select 'all' as `all`
      , sum(e.orderNumber) AS orderNumber 
    from (
      SELECT count(oh.orderId) AS orderNumber
      FROM ods_htl_bnborderdb.order_header_v2 oh,
        ods_htl_bnborderdb.order_item oi,
        ods_htl_bnborderdb.order_item_space ois
      WHERE oh.orderId = oi.orderId 
        AND oi.orderItemId = ois.orderItemId 
        AND oh.payStatusId IN (12, 20, 22, 23)
        AND ois.cityId IS NOT NULL
        AND oh.createdTime >= :Date-1
        AND oh.createdTime <:Date 
        AND oh.salesChannel = 1 
        AND oh.visitsource NOT IN (14, 18)
        AND oh.d = :Date 
        AND oi.d = :Date 
        AND ois.d = :Date 

      UNION ALL

      SELECT count(om.orderid) AS orderNumber
      FROM dw_htlmaindb.facthotelorder_all_inn om 
      LEFT JOIN Dim_HtlDB.dimhtlhotel dhh ON dhh.hotel = om.hotel
      WHERE om.ordertype_ubt = '直接订单' 
        AND om.orderstatus != 'C'
        AND om.orderdate >=:Date-1 
        AND om.orderdate<:Date 
        AND om.d =:Date ) e 
  ) b ON a.all = b.all
) all
order by  `支付订单数`  desc
  ,`转化率` desc
limit 10000