-- hive sql tips
-- 1.子查询必须在from语句中
-- 2.列别名不能加''
-- 3.列别名为中文时，必须用``包围
-- 4.有聚合函数时，查询的字段必须在group by 中(=> Invalid column reference 'xxx')
-- 5.有排序时，必须同时有limit 
-- if(source_uv.uv>0, concat(round(count(distinct oh.orderId)/source_uv.uv,4)*100, '%'), '-') as `转化率`
-- :date 可作为变量表示今天, :date-1 表示昨天
-- 注意不能用zdt.addDay(-1).format(),只能用:date

   SELECT 
      (
        case
          when oh.visitSource = 13 then '蜂鸟入口'
          when oh.visitSource = 14 then '酒店列表banner'
          when oh.visitSource = 15 then '攻略民宿爆款推荐'
          when oh.visitSource = 16 then '攻略直播乌镇民宿戏剧节'
          when oh.visitSource = 17 then '途家豪宅统计订单'
          when oh.visitSource = 20 then '携程APP民宿宫格'
          when oh.visitSource = 31 then '携程团购列表页'
          when oh.visitSource = 32 then '携程团购详情页'
          when oh.visitSource = 33 then '携程团购首页'
          when oh.visitSource = 40 then '来自汽车票'
          when oh.visitSource = 50 then '携程攻略'
          when oh.visitSource = 21 then '携程酒店SEO和SEM'
          when oh.visitSource = 60 then '全站搜索'
          when oh.visitSource = 70 then 'IM跳详情页'
          when oh.visitSource = 80 then '我的行程-目的地'
          when oh.visitSource = 81 then '我的行程-出发地'
          when oh.visitSource = 22 then '我的行程订单'
          when oh.visitSource = 23 then '酒店跳订单详情页'
          when oh.visitSource = 100 then '携程APP民宿宫格首页banner'
          when oh.visitSource = 18 then '酒店tab页跳转民宿'
          when oh.visitSource = 19 then '酒店tab页跳转民宿（海外）'
          when oh.visitSource = 101 then '携程APP美食林首页banner'
          when oh.visitSource = 102 then '携程APP火车票首页banner'
          when oh.visitSource = 103 then '携程攻略首页banner'
          when oh.visitSource = 104 then '携程团购首页banner'
          when oh.visitSource = 105 then '携程微信公众号'
          when oh.visitSource = 106 then '1028周年庆的分会场'
          when oh.visitSource = 107 then '途家漂亮房子活动'
          when oh.visitSource = 108 then '北雁南飞2期'
          when oh.visitSource = 109 then '途家豪宅二期'
          when oh.visitSource = 110 then '途家豪宅三期'
          when oh.visitSource = 111 then '火车票首页banner'
          when oh.visitSource = 90 then '产品推荐站内信'
          when oh.visitSource = 120 then '浏览历史'
          when oh.visitSource = 130 then '我的收藏'
          when collect_set(oh.salesChannel)[0] = 2 then '酒店分销'
        else '其他'
        end
      ) as `订单来源（订单统计范围: #order_create_date#）`,
      COALESCE(collect_set(source_uv.uv)[0],'-') as `UV`,
      count(distinct oh.orderId) as `支付订单量`
   FROM ods_htl_bnborderdb.order_header_v2  oh 
   left join ods_htl_bnborderdb.order_item  oi on oh.orderId = oi.orderId AND oi.d = :date 
   left join ods_htl_bnborderdb.order_item_space  ois on ois.orderItemId = oi.orderItemId AND  ois.d = :date
   left join 
   (
      select 
         case 
            when pagecode = '600003560' then 13
            when pagecode = '600003563' then 14
            else '未知'
         end
       as source, 
      count(distinct clientcode) as uv
      FROM dw_mobdb.factmbpageview
      WHERE d = :date-1 and 
      (
          (pagecode = '600003560' and prepagecode = 'hotel_inland_inquire') 
          or
          (pagecode = '600003563' and prepagecode = 'hotel_inland_list')
      )
      GROUP BY pagecode

   ) source_uv on source_uv.source = oh.visitSource
   WHERE 
       from_unixtime(unix_timestamp(oh.createdTime, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') 
       = :date-1 AND 
      oh.paystatusid IN ( 12, 20, 22, 23 ) AND 
      oh.d = :date
   GROUP BY oh.visitSource
   order by `支付订单量` desc LIMIT 100
