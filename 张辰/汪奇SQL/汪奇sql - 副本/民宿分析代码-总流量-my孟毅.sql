select * from
  (select a.day
      ,a.`首页总UV`
      ,b.`列表页总UV`
      ,c.`宫格详情页UV`
      ,d.`宫格填写页UV`
      ,e.`宫格支付直接订单`
      ,f.`宫格支付引流订单`
      ,concat(cast(100*(e.`宫格支付直接订单`/a.`首页总UV`) as decimal(5,2)),'%') AS `转化率`
  from
    (select  d as day
      , count(distinct  clientcode) as `首页总UV`
    from DW_MobDB.factmbpageview
    where d>=:date-8
      and d<=:date-1  
      and pagecode in ('600003560')    -- 600003560 民宿首页
      and (prepagecode = '215019' or prepagecode = 'home')  -- 215019 团购首页
    group by d ) a   ---- 首页
  left join (
    select  d as day
      , count(distinct  clientcode) as `列表页总UV`
    from DW_MobDB.factmbpageview
    where d>=:date-8  
        and d<=:date-1  
        and pagecode in ('600003563')   -- 600003563 民宿产品列表页
        and (prepagecode IN ('600003560'))
      group by d) b --- 列表页
    on a.day=b.day
  left join ( 
    select  a1.day as day
      , count(distinct  a1.clientcode) as `宫格详情页UV`
    from
      (select distinct d as day
        ,clientcode as clientcode
      from DW_MobDB.factmbpageview
      where d>=:date-8
        and d<=:date-1  
        and pagecode in ('hotel_inland_detail')    -- 国内酒店详情页
        and (prepagecode IN ('600003563'))
      union all
      select distinct d as day
        ,clientcode as clientcode
      from DW_MobDB.factmbpageview
      where d>=:date-8  
        and d<=:date-1  
        and pagecode in ('600003564')    -- 600003564 民宿产品详情页
        and prepagecode IN ('600003563'))a1
       group by a1.day) c --- 详情页
    on a.day=c.day
  left join 
    (select  b1.day
      ,count(distinct  b1.clientcode) as `宫格填写页UV`
    from
      (select distinct a1.d as day
        ,a1.clientcode as clientcode
      from
        (select * 
        from
          (select a.d
            ,a.clientcode
            ,count(distinct a.type) as time
            from
            (select distinct '客栈详情页' as type
              ,d 
              ,clientcode
            from DW_MobDB.factmbpageview
            where d>=:date-8
              and d<=:date-1  
              and pagecode in ('hotel_inland_detail') 
              and (prepagecode IN ('600003563'))   -- 600003563 民宿产品列表页
          union all
          select distinct '客栈填写页' as type
            ,d 
            ,clientcode
          from DW_MobDB.factmbpageview
          where d>=:date-8 
            and d<=:date-1  
            and pagecode in ('hotel_inland_order')       --hotel_inland_order 国内酒店订单填写页
            and (prepagecode IN ('hotel_inland_detail')))a
          group by a.d
          ,a.clientcode)b 
        where b.time>1)a1
        union all
        select distinct d as day
          ,clientcode as clientcode
        from DW_MobDB.factmbpageview
        where d>=:date-8  
          and d<=:date-1  
          and pagecode in ('600003570')            --600003570 民宿-订单填写页
          and prepagecode IN ('600003564'))b1      -- 600003564 民宿-产品详情页
        group by b1.day)d --- 填写页 
    on a.day=d.day

  left join
    (select c.day
      ,sum(c.directorder) as `宫格支付直接订单`
    from
      (
      -------酒店的客栈订单 --------  
      select to_date(a.orderdate) as day
         ,count(distinct a.orderid) as directorder
      from dw_htlmaindb.FactHotelOrder_All_Inn a
      where to_date(a.orderdate)>=:date-8
        and to_date(a.orderdate)<=:date-1 
        and a.d=:date 
        and a.ordertype_ubt='直接订单'
      group by to_date(a.orderdate)

      union all
      --------老订单表，不再更新，仅到2017年4月份-----------
      select b1.day
        ,count(distinct b1.ctriporderid) as directorder
      from
        (select to_date(a.createtime) as day
            ,a.ctriporderid
            ,datediff(to_date(a.checkout),to_date(a.checkin))*(a.quantity) as output
            ,a.totalamount	
  	        ,case when a.statusId in (11,19) 
                      OR (a.statusId = 13 AND a.paymentStatusId = 3) then '预订成功'
                  when (a.statusId = 12 AND a.vendorId != 108) 
                      OR (a.vendorId = 108 AND a.paymentStatusId = 2) 
                      OR (a.vendorid !=108 and a.statusid=7) then '房东拒单'
                  when  a.statusid in (11,10,19) 
                      or (a.paymentstatusid in (2,3))
                      or (a.statusid=12 and a.vendorid!=108)
                      or (a.statusId = 13  AND a.paymentStatusId = 3) then '支付成功'
                  when a.statusid=7 then '房东拒绝'
                  when a.statusid=8 then '未支付未确认'
                  when a.statusid=10 then '已支付未确认'                   
                  when a.statusid=12 then '其他原因关闭'
                  when a.statusid=13 then '用户关闭'
                  when a.statusid=19 then '已完成'
                  when a.statusid=9 then '已确认未支付'
                  else '其他' 
            end as orderstatus            
          from ods_htl_groupwormholedb.bnb_order a
          where a.d=:date 
            and to_date(a.createtime)<=:date-1  
            and to_date(a.createtime)>=:date-8
            and a.saleamount>=20 )b1 
      where b1.orderstatus in('预订成功','房东拒单','支付成功')
      group by b1.day

      union all
      --------新的民宿的订单-----------
      select b2.day
        ,count(distinct b2.ctriporderid) as directorder
      from
        (select to_date(b1.createdtime) as day
            ,a1.orderid as ctriporderid
            ,a1.saleamount
            ,datediff(to_date(c1.checkout), to_date(c1.checkin))*(a1.quantity) as output 
        from ods_htl_bnborderdb.order_item a1
        left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=:date
        left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=:date
        where to_date(b1.createdtime)<=:date-1 
          and to_date(b1.createdtime)>=:date-8
          and a1.saleamount>=20 
          and a1.d=:date 
          and uid not in('$seller-agent') 
          and b1.visitsource not in (13,14,18) 
          and (a1.statusid like '12%' 
                OR a1.statusid like '20%' 
                OR a1.statusid like '22%' 
                OR a1.statusid like '23%') ) b2 
        group by b2.day) c  
    group by c.day) e  --- 支付订单
    on a.day=e.day

  left join
    (select c.day
        ,sum(c.directorder) as `宫格支付引流订单`
    from
    (select b2.day
        ,count(distinct b2.ctriporderid) as directorder
      from
        (select to_date(b1.createdtime) as day
          ,a1.orderid as ctriporderid
          ,a1.saleamount
          ,datediff(to_date(c1.checkout), to_date(c1.checkin))*(a1.quantity) as output 
        from ods_htl_bnborderdb.order_item a1
        left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=:date
        left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=:date
        where to_date(b1.createdtime)<=:date-1 
          and to_date(b1.createdtime)>=:date-8
          and a1.saleamount>=20 
          and a1.d=:date and uid not in('$seller-agent') 
          and b1.visitsource in (13,14,18) 
          and (a1.statusid like '12%' 
              OR a1.statusid like '20%' 
              OR a1.statusid like '22%' 
              OR a1.statusid like '23%') ) b2 
        group by b2.day)c
    group by c.day)f 
    on a.day=f.day )b
sort by b.day desc