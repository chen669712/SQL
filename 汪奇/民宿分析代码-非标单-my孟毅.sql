select t1.day as `日期`,
round((t1.tujiaorderid+t1.mayiorderid+t1.dayuorder+t1.qitaorderid+t1.jieruorderid+t1.fenxiaoorderid+t1.jiaogeorder),0) as `总订单`,
t1.tujiaorderid as `宫格途家订单`,
t1.mayiorderid as  `宫格蚂蚁订单`,
t1.dayuorder as `宫格大鱼订单`,
t1.jieruorderid as `客栈公寓订单`,
t1.jiaogeorder as `交割宫格订单`,
t1.qitaorderid as `宫格其他订单`,
t1.fenxiaoorderid  as `酒店频道订单`,
round((t1.tujiaoutput+t1.mayioutput+t1.dayuoutput+t1.qitaoutput+t1.jieruoutput+t1.fenxiaooutput+t1.jiaogeoutput),0) as `总间夜`,
t1.tujiaoutput as `宫格途家间夜`,
t1.mayioutput as  `宫格蚂蚁间夜`,
t1.dayuoutput as `宫格大鱼间夜`,
t1.jieruoutput  as `客栈公寓宫格间夜`,
t1.jiaogeoutput as `交割宫格间夜`,
t1.qitaoutput as `宫格其他间夜`,
t1.fenxiaooutput as `酒店频道间夜`,
round((t1.tujiagmv+t1.mayigmv+t1.dayugmv+t1.qitagmv+t1.jierugmv+t1.fenxiaogmv+t1.jiaogegmv)/(t1.tujiaorderid+t1.mayiorderid+t1.dayuorder+t1.qitaorderid+t1.jieruorderid+t1.fenxiaoorderid+t1.jiaogeorder),0) as `订单总均价`,
round(t1.tujiagmv/t1.tujiaorderid,0) as `宫格途家订单均价`,
round(t1.mayigmv/t1.mayiorderid,0) as `宫格蚂蚁订单均价`,
round(t1.dayugmv/t1.dayuorder,0) as `宫格大鱼订单均价`,
round(t1.jierugmv/t1.jieruorderid,0) as `客栈公寓宫格订单均价`,
case when round(t1.jiaogegmv/t1.jiaogeorder,0 ) is null then 0 else round(t1.jiaogegmv/t1.jiaogeorder,0 ) end  as `交割宫格订单均价`,
round(t1.qitagmv/t1.qitaorderid,0) as `宫格其他订单均价`,
case when round(t1.fenxiaogmv/t1.fenxiaoorderid,0 ) is null then 0 else round(t1.fenxiaogmv/t1.fenxiaoorderid,0 ) end  as `酒店频道订单均价`,
round((t1.tujiaoutput+t1.mayioutput+t1.dayuoutput+t1.qitaoutput+t1.jieruoutput+t1.fenxiaooutput+t1.jiaogeoutput)/(t1.tujiaorderid+t1.mayiorderid+t1.dayuorder+t1.qitaorderid+t1.jieruorderid+t1.fenxiaoorderid+t1.jiaogeorder),2) as `订单平均间夜`,
t1.uv as `民宿宫格首页uv`,
concat(cast(100*((t1.zhijieorderid)/t1.uv) as decimal(5,2)),'%') as `转化率`,
ROUND((t1.tujiagmv+t1.mayigmv+t1.dayugmv+t1.qitagmv+t1.jierugmv+t1.fenxiaogmv+t1.jiaogegmv),0) as `非标总GMV`
from(
  select t2.day,
    t2.tujiaorderid,
    t2.tujiaoutput,
    t2.tujiagmv,
    t2.mayiorderid,
    t2.mayioutput,
    t2.mayigmv,
    t2.qitaorderid,
    t2.qitaoutput,
    t2.qitagmv,
    t2.jieruorderid,
    t2.jieruoutput,
    t2.jierugmv,
    t2.fenxiaoorderid,
    t2.fenxiaooutput,
    t2.fenxiaogmv,
    t2.jiaogeorder,
    t2.jiaogeoutput,
    t2.jiaogegmv,
    t2.uv,
    t2.zhijieorderid,
    t2.zhijieoutput,
    t2.zhijiegmv,
    case when t2.dayuorderid is null then 0 else t2.dayuorderid end as dayuorder,
    case when t2.dayugmv is null then 0 else t2.dayugmv end as dayugmv,
    case when t2.dayuoutput is null then 0 else t2.dayuoutput end as dayuoutput
  from(
    select a.day,a.tujiaorderid,a.tujiaoutput,a.tujiagmv,
      b.mayiorderid,b.mayioutput,b.mayigmv,
      c.dayuorderid,c.dayuoutput,c.dayugmv,
      d.qitaorderid,d.qitaoutput,d.qitagmv,
      e.jieruorderid,e.jieruoutput,e.jierugmv,
      f.fenxiaoorderid,f.fenxiaooutput,f.fenxiaogmv,
      g.jiaogeorder,g.jiaogeoutput,g.jiaogegmv,
      h.uv,
      i.zhijieorderid,i.zhijieoutput,i.zhijiegmv
    from
    (select to_date(b1.createdtime) as day,
      count(distinct a1.orderid) as tujiaorderid,
      sum(a1.saleamount) as tujiagmv,
      sum (datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity)) as tujiaoutput
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=:date
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=:date
    where to_date(b1.createdtime)<=:date-1
      and to_date(b1.createdtime)>=:date-8
      and a1.saleamount>=20 and a1.d=:date
      and b1.sellerid=0 and a1.vendorid in (105,115)
      and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
    group by to_date(b1.createdtime)) a-----------------------------途家

    left join

    (select to_date(b1.createdtime) as day,
      count(distinct a1.orderid) as mayiorderid,
      sum(a1.saleamount) as mayigmv,
      sum (datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity)) as mayioutput
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=:date
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=:date
    where to_date(b1.createdtime)<=:date-1
      and to_date(b1.createdtime)>=:date-8
      and a1.saleamount>=20 and a1.d=:date
      and b1.sellerid=0 and a1.vendorid in (104,114)
      and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
    group by to_date(b1.createdtime)) b  on a.day=b.day---------------------------蚂蚁

    left join

    (select to_date(b1.createdtime) as day,
      count(distinct a1.orderid) as dayuorderid,
      sum(a1.saleamount) as dayugmv,
      sum (datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity)) as dayuoutput
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=:date
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=:date
    where to_date(b1.createdtime)<=:date-1 and to_date(b1.createdtime)>=:date-8
      and a1.saleamount>=20 and a1.d=:date
      and b1.sellerid=0 and a1.vendorid in (201)
      and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
    group by to_date(b1.createdtime)) c on c.day=a.day-----------------------------大鱼

    left join

    (select to_date(b1.createdtime) as day,
      count(distinct a1.orderid) as qitaorderid,
      sum(a1.saleamount) as qitagmv,
      sum (datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity)) as qitaoutput
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=:date
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=:date
    where to_date(b1.createdtime)<=:date-1 and to_date(b1.createdtime)>=:date-8
      and a1.saleamount>=20 and a1.d=:date
      and b1.sellerid=0 and a1.vendorid not in (105,115,104,114,201)
      and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
    group by to_date(b1.createdtime)) d on a.day=d.day-------------------------------其他

    left join

    (select to_date(a.orderdate) as day,
      count(distinct a.orderid) as jieruorderid,
      sum(a.ciiquantity) as jieruoutput,sum(a.ciireceivable) as jierugmv
    from dw_htlmaindb.FactHotelOrder_All_Inn a
    join dim_htldb.dimroom dr on a.room=dr.room
    join dim_htldb.dimhtlhotel dh on dr.hotel=dh.hotel
    where to_date(a.orderdate)>=:date-8  and to_date(a.orderdate)<=:date-1
      and a.d=:date
      and (a.ordertype_ubt not in ('hotel_order') or a.ordertype_ubt is null)
      and dh.masterhotelid not in ()  ----这里有非交个产品Id集
    group by to_date(a.orderdate) ) g on g.day=a.day----------------------交割

    left join

    (select  d as day,
      count(distinct  clientcode) as uv
    from DW_MobDB.factmbpageview
    where d>=:date-8  and d<=:date-1
      and pagecode in ('600003560', '10320675332')
      and prepagecode in ('home', '215019', '0')
    group by d)h on h.day=a.day-----------------------------------首页UV

    left join

    (select x.day,
      count(x.orderid) as zhijieorderid,
      sum(x.output) as zhijieoutput,
      sum(x.gmv) as zhijiegmv
    from
    (select distinct to_date(b1.createdtime) as day,
      a1.orderid,
      a1.saleamount as gmv,
      datediff(to_date(c1.checkout),to_date(c1.checkin))*(a1.quantity) as output
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d=:date
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d=:date
    where to_date(b1.createdtime)<=:date-1 and to_date(b1.createdtime)>=:date-8
      and a1.saleamount>=20 and a1.d=:date
      and b1.sellerid =0 and b1.visitsource not in (13,14,18)
      and (a1.statusid like '12%' or a1.statusid like '20%' or a1.statusid like '22%' or a1.statusid like '23%')
    union all
    select distinct to_date(a.orderdate) as day,
      a.orderid,
      a.ciireceivable as gmv,
      a.ciiquantity as output
    from dw_htlmaindb.FactHotelOrder_All_Inn a
    join dim_htldb.dimroom dr on a.room=dr.room
    join dim_htldb.dimhtlhotel dh on dr.hotel=dh.hotel
    where to_date(a.orderdate)>=:date-8  and to_date(a.orderdate)<=:date-1
      and a.d=:date and a.ordertype_ubt='直接订单')x
    group by x.day)i on i.day=a.day--------------------直接订单
  )t2
)t1 sort by `日期` desc
