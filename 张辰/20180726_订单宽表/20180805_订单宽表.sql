use bnb_hive_db;
insert overwrite table bnb_data_FactOrderInfo
partition(d = '${zdt.addDay(-1).format("yyyy-MM-dd")}')
    select a.orderid as orderid 
        , a.orderdate as ordertime
        , a.uid as uid
        , null as vid
        , a.clientid as clientid
        , a.hotel as productid
        , case when a.uidname = '' then null else a.uidname end as contactsname
        , a.orderstatus as orderstatus --订单状态 C：取消 P：处理中（己确认用户和酒店） S：成交（包括全部成交和提前离店） W：提交未处理
        , substr(a.arrival,1,10) as checkin 
        , substr(a.departure,1,10) as checkout
        , a.ordcost as ordercost
        , a.ordamount as orderamount
        , a.ciiamount as payamount
        , null as discountamount
        , null as realcost
        , null as refundamount
        , null as refunddisamount
        , couponid as couponid
        , couponcode as couponcode
        , a.cityid as cityid
        , null as vendorId
        , a.serverfrom as terminal
        , case when a.submitfrom = 'client' then 10
               when a.submitfrom = 'H5' then 20
               when a.submitfrom = 'online' then 30 
               else a.submitfrom end as platform
        , null as visitsource
        , 2 as category 
        , null as saleschannel
        , null as sellerId
        , null as sellerorderid
        , null as isinvoice
        , null as deposit
        , null as deposittype
        , a.allianceid as allianceid
        , null as alliancesid
        , null as allianceouid
        , null as alliancesourceid
        , case when a.paymenttype in ('TMONY','TMony') then 1 else 0 end as isgiftcard
        , null as activityid
        , null as ext       
    from
    (select orderid,orderdate,uid,clientid,hotel,uidname,orderstatus,arrival,departure,ordcost,ordamount,ciiamount,cityid,serverfrom,submitfrom,allianceid,paymenttype,couponid,couponcode from dwhtl.edw_htl_order_all
      where d = '${zdt.addDay(-1).format("yyyy-MM-dd")}'
      and subchannel = 'h5_kezhan') a

    union all

    select a.orderid as orderid
          , a.createdtime as ordertime
          , a.uid as uid
          , null as vid
          , a.clientid as clientid
          , c.spaceid as productid
          , null as contactsname
          , b.statusid as orderstatus 
          , c.checkin as checkin
          , c.checkout as checkout
          , a.totalcostamount as ordercost
          , a.totalsaleamount as orderamount
          , d.payamount as payamount
          , e.payamount as discountamount
          , null as realcost
          , f.payamount as refundamount
          , g.payamount as refunddisamount
          , e.cardparentno as couponid
          , e.cardno as couponcode
          , c.cityid as cityid
          , b.vendorid as vendorid
          , case when a.d >= '2018-05-26' then a.applicationtype else a.terminaltype end as terminal
          , case when a.d >= '2018-05-26' then a.terminaltype else a.applicationType end as platform
          , a.visitsource as visitsource
          , 1 as category
          , a.saleschannel as saleschannel
          , a.sellerid as sellerid
          , a.sellerorderid as sellerorderid
          , case when k.orderid is not null then 1 else 0 end as isinvoice
          , case when a.depositonlinepayment > 0 then a.depositonlinepayment
                 when a.depositofflinepayment > 0 then a.depositofflinepayment
                 else a.depositonlinepayment end as deposit
          , case when a.depositonlinepayment > 0 then 1      --deposittype=1 为线上押金
                 when a.depositofflinepayment > 0 then 2     --deposittype=2 为线下押金
                 else 0 end as deposittype                   --deposittype=0 为无押金
          , a.allianceid as allianceid
          , a.alliancesid as alliancesid
          , a.allianceouid as allianceouid
          , a.alliancesourceid as alliancesourceid
          , null as isgiftcard
          , null as activityid
          , null as ext
    from
      (select orderid
            , createdtime
            , uid
            , clientid
            , totalcostamount
            , totalsaleamount
            , onlinepayment
            , totaldiscountamount
            , applicationType
            , terminaltype
            , visitsource
            , saleschannel
            , sellerid
            , sellerorderid
            , depositonlinepayment
            , depositofflinepayment
            , allianceid
            , alliancesid
            , allianceouid
            , alliancesourceid
            , d
      from ods_htl_bnborderdb.order_header_v2 
      where d = '${zdt.addDay(0).format("yyyy-MM-dd")}') a
      left outer join
      (select orderid,orderitemid,statusid,vendorid from ods_htl_bnborderdb.order_item 
        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
        and producttype = 1) b
      on a.orderid =b.orderid
      left outer join 
      (select orderitemid,checkin,checkout,spaceid,cityid from ods_htl_bnborderdb.order_item_space where d = '${zdt.addDay(0).format("yyyy-MM-dd")}') c
      on b.orderitemid = c.orderitemid
      left outer join 
      (select orderid,payid,payamount,reverseflag,paychannel from ods_htl_bnborderdb.order_pay 
        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
        and payamounttype = 1
        and reverseflag = 1
        and paychannel = 10) d
      on a.orderid = d.orderid
      left outer join 
      (select orderid,payid,payamount,reverseflag,paychannel,cardparentno,cardno from ods_htl_bnborderdb.order_pay 
        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
        and payamounttype = 1
        and reverseflag = 1
        and paychannel = 11) e
      on a.orderid = e.orderid
      left outer join 
      (select orderid,sum(payamount) as payamount from ods_htl_bnborderdb.order_pay 
        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
        and payamounttype = 1
        and reverseflag = 2
        and paychannel = 10
      group by orderid) f
      on a.orderid = f.orderid
      left outer join
      (select orderid,sum(payamount) as payamount from ods_htl_bnborderdb.order_pay 
        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
        and payamounttype = 1
        and reverseflag = 2
        and paychannel = 11
      group by orderid) g
      on a.orderid = g.orderid
--      left outer join
--      (select transid,payid from ods_htl_bnborderdb.order_pay_trans 
--        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
--        and statusid = 2  --支付成功订单
--        and deletedflag = 0
--      group by transid,payid) h
--      on d.payid = h.payid
--      left outer join
--      (select transid,paytypecode from ods_htl_bnborderdb.order_pay_trans_detail 
--        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}' 
--        and paytypecode = 'TMPAY'
--      group by transid,paytypecode) i
--      on h.transid = i.transid
--      left outer join
--      (select orderid,couponid,couponcode,orderamount,deductionamount from Dw_pubsharedb.FactCouponOrdDetail_BNB) j
--      on e.paydesc = j.couponcode
      left outer join
      (select orderid from ods_htl_bnborderdb.order_item 
        where d = '${zdt.addDay(0).format("yyyy-MM-dd")}'
        and producttype = 2
        and statusid != -1
      group by orderid) k
      on a.orderid =k.orderid

union all

select old.ctriporderid as orderid
      , old.createtime as ordertime
      , old.uid as uid
      , null as vid
      , null as clientid
      , old.productid as productid
      , old.contactname as contactsname
      , old.statusid as orderstatus 
      , old.checkin as checkin
      , old.checkout as checkout
      , old.purchaseamount as ordercost
      , old.totalamount as orderamount
      , old.paymentamount as payamount
      , old.discountamount as discountamount
      , null as realcost
      , null as refundamount
      , null as refunddisamount
      , null as couponid
      , null as couponcode
      , bsa.cityid as cityid
      , old.vendorid as vendorid
      , null as terminal
      , old.terminaltype as platform
      , null as visitsource
      , 1 as category
      , null as saleschannel
      , null as sellerid
      , null as sellerorderid
      , null as isinvoice
      , null as deposit
      , null as deposittype                  
      , null as allianceid
      , null as alliancesid
      , null as allianceouid
      , null as alliancesourceid
      , null as isgiftcard
      , null as activityid
      , null as ext
  from 
  (select * from ods_htl_groupwormholedb.bnb_order
  where d = '${zdt.addDay(0).format("yyyy-MM-dd")}') old
  left join
  (select spaceid
    , cityid
  from ods_htl_groupwormholedb.bnb_space_address
  where d='${zdt.addDay(0).format("yyyy-MM-dd")}' ) bsa on old.productid = bsa.spaceid