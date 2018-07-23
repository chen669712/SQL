--民宿订单（去除港澳台）
select month(a.d)
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
    , c1.cityid
from ods_htl_bnborderdb.order_item a1
left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d= '2018-07-16'
left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d='2018-07-16'
inner join (select cityid,cityname,provincename from ods_htl_groupwormholedb.bnb_city where d = '2018-07-16'
            and (cityname not in ('香港','澳门','台湾') or provincename not in ('香港','澳门','台湾'))) d1
            on c1.cityid = d1.cityid 
where substring(b1.createdtime,0,10)>='2018-01-01'
  and substring(b1.createdtime,0,10)<='2018-06-30'
  and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
  and (a1.statusid like '12%' OR a1.statusid like '20%' OR a1.statusid like '22%' OR a1.statusid like '23%')
  and a1.saleamount>=20 and a1.d='2018-07-16' and b1.sellerid=0) a
group by a.d) a
group by month(a.d)

--客栈订单（去除港澳台）
select month(a.d)
  , round(count(distinct a.orderid)*0.85,0) as ois
from
(select substring(orderdate, 0, 10) as d
  ,cityid
  ,orderid
from dw_htlmaindb.FactHotelOrder_All_Inn F
where substring(orderdate,0,10)>='2018-01-01'
  and substring(orderdate,0,10)<='2018-06-30'
  and d ='2018-07-16' 
  and F.FromPackageOrder='F' 
  and F.custtype<>'CC' 
  and F.IsFHpkg='F' 
  and F.IsCorp='F') a
inner join (select cityid,cityname,provincename from ods_htl_groupwormholedb.bnb_city where d = '2018-07-16'
            and (cityname not in ('香港','澳门','台湾') or provincename not in ('香港','澳门','台湾'))) b
on a.cityid = b.cityid 
group by month(a.d)