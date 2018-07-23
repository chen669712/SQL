--提交单手机型号拆分
select d
	, clientversion
	, operationsystem
	, count(distinct clientcode) as uv
from dw_mobdb.factmbpageview
where d>='2018-07-01'
	and pagecode = '600003560'
	and prepagecode in('home', '0')
group by d
	, clientversion
	, operationsystem

select substring(b1.createdtime, 0, 10) as d
        , b1.applicationType
        , b1.terminalType
        , count(distinct a1.orderid) as oi
    from ods_htl_bnborderdb.order_item a1
    left join ods_htl_bnborderdb.order_header_v2 b1 on a1.orderid=b1.orderid and b1.d="2018-07-10"
    left join ods_htl_bnborderdb.order_item_space c1 on c1.orderitemid=a1.orderitemid and c1.d="2018-07-10"
    where substring(b1.createdtime,0,10)>="2018-07-01"
      and b1.visitsource in (0, 20, 70, 120, 130, 201, 203, 205)
      and a1.saleamount>=20 and a1.d="2018-07-10" and b1.sellerid=0
	 group by substring(b1.createdtime, 0, 10)
        , b1.applicationType
        , b1.terminalType