  use bnb_hive_db;
insert overwrite table bnb_data_coupon_date
partition(d='${zdt.addDay(-1).format("yyyy-MM-dd")}')
select a.uid as UId
	, a.promotionid
    , c.name
    , a.startdate
    , a.enddate
    , a.createtime
    , b.createdate as usedate
    , b.orderid
from Dw_pubsharedb.factcoupon_BNB a
left join Dw_pubsharedb.FactCouponOrdDetail_BNB b on a.couponid = b.couponid
left join Dw_pubsharedb.factpromotion_BNB c on a.promotionid = c.promotionid
where to_date(a.startdate) <= '${zdt.addDay(0).format("yyyy-MM-dd")}'
and a.promotionid in (66644,
73292,
73291,
72772,
72770,
72769,
72767,
58887,
72232,
72230,
72229,
62273,
63137,
63135,
74814,
70810,
65477,
64622,
74880,
74834,
74733,
74255,
74254,
73293,
72169,
72168,
72064,
70693,
70692,
70691,
69099,
69098,
68744,
68741,
68739,
68195,
68193,
68192,
67459,
67458,
67455,
67248,
67247,
67245,
75190,
75165,
75163,
74836
)



---宫格统计
select *
from 
(select 
lql.promotionid,
lql.promotionname,
lql.date,
lql.lq,
syl.sy,
syl.sy/lql.lq as bl,
syl.yl,
syl.jlr
from
(select promotionid, 
		promotionname, 
		to_date(createtime) as date,
		count(createtime) as lq
 from bnb_hive_db.bnb_data_coupon_date
 where d = "$effectdate('yyyy-MM-dd',-1)"
 and to_date(createtime) >= "$effectdate('yyyy-MM-dd',-7)"
 and promotionid in (73292,
					73291,
					72772,
					72770,
					72769,
					72767,
					72232,
					72230,
					72229,
					74814)
 group by promotionid, promotionname, to_date(createtime)) lql
left join 
(select a.promotionid, 
		a.promotionname, 
		to_date(a.usedate) date,
		count(a.usedate) as sy,
		sum(b.orderamount-c.costamount) as yl,
        sum(b.orderamount-c.costamount)/count(a.usedate) as jlr
 from bnb_hive_db.bnb_data_coupon_date a 
 left join Dw_pubsharedb.FactCouponOrdDetail_BNB b
 on a.couponID = b.orderid
 left join ods_htl_bnborderdb.order_item c
 on a.couponID = c.orderid and c.d = "$effectdate('yyyy-MM-dd',-1)"
 where a.d = "$effectdate('yyyy-MM-dd',-1)"
 and to_date(a.usedate) >= "$effectdate('yyyy-MM-dd',-7)"
 and a.promotionid in (73292,
					73291,
					72772,
					72770,
					72769,
					72767,
					72232,
					72230,
					72229,
					74814)
 group by a.promotionid, a.promotionname, to_date(a.usedate)) syl
on lql.promotionid = syl.promotionid and lql.date=syl.date) tj
where tj.date <= "$effectdate('yyyy-MM-dd',-1)"


----非宫格统计
select *
from 
(select 
lql.promotionid,
lql.promotionname,
if((lql.promotionid in(70693,70692,70691,68195,68193,68192)),'超级会员', 
			if((lql.promotionid in(74880)),'攻略首页',
				if((lql.promotionid in(73293)),'机票订单完成页',
					if((lql.promotionid in(68744,68741,68739)),'积分商城', 
						if((lql.promotionid in(74255,74254,72169,72168,72064,75165,75163,74836)),'其他', 
							if((lql.promotionid in(62273,63137,63135,70810,65477,64622,69099,69098,67248,67247,67245)),'市场活动', 
								if((lql.promotionid in(67459,67458,67455)),'我的特权',
									if((lql.promotionid in(74733)),'我携领券中心', 
										if((lql.promotionid in(74834)),'站内信推广',
											if((lql.promotionid in(58887)),'众筹活动',
                                                 if((lql.promotionid in(75190)),'船票活动页','NULL'))))))))))) as source,
lql.date,
lql.lq,
syl.sy,
syl.sy/lql.lq as bl,
syl.yl,
syl.jlr
from
(select promotionid, 
		promotionname, 
		to_date(createtime) as date,
		count(createtime) as lq
 from bnb_hive_db.bnb_data_coupon_date
 where d = "$effectdate('yyyy-MM-dd',-1)"
 and to_date(createtime) >= "$effectdate('yyyy-MM-dd',-7)"
 and promotionid in (70693,
					70692,
					70691,
					68195,
					68193,
					68192,
					74880,
					73293,
					68744,
					68741,
					68739,
					74255,
					74254,
					72169,
					72168,
					72064,
					62273,
					63137,
					63135,
					70810,
					65477,
					64622,
					69099,
					69098,
					67248,
					67247,
					67245,
					67459,
					67458,
					67455,
					74733,
					74834,
					58887)
 group by promotionid, promotionname, to_date(createtime)) lql
left join 
(select a.promotionid, 
		a.promotionname, 
		to_date(a.usedate) date,
		count(a.usedate) as sy,
		sum(b.orderamount-c.costamount) as yl,
        sum(b.orderamount-c.costamount)/count(a.usedate) as jlr
 from bnb_hive_db.bnb_data_coupon_date a 
 left join Dw_pubsharedb.FactCouponOrdDetail_BNB b
 on a.couponID = b.orderid
 left join ods_htl_bnborderdb.order_item c
 on a.couponID = c.orderid and c.d = "$effectdate('yyyy-MM-dd',-1)"
 where a.d = "$effectdate('yyyy-MM-dd',-1)"
 and to_date(a.usedate) >= "$effectdate('yyyy-MM-dd',-7)"
 and a.promotionid in (70693,
					70692,
					70691,
					68195,
					68193,
					68192,
					74880,
					73293,
					68744,
					68741,
					68739,
					74255,
					74254,
					72169,
					72168,
					72064,
					62273,
					63137,
					63135,
					70810,
					65477,
					64622,
					69099,
					69098,
					67248,
					67247,
					67245,
					67459,
					67458,
					67455,
					74733,
					74834,
					58887)
 group by a.promotionid, a.promotionname, to_date(a.usedate)) syl
on lql.promotionid = syl.promotionid and lql.date=syl.date) tj
where tj.date <= "$effectdate('yyyy-MM-dd',-1)"


