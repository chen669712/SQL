select a.cityname as `城市名称`
	,a.DAU as `DAU`
	,a.order_d as `日支付总订单`
	,a.conversion_d as `日转化率`
	,a.bnborder_d as `日民宿支付订单`
	,a.hotelorder_d as `日客栈支付订单`
	,b.WAU as `WAU`
	,b.order_w as `周支付总订单`
	,b.conversion_w as `周转化率`
	,b.bnborder_w as `周民宿支付订单`
	,b.hotelorder_w as `周客栈支付订单`
	,c.MAU as `MAU`
	,c.order_m as `月支付总订单`
	,c.conversion_m as `月转化率`
	,c.bnborder_m as `月民宿支付订单`
	,c.hotelorder_m as `月客栈支付订单`
from 
(select * from bnb_hive_db.bnb_data_order_conversion_rate_d where d = "$effectdate('yyyy-MM-dd',-1)" and order_d > 0) a
left outer join 
(select * from bnb_hive_db.bnb_data_order_conversion_rate_w where d = "$effectdate('yyyy-MM-dd',-1)" and order_w > 0) b 
on a.cityname = b.cityname
left outer join 
(select * from bnb_hive_db.bnb_data_order_conversion_rate_m where d = "$effectdate('yyyy-MM-dd',-1)" and order_m > 0) c
on a.cityname = c.cityname
