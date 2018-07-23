--日本产品详情
select a.spaceid,a.name,a.description from 
(select name,spaceid,description from ods_htl_groupwormholedb.bnb_space where statusid = 2 and d = '2018-06-14' ) a
join 
(select spaceid from ods_htl_groupwormholedb.bnb_space_source where vendorid = 201 and d = '2018-06-14') b
on a.spaceid = b.spaceid
join
(select cityid,spaceid from ods_htl_groupwormholedb.bnb_space_address where d = '2018-06-14') c
on b.spaceid = c.spaceid
join
(select countryname,cityid from ods_htl_groupwormholedb.bnb_city_data 
 where d = '2018-06-14'
and countryname like '%日本%' ) d1
on c.cityid = d1.cityid