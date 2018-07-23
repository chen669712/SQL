SELECT
confirm.vendorId as 'vendorId',
confirm.productId AS 'productId',
confirm.ownerid AS 'landlordUId',
count(1) As 'confirmCount',
if(avg(confirm.confirmMinutes) <= 0,0.1,avg(confirm.confirmMinutes)) AS 'avgConfirmMinutes'
FROM(
SELECT item.productId,
item.vendorId,
space.ownerid,
TIME_TO_SEC(TIMEDIFF(log.createtime,
CASE WHEN item.vendorid IN ( 100, 104, 109, 113, 114 ) THEN header.createdtime ELSE pay_log.createtime END)) / 60 AS 'confirmMinutes'
FROM ( select createtime,orderid from order_log 
		where log.operateType = 'PROCESS' 
		AND log.result = 0 
		AND (log.subOperateType = 'OWNER_ACCEPT' 
		OR log.subOperateType = 'OWNER_DENY'))log 
INNER JOIN (select createdtime,orderid from order_header)  header ON log.orderid = header.orderid
INNER JOIN (select productId,vendorId from order_item) item ON log.orderid = item.orderid
INNER JOIN ( select ownerid,checkIn from order_item_space where space.ownerId IS NOT NULL
			) space 
ON item.orderitemid = space.orderitemid
INNER JOIN （ select orderid,createtime from order_log 
				where pay_log.operateType = 'PAY' 
				AND pay_log.subOperateType = 'PAYED' 
				AND pay_log.operateChannel = 'CTRIP_PAYMENT_PLATFORM' 
				and pay_log.result = 0 ）pay_log 
ON log.orderid = pay_log.orderid 
WHERE  (date(if(item.vendorid IN ( 100, 104, 109, 113, 114 ), header.createdtime, pay_log.createtime)) = space.checkIn OR 
(TIME(if(item.vendorid IN ( 100, 104, 109, 113, 114 ), header.createdtime, pay_log.createtime)) > '09:00:00' 
AND TIME(if(item.vendorid IN ( 100, 104, 109, 113, 114 ), header.createdtime, pay_log.createtime))<'22:00:00'))
AND log.createtime BETWEEN '2018-05-22'  AND '2018-06-22' GROUP BY header.orderid) AS confirm
GROUP BY confirm.productId, confirm.vendorId;  



SELECT
confirm.vendorId as 'vendorId',
confirm.productId AS 'productId',
confirm.ownerid AS 'landlordUId',
count(1) As 'confirmCount',
if(avg(confirm.confirmMinutes)   <=   0, 0.1, avg(confirm.confirmMinutes)) AS 'avgConfirmMinutes'
FROM(
SELECT item.productId,
item.vendorId,
space.ownerid,
TIME_TO_SEC(TIMEDIFF(log.createtime,
CASE WHEN item.vendorid IN ( 100, 104, 109, 113, 114 ) THEN header.createdtime ELSE pay_log.createtime END)) / 60 AS 'confirmMinutes'
FROM order_log log 
INNER JOIN order_header  header ON log.orderid = header.orderid
INNER JOIN order_item  item ON log.orderid = item.orderid
INNER JOIN order_item_space  space ON item.orderitemid = space.orderitemid
INNER JOIN order_log  pay_log ON (log.orderid = pay_log.orderid AND pay_log.operateType = 'PAY' AND pay_log.subOperateType = 'PAYED' AND pay_log.operateChannel = 'CTRIP_PAYMENT_PLATFORM' and pay_log.result = 0)
WHERE log.operateType = 'PROCESS' AND log.result = 0 AND (log.subOperateType = 'OWNER_ACCEPT' OR log.subOperateType = 'OWNER_DENY') 
AND space.ownerId IS NOT NULL 
AND (date(if(item.vendorid IN ( 100, 104, 109, 113, 114 ), header.createdtime, pay_log.createtime)) = space.checkIn OR 
(TIME(if(item.vendorid IN ( 100, 104, 109, 113, 114 ), header.createdtime, pay_log.createtime)) > '09:00:00' 
AND TIME(if(item.vendorid IN ( 100, 104, 109, 113, 114 ), header.createdtime, pay_log.createtime))<'22:00:00'))
AND log.createtime BETWEEN '2018-05-22'  AND '2018-06-22' GROUP BY header.orderid) AS confirm
GROUP BY confirm.productId, confirm.vendorId;  
