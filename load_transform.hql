set hive.execution.engine=spark;
INSERT OVERWRITE TABLE retail_transform.retail
SELECT
case when invoiceno = '' then '' else cast(invoiceno as string) end as invoiceno,
case when stockcode = '' then '' else cast(stockcode as string) end as stockcode,
case when description = '' then '' else cast(description as string) end as description,
case when quantity = '' then '' else cast(quantity as bigint) end as quantity,
case when invoicedate = '' then NULL else from_unixtime(unix_timestamp(invoicedate, 'dd-MM-yyyy' )) end as invoicedate,
case when unitprice = '' then '' else cast(unitprice as decimal) end as unitprice,
case when customerid = '' then '' else cast(customerid as bigint) end as customerid,
case when country = '' then '' else cast(country as string) end as country
from raw.retail;
MSCK REPAIR TABLE retail_transform.retail;
