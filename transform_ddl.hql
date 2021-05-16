drop table retail_transform.retail;
drop database retail_transform;
create database retail_transform;
drop table if exists retail_transform.retail;
CREATE EXTERNAL TABLE retail_transform.retail(invoiceno string, stockcode string, description string, quantity bigint, invoicedate timestamp, unitprice decimal, customerid bigint, country string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile tblproperties('serialization.null.format'='','skip.header.line.count'='1');
