drop table retail_publish.retail;
drop database retail_publish;
create database retail_publish;
drop table if exists retail_publish.retail;
CREATE EXTERNAL TABLE retail_publish.retail(invoiceno string, stockcode string, description string, quantity bigint, invoicedate timestamp, unitprice decimal, customerid bigint, country string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile tblproperties('serialization.null.format'='','skip.header.line.count'='1');

