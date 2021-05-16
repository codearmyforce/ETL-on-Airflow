drop table raw.retail;
drop database raw;
create database raw;
drop table if exists raw.retail;
CREATE EXTERNAL TABLE raw.retail(invoiceno string, stockcode string, description string, quantity string, invoicedate string, unitprice string, customerid string, country string)
row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile tblproperties('serialization.null.format'='','skip.header.line.count'='1');
