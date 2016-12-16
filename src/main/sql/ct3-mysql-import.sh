#!/bin/bash

function catdata { cat ct3/*.tsv.gz | gzip -c -d; }

dockername=ctmysql
mysqldatadir=$(pwd)/ct3-mysql
targetport=3307

# start a mysql docker container, name it
docker run -p $targetport:3306 --name $dockername -v $mysqldata:/var/lib/mysql -e LC_ALL=C.UTF-8 -e MYSQL_ROOT_PASSWORD=root -d mysql:5.7

# DB definition
sql_db_schema="
-- create and use the database
CREATE DATABASE IF NOT EXISTS ct DEFAULT CHARACTER SET binary;
USE ct;

-- create the table
CREATE TABLE ct3 ( 
	a    varchar(128) NOT NULL DEFAULT '',  
	b    varchar(128) NOT NULL DEFAULT '',
	c    varchar(128) NOT NULL DEFAULT '',
	nabc double unsigned NOT NULL DEFAULT '0',
	nab  double unsigned NOT NULL DEFAULT '0',
	nac  double unsigned NOT NULL DEFAULT '0',
	nbc  double unsigned NOT NULL DEFAULT '0',
	na   double unsigned NOT NULL DEFAULT '0',
	nb   double unsigned NOT NULL DEFAULT '0',
	nc   double unsigned NOT NULL DEFAULT '0',
	n    double unsigned NOT NULL DEFAULT '0',
	oab  double unsigned NOT NULL DEFAULT '0',
	oac  double unsigned NOT NULL DEFAULT '0',
	obc  double unsigned NOT NULL DEFAULT '0',
	oa   double unsigned NOT NULL DEFAULT '0',
	ob   double unsigned NOT NULL DEFAULT '0',
	oc   double unsigned NOT NULL DEFAULT '0',
	o    double unsigned NOT NULL DEFAULT '0'
) ENGINE=MyISAM;

-- load the data (do smtg like mkfifo /var/lib/mysql-files/tmp and cat dir/p* > /var/lib/mysql-files/tmp before)
SELECT 'loading data...';
LOAD DATA INFILE '/var/lib/mysql-files/tmp' INTO TABLE ct3;

-- remove wrongly imported data
SELECT 'removing corrupted data...';
DELETE FROM ct3
WHERE 
	a LIKE '' OR
	b LIKE '' OR
	c LIKE '' OR
	nabc <= 0 OR
	nab  <= 0 OR
	nac  <= 0 OR
	nbc  <= 0 OR
	na   <= 0 OR
	nb   <= 0 OR
	nc   <= 0 OR
	n    <= 0 OR
	oab  <= 0 OR
	oac  <= 0 OR
	obc  <= 0 OR
	oa   <= 0 OR
	ob   <= 0 OR
	oc   <= 0 OR
	o <= 0;

-- add index
SELECT 'activating index...';
ALTER TABLE ct3 ADD KEY (a), ADD KEY (b), ADD KEY (c);
"

# import data from $dir into mysql

export LC_ALL=C.UTF-8 
docker exec $dockername sh -c 'mkfifo /var/lib/mysql-files/tmp'
catdata | docker exec -i $dockername sh -c 'cat - > /var/lib/mysql-files/tmp' &
echo "$sql_db_schema" | docker exec -i $dockername sh -a -c 'cat - | mysql -proot'
docker exec $dockername sh -c 'rm /var/lib/mysql-files/tmp'

# docker exec -ti $dockername bash
# docker exec -ti $dockername mysql -BNe "select * from ct3 limit 10;" ct
# docker exec -ti $dockername mysql -BNe "select * from ct3 limit 10;" ct



