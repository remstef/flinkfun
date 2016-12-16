#!/bin/bash

dockername=ctmysql
mysqldatadir=$(pwd)/ct-mysql
targetport=3308

function catdata { cat ct2/p*.csv; }

# start a mysql docker container, name it 
docker run -p $targetport:3306 --name $dockername -v $mysqldatadir:/var/lib/mysql -e LC_ALL=C.UTF-8 -e MYSQL_ROOT_PASSWORD=root -d mysql:5.7

# DB definition
sql_db_schema="
-- create and use the database
CREATE DATABASE IF NOT EXISTS ct DEFAULT CHARACTER SET binary;
USE ct;

-- create the table
CREATE TABLE ct2 ( 
	a   varchar(128) NOT NULL DEFAULT '',  
	b   varchar(128) NOT NULL DEFAULT '',  
	nab double unsigned NOT NULL DEFAULT '0',
	na  double unsigned NOT NULL DEFAULT '0',  
	nb  double unsigned NOT NULL DEFAULT '0',
	n   double unsigned NOT NULL DEFAULT '0',
	oa  double unsigned NOT NULL DEFAULT '0',
	ob  double unsigned NOT NULL DEFAULT '0',  
	o   double unsigned NOT NULL DEFAULT '0'
) ENGINE=MyISAM;

-- define a similarity function (currently lmi)
CREATE FUNCTION simfun
(nab DOUBLE unsigned, na DOUBLE unsigned, nb DOUBLE unsigned, n DOUBLE unsigned, oa DOUBLE unsigned, ob DOUBLE unsigned, o DOUBLE unsigned)
RETURNS DOUBLE
RETURN nab * (log(nab) + log(n) - log(na) - log(nb));

-- create a view with the significance value and pruned fields 
CREATE VIEW ct2v AS SELECT a, b, nab, na, nb, n, oa, ob, o, simfun(nab, na, nb, n, oa, ob, o) as sig
FROM ct2
WHERE
	nab >= 2 AND
	na  >= 2 AND
	oa  >= 2 AND
	nb  >= 2 AND
	ob  >= 2 AND
	ob  <= 1000 AND
	simfun(nab, na, nb, n, oa, ob, o) > 0;

-- define a procedure to quickly get similar entries for 'a'
DELIMITER //
CREATE PROCEDURE getsimilarA
(IN inputquery VARCHAR(128), IN topn INT, IN lim INT)
BEGIN
  SELECT c1.a, c2.a, COUNT(c1.b) as c, COUNT(c1.nab) as s1, COUNT(c2.nab) as s2 
  FROM (
  	SELECT t.a, t.b, t.nab, t.sig 
  	FROM ct2v t 
  	WHERE t.a = inputquery
  	ORDER BY t.sig 
  	DESC LIMIT topn
  	) c1  
  INNER JOIN ct2v c2 
  ON (c1.b = c2.b) 
  GROUP BY c1.a, c2.a
  ORDER BY c DESC
  LIMIT lim;
END //
DELIMITER ;

-- define a procedure to quickly get similar entries for 'a'
DELIMITER //
CREATE PROCEDURE getsimilarB
(IN inputquery VARCHAR(128), IN topn INT, IN lim INT)
BEGIN
  SELECT c1.b, c2.b, COUNT(c1.a) as c, COUNT(c1.nab) as s1, COUNT(c2.nab) as s2 
  FROM (
  	SELECT t.b, t.a, t.nab, t.sig 
  	FROM ctv t 
  	WHERE t.b = inputquery 
  	ORDER BY t.sig 
  	DESC LIMIT topn
  	) c1
  INNER JOIN ctv c2 
  ON (c1.a = c2.a) 
  GROUP BY c1.b, c2.b
  ORDER BY c DESC
  LIMIT lim;
END //
DELIMITER ;

-- load the data (do smtg like mkfifo /var/lib/mysql-files/tmp and cat dir/p* > /var/lib/mysql-files/tmp before)
SELECT 'loading data...';
LOAD DATA INFILE '/var/lib/mysql-files/tmp' INTO TABLE ct2;

-- remove wrongly imported data
SELECT 'removing corrupted data...';
DELETE FROM ct2
WHERE 
	a LIKE '' OR
	b LIKE '' OR
	nab <= 0 OR
	na <= 0 OR
	nb <= 0 OR
	n <= 0 OR
	oa <= 0 OR
	ob <= 0 OR
	o <= 0;

-- add index
SELECT 'activating index...';
ALTER TABLE ct2 ADD KEY (a), ADD KEY (b);
"

# import data from $dir into mysql

export LC_ALL=C.UTF-8 
docker exec $dockername sh -c 'mkfifo /var/lib/mysql-files/tmp'
catdata |  docker exec -i $dockername sh -c 'cat - > /var/lib/mysql-files/tmp' &
echo "$sql_db_schema" | docker exec -i $dockername sh -a -c 'cat - | mysql -proot'
docker exec $dockername sh -c 'rm /var/lib/mysql-files/tmp'



