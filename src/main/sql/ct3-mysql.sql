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

-- get a similarity value between a1 and a2
DROP PROCEDURE IF EXISTS getSimilarityProb3;
DELIMITER //
CREATE PROCEDURE getSimilarityProb3
(IN a1input VARCHAR(128), IN a2input VARCHAR(128), IN max_ob INT, IN maxcontexts INT)
BEGIN
  -- DECLARE oa_a2 INT;
  SET @oa_a2 = (SELECT oa FROM ct2 WHERE a=a2input LIMIT 1);
  -- get contexts of a1 and compute KN-backoff probabilities when joined with a2 and p_AgivenB is NULL
  select a1input, a2input, count(c1.b) as shared_contexts,
    sum(coalesce(
      EXP( LOG(c2.p_AgivenB) + LOG(c1.p_BgivenA) ), /* <-- if not null, otherwise  */
      EXP( LOG(@D)+LOG(c1.ob)- LOG(c1.nb)+LOG(@oa_a2) -LOG(c1.o) /*<-- kn backoff*/  + LOG(c1.p_BgivenA)/* <-- p(c|a) */ ) /* otherwise */
    )) as p_A2givenA1 
    from 
	  (select * from ct2p where a=a1input and ob <= max_ob limit maxcontexts) c1 
	  left outer join 
	    (select * from ct2p where a = a2input) c2 
	  on (c1.b = c2.b);
END //
DELIMITER ;


call getSimilarityProb('mouse','keyboard', 1000, 10);

/**
****
*****
***** SOME SELECTED EXAMPLE SQL QUERIES
*****
****
**/

-- get contexts of a1 and join by (b,c) with cts from a2
select *
from 
	(select * from ct2p where a='mouse' and ob <= 1000) c1 
	left outer join 
	(select * from ct2p where a = 'keyboard') c2 
	on (c1.b = c2.b)
;


