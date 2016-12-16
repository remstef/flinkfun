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
CREATE FUNCTION simfun(nab DOUBLE unsigned, na DOUBLE unsigned, nb DOUBLE unsigned, n DOUBLE unsigned, oa DOUBLE unsigned, ob DOUBLE unsigned, o DOUBLE unsigned) RETURNS DOUBLE
RETURN p_BgivenA(nab, na, nb, n, oa, ob, o);
-- RETURN lmi(nab, na, nb, n, oa, ob, o);

-- lmi
CREATE FUNCTION lmi(nab DOUBLE unsigned, na DOUBLE unsigned, nb DOUBLE unsigned, n DOUBLE unsigned, oa DOUBLE unsigned, ob DOUBLE unsigned, o DOUBLE unsigned) RETURNS DOUBLE
RETURN nab * (log(nab) + log(n) - log(na) - log(nb));

-- y + log(exp(x-y)+1)
CREATE FUNCTION sum_log_prob(lnx DOUBLE, lny DOUBLE) RETURNS DOUBLE
RETURN lny + LOG(EXP(lnx-lny)+1);

-- kneser ney probability (set discount factor 0 < D <= 1 ): select p_a_given_b_KN(10,100,100,1000,20,20,30);
SET @D=0.5;
CREATE FUNCTION p_AgivenB_KN(nab DOUBLE unsigned, na DOUBLE unsigned, nb DOUBLE unsigned, n DOUBLE unsigned, oa DOUBLE unsigned, ob DOUBLE unsigned, o DOUBLE unsigned) RETURNS DOUBLE
RETURN EXP( LOG(nab-@D)-LOG(nb) ) + EXP( LOG(@D)+LOG(ob)-LOG(nb)+LOG(oa)-LOG(o) );

-- standard conditional probability
CREATE FUNCTION p_AgivenB(nab DOUBLE unsigned, na DOUBLE unsigned, nb DOUBLE unsigned, n DOUBLE unsigned, oa DOUBLE unsigned, ob DOUBLE unsigned, o DOUBLE unsigned) RETURNS DOUBLE
RETURN EXP( LOG(nab)-LOG(nb) );

-- standard conditional probability
CREATE FUNCTION p_BgivenA(nab DOUBLE unsigned, na DOUBLE unsigned, nb DOUBLE unsigned, n DOUBLE unsigned, oa DOUBLE unsigned, ob DOUBLE unsigned, o DOUBLE unsigned) RETURNS DOUBLE
RETURN EXP( LOG(nab)-LOG(na) );


-- create a view with the significance value and pruned fields 
CREATE VIEW ct2v AS SELECT a, b, nab, na, nb, n, oa, ob, o, simfun(nab, na, nb, n, oa, ob, o) as sig
FROM ct2;

-- create a view with the probability values
CREATE VIEW ct2p AS SELECT a, b, nab, na, nb, n, oa, ob, o, p_AgivenB_KN(nab, na, nb, n, oa, ob, o) AS p_AgivenB, p_BgivenA(nab, na, nb, n, oa, ob, o) AS p_BgivenA
FROM ct2;

-- define a procedure to quickly get similar entries for 'a' by Melamuds proability method
DELIMITER //
CREATE PROCEDURE getSimilarAprob
(IN inputquery VARCHAR(128), IN max_ob INT, IN topn INT, IN lim INT)
BEGIN
  SELECT c1.a as a1, c2.a as a2, COUNT(c1.b) as cnt, SUM(c1.nab) as sumnab_1, SUM(c2.nab) as sumnab_2, SUM(c1.p_mult) as psim_A2givenA1
  FROM (
    SELECT t.a, t.b, t.nab, t.p_AgivenB, t.p_BgivenA, EXP(LOG(t.p_AgivenB) + LOG(t.p_BgivenA)) AS p_mult
    FROM ct2p t
    WHERE 
      t.ob >= 2 AND
      t.a = inputquery
  ) c1  
  INNER JOIN ct2 c2 
  ON (c1.b = c2.b) 
  GROUP BY c1.a, c2.a
  ORDER BY psim_A2givenA1 DESC
  LIMIT lim;
END //
DELIMITER ;

-- define a procedure to quickly get similar entries for 'a'
DELIMITER //
CREATE PROCEDURE getSimilarA
(IN inputquery VARCHAR(128), IN max_ob INT, IN topn INT, IN lim INT)
BEGIN
  SELECT c1.a, c2.a, COUNT(c1.b) as cnt, SUM(c1.nab) as sumnab_1, SUM(c2.nab) as sumnab_2, SUM(c1.sig) as sumsig
  FROM (
    SELECT t.a, t.b, t.nab, t.sig 
    FROM ct2v t 
    WHERE
      t.ob  >= 2 AND
      t.a = inputquery AND
      t.ob  <= max_ob
    ORDER BY t.sig 
    DESC LIMIT topn
  ) c1  
  INNER JOIN ct2 c2 
  ON (c1.b = c2.b) 
  GROUP BY c1.a, c2.a
  ORDER BY cnt DESC
  LIMIT lim;
END //
DELIMITER ;

-- define a procedure to quickly get similar entries for 'a'
DELIMITER //
CREATE PROCEDURE getSimilarAplain
(IN inputquery VARCHAR(128), IN lim INT)
BEGIN
  SELECT c1.a, c2.a, COUNT(c1.b) as c, COUNT(c1.nab) as s1, COUNT(c2.nab) as s2 
  FROM ct2 c1  
  INNER JOIN ct2 c2 
  ON (c1.b = c2.b) 
  WHERE c1.a = inputquery
  GROUP BY c1.a, c2.a
  ORDER BY c DESC
  LIMIT lim;
END //
DELIMITER ;

-- define a procedure to quickly get similar entries for 'a'
DELIMITER //
CREATE PROCEDURE getSimilarB
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
  INNER JOIN ct2 c2 
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

/**
****
*****
***** SOME SELECTED EXAMPLE SQL QUERIES
*****
****
**/
-- get dt entries for A = 'cat' and B = 'PER::know::@' (unpruned)
SELECT c1.a, c2.a, count(c1.b) as c, sum(c1.nab) as s1, sum(c2.nab) as s2 FROM ct2v c1 INNER JOIN ct2v c2 ON (c1.b = c2.b) WHERE c1.a = 'cat' group by c1.a, c2.a order by c desc limit 10;
SELECT c1.b, c2.b, count(c1.a) as c, sum(c1.nab) as s1, sum(c2.nab) as s2 FROM ct2v c1 INNER JOIN ct2v c2 ON (c1.a = c2.a) WHERE c1.b = 'PER::know::@' group by c1.b, c2.b order by c desc limit 10;

-- select 10 random rows
SELECT * FROM ct2 ORDER BY RAND() LIMIT 10;

-- check for duplicates
select group_concat(b), count(*) as count from ct2 where a = 'cat' group by a,b having count >= 2;



-- get dt entries for 'cat' with pruned contexts (select top 100 by sig)
-- SELECT c1.a, c2.a, count(c1.b) as c, sum(c1.nab) as s1, sum(c2.nab) as s2 FROM (SELECT * from ctv t WHERE t.a = 'cat' ORDER BY t.sig DESC LIMIT 100) c1  INNER JOIN ctv c2 ON (c1.b = c2.b) group by c1.a, c2.a  ORDER BY c DESC LIMIT 10;


