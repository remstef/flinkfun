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


-- get a similarity value between a1 and a2
DROP PROCEDURE IF EXISTS getSimilarityProb;
DELIMITER //
CREATE PROCEDURE getSimilarityProb
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
call getSimilarityProb('mouse','animal', 1000, 10);
call getSimilarityProb('mouse','clock', 1000, 10);
call getSimilarityProb('mouse','water', 10, 10);
call getSimilarityProb('water','mouse', 10, 10);
call getSimilarityProb('easy','tough', 1000, 1000);
call getSimilarityProb('formal','proper', 100, 100);

-- define a procedure to quickly get similar entries for a
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
CREATE PROCEDURE getSimilarity
(IN a1input VARCHAR(128), IN a2input VARCHAR(128), IN max_ob INT, IN topn INT, IN lim INT)
BEGIN
  SELECT c1.a, c2.a, COUNT(c1.b) as cnt, SUM(c1.nab) as sumnab_1, SUM(c2.nab) as sumnab_2, SUM(c1.sig) as sumsig
  FROM (
    SELECT t.a, t.b, t.nab, t.sig 
    FROM ct2v t 
    WHERE
      t.ob  >= 2 AND
      t.a = a1input AND
      t.ob  <= max_ob
    ORDER BY t.sig 
    DESC LIMIT topn
  ) c1  
  INNER JOIN ct2 c2 
  ON (c1.b = c2.b AND c2.a = a2input) 
  GROUP BY c1.a, c2.a
  ORDER BY cnt DESC
  LIMIT lim;
END //
DELIMITER ;

call getSimilarity('mouse', 'animal', 1000, 1000, 200);
call getSimilarity('mouse', 'keyboard', 1000, 1000, 200);
call getSimilarity('mouse', 'clock', 1000, 1000, 200);
call getSimilarity('mouse', 'water', 100, 10, 200);

-- define a procedure to quickly get similar entries for 'a'
DROP PROCEDURE IF EXISTS getSimilarAplain;
DELIMITER //
CREATE PROCEDURE getSimilarAplain
(IN inputquery VARCHAR(128), IN lim INT)
BEGIN
  SELECT c1.a, c2.a, COUNT(c1.b) as c, COUNT(c1.nab) as s1, COUNT(c2.nab) as s2 
  FROM (SELECT * FROM ct2 WHERE a = inputquery) c1  
  INNER JOIN ct2 c2 
  ON (c1.b = c2.b) 
  GROUP BY c1.a, c2.a
  ORDER BY c DESC
  LIMIT lim;
END //
DELIMITER ;

call getSimilarAplain('Tom', 200);

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

select * from ct2p limit 10;

-- select 10 random rows
SELECT * FROM ct2 ORDER BY RAND() LIMIT 10;

-- check for duplicates
select group_concat(b), count(*) as count from ct2 where a = 'cat' group by a,b having count >= 2;

-- get dt entries for 'cat' with pruned contexts (select top 100 by sig)
SELECT c1.a, c2.a, count(c1.b) as c, sum(c1.nab) as s1, sum(c2.nab) as s2 FROM (SELECT * from ctv t WHERE t.a = 'cat' ORDER BY t.sig DESC LIMIT 100) c1  INNER JOIN ctv c2 ON (c1.b = c2.b) group by c1.a, c2.a  ORDER BY c DESC LIMIT 10;

-- join cts from a1 and a2 by joint contexts and add rank to query result
select @r:=@r+1 as r, c1.b
from 
	(SELECT @r := 0) t,
	ct2 c1
	join ct2 c2
	on(c1.b = c2.b AND c1.a = 'mouse' AND c2.a = 'keyboard')
limit 100;

-- get contexts of a1 and join by b with cts from a2
select * -- c1.a, c2.a, c1.b, c1.p_AgivenB, c1.p_BgivenA, c2.p_AgivenB, c2.p_BgivenA, EXP(LOG(c2.p_AgivenB) + LOG(c1.p_BgivenA)) as p_mult
from 
	(select * from ct2p where a='mouse' and ob <= 1000) c1 
	left outer join 
	(select * from ct2p where a = 'keyboard') c2 
	on (c1.b = c2.b)
limit 50;






