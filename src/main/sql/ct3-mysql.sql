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
SELECT 'loading data...';

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
ALTER TABLE ct3 ADD KEY a (a), ADD KEY b (b), ADD KEY c (c), ADD KEY ab (a,b), ADD KEY ac (a,c), ADD KEY abc (a,b,c);
SELECT 'Indexing finished.';

-- create a view with the probability values
DROP VIEW IF EXISTS ct3p;
CREATE VIEW ct3p AS SELECT 
  a, b, c, 
  
  pXgivenY_lg(nabc, nbc) as pAgivenBC_lg, 
  pXgivenY_lg(nabc, nb) as  pAgivenB_lg, 
  pXgivenY_lg(nabc, nc) as  pAgivenC_lg, 
  
  pXgivenY_lg(nabc, nbc) as pBgivenAC_lg, 
  pXgivenY_lg(nabc, na) as  pBgivenA_lg, 
  pXgivenY_lg(nabc, nc) as  pBgivenC_lg, 

  pXgivenY_lg(nabc, nbc) as pCgivenAB_lg,
  pXgivenY_lg(nabc, na) as  pCgivenA_lg, 
  pXgivenY_lg(nabc, nb) as  pCgivenB_lg

FROM ct3;

-- p(X|Y) = p(X,Y)/P(Y) = n(xy)/n(y)
DROP FUNCTION IF EXISTS pXgivenY_lg;
CREATE FUNCTION pXgivenY_lg(nxy DOUBLE unsigned, ny DOUBLE unsigned) RETURNS DOUBLE
RETURN log(nxy) - log(ny);

-- p(c1c2|w1)
drop function if exists plog_CgvnW1;
DELIMITER //
create function plog_CgvnW1 (
  nc1c2w1 double unsigned, nw1 double unsigned, 
  nc1w1 double unsigned, nc2w1 double unsigned)
returns double
BEGIN
  return /*p(c1c2|w1)*/ log(nc1c2w1) - log(nw1);
END //
DELIMITER ;

-- p(w2|c1c2)
drop function if exists plog_W2gvnC;
DELIMITER //
CREATE FUNCTION plog_W2gvnC(
  w2 varchar(128), c1 varchar(128), c2 varchar(128)
  ) RETURNS double
BEGIN
  declare nw2c1c2, nc1c2, nw2c1, nw2c2, nw2, nc1, nc2, n_ double default NULL;
  set @log_counter_lambda_mu = log(1 - exp(@log_lambda) - exp(@log_mu));
  -- check (w2,c1,c2) and compute (p(w2|c1c2)p(w2|c1)p(w2|c2)) / 3
  select nabc, nbc, oab, oac, ob, oc, oa, o into nw2c1c2, nc1c2, nw2c1, nw2c2, nc1, nc2, nw2, n_ from ct3 where a=w2 and b=c1 and c=c2 limit 1; -- limit 1 should not be necessary
  if nw2c1c2 is not null then
    return log(exp(/*p(w2|c1c2)/3*/ log(nw2c1c2) - log(nc1c2) + @log_counter_lambda_mu) + exp(/*p(w2|c1)/3*/ log(nw2c1) - log(nc1) + @log_lambda-log(2)) + exp(/*p(w2|c2)/3*/log(nw2c2)-log(nc2) + @log_lambda-log(2)) + exp(/*p(w2)*/ log(nw2) - log(n_) + @log_mu) ) ;
  end if;
  -- check (w2,c1) and compute p(w2|c1) / 3
  select oab, ob, oa, o into nw2c1, nc1, nw2, n_ from ct3 where a=w2 and b=c1 limit 1;
  if nw2c1 is not null then -- compute  (p(w2|c1c2)p(w2|c1)p(w2|c2)) / 3
    return log(exp(/*p(w|c1)*/ log(nw2c1) - log(nc1) + @log_lambda-log(2)) + exp(/*p(w2)*/ log(nw2) - log(n_) + @log_mu) ) ;
  end if;
  -- check (w2,c2) and compute p(w2|c2) / 3
  select oac, oc, oa, o into nw2c2, nc2, nw2, n_ from ct3 where a=w2 and c=c2 limit 1;
  if nw2c2 is not null then -- compute  p(w2|c2) / 3
    return log(exp(/*p(w|c2)*/ log(nw2c2) - log(nc2) + @log_lambda-log(2)) + exp(/*p(w2)*/ log(nw2) - log(n_) + @log_mu) ) ;
  end if;
  -- check w2 and compute p(w2)
  select oa, o into nw2, n_ from ct3 where a=w2 limit 1;
  return /*p(w2)*/ log(nw2) - log(n_) + @log_mu;
END //
DELIMITER ;



-- get a similarity value between a1 and a2
DROP FUNCTION IF EXISTS getSimilarityProb3;
DELIMITER //
CREATE FUNCTION getSimilarityProb3
(w1 VARCHAR(128), w2 VARCHAR(128), max_ob INT, maxcontexts INT) RETURNS DOUBLE
BEGIN
  declare psim double default 0;
  select sum(exp(plog_w1 + plog_w2)) into sim from (select plog_CgvnW1(nabc, na, nab, nac) as plog_w1, plog_W2gvnC(w2, b, c) as plog_w2 from ct3 where a=w1 and ob <= max_ob limit maxcontexts) t where plog_w2 != 0;
  return psim;
END //
DELIMITER ;

-- get a similarity value between a1 and a2
DROP FUNCTION IF EXISTS getSimilarityP3;
DELIMITER //
CREATE FUNCTION getSimilarityP3
(w1 VARCHAR(128), w2 VARCHAR(128), maxcontexts INT) RETURNS DOUBLE
BEGIN
  declare psim double default 0;
  select sum(exp(plog_w1 + plog_w2)) into psim from (select plog_CgvnW1(nabc, na, nab, nac) as plog_w1, plog_W2gvnC(w2, b, c) as plog_w2 from ct3 where a=w1 order by nabc limit maxcontexts) t where plog_w2 != 0;
  return psim;
END //
DELIMITER ;

call getSimilarityProb3('chase','pursue', 1000, 10);
call getSimilarityProb3('pursue','chase', 1000, 10);
call getSimilarityProb3('god','prey', 1000, 10000);
call getSimilarityProb3('prey','god', 1000, 10000);
call getSimilarityProb3('pursue','pursue', 10000, 10000);
call getSimilarityProb3('chase','chase', 10000, 10000);
call getSimilarityProb3('buy','acquire', 10000, 10000);

select sum(exp(plog_w1 + plog_w2)) from (select plog_CgvnW1(nabc, na, nab, nac) as plog_w1, plog_W2gvnC('pursue', b, c) as plog_w2 from ct3 where a='chase' and ob <= 1000 limit 10) t where plog_w2 != 0;

select *, exp(plog_w1 + plog_w2) as p from (
  select *, plog_CgvnW1(nabc,na,nab,nac) as plog_w1, plog_W2gvnC('god', b, c) as plog_w2 from ct3 where a='prey' and ob <= 10000 limit 10
) t;

select * from ct3 limit 1;



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
	(select * from ct3 where a='read') c1 
	left outer join 
	(select * from ct3 where a='write') c2 
	on (c1.b = c2.b)
;


(SELECT * FROM ct3 LIMIT 2) UNION (SELECT * FROM ct3 LIMIT 2);

(select a,b,c,nabc,nab,nac,nbc,na,nb,nc,n,oab,oac,obc,oa,ob,oc,o,"a" as pos from ct3 limit 3) union (select b,a,c,nabc,nab,nac,nbc,na,nb,nc,n,oab,oac,obc,oa,ob,oc,o,"b" as pos from ct3 limit 3);


select * from (
	(select a as w, concat("@::",b,"::",c) as c12 from ct3) 
	union 
	(select b as w, concat(a,"::@::",c) as c12 from ct3) 
  union
  (select c as w, concat(a,"::",b,"::@") as c12 from ct3)
) t
where w = 'talk'
limit 100;


CREATE TABLE ct3asct2 
  (select a as a, concat("@::",b,"::",c) as b, nabc as nab, na as na, nbc as nb, n as n, oa as oa, obc as ob, o as o, "a" as asrc, "bc" as bsrc from ct3) 
  union
  (select a as a, concat("@::",b,"::*")  as b, nabc as nab, na as na, nb  as nb, n as n, oa as oa, ob  as ob, o as o, "a" as asrc, "b"  as bsrc from ct3) 
	union
  (select a as a, concat("@::*::",c)     as b, nabc as nab, na as na, nc  as nb, n as n, oa as oa, oc  as ob, o as o, "a" as asrc, "c"  as bsrc from ct3) 

  union
	(select b as a, concat(a,"::@::",c)    as b, nabc as nab, nb as na, nac as nb, n as n, ob as oa, oac as ob, o as o, "b" as asrc, "a,c" as bsrc from ct3) 
  union
  (select b as a, concat("*::@::",c)     as b, nabc as nab, nb as na, nc  as nb, n as n, ob as oa, ob  as ob, o as o, "b" as asrc, "c"   as bsrc from ct3) 
	union
  (select b as a, concat(a,"::@::*")     as b, nabc as nab, nb as na, na  as nb, n as n, ob as oa, oc  as ob, o as o, "b" as asrc, "a"   as bsrc from ct3) 
  
  union
  (select c as a, concat(a,"::",b,"::@") as c, nabc as nab, nc as na, nab as nb, n as n, oc as oa, oac as ob, o as o, "c" as asrc, "a,b" as bsrc from ct3) 
  union
  (select c as a, concat("*::",b,"::@")  as c, nabc as nab, nc as na, nb  as nb, n as n, oc as oa, ob  as ob, o as o, "c" as asrc, "b"   as bsrc from ct3) 
	union
  (select c as a, concat(a,"::*::@")     as c, nabc as nab, nc as na, na  as nb, n as n, oc as oa, oa  as ob, o as o, "c" as asrc, "a"   as bsrc from ct3) 
;

drop function if exists plog_CgvnW1_DEBUG;
DELIMITER //
CREATE FUNCTION plog_CgvnW1_DEBUG(
  nc1c2w1 double unsigned, nw1 double unsigned, 
  nc1w1 double unsigned, nc2w1 double unsigned) RETURNS varbinary(128)
BEGIN
  return concat(
    "p(c1c2|w1) = ", 
    log(nc1c2w1) - log(nw1)
  );
END //
DELIMITER ;


drop function if exists plog_W2gvnC_DEBUG;
DELIMITER //
CREATE FUNCTION plog_W2gvnC_DEBUG(
  w2 varchar(128), c1 varchar(128), c2 varchar(128)
  ) RETURNS varbinary(128)
BEGIN
  declare nw2c1c2, nc1c2, nw2c1, nw2c2, nw2, nc1, nc2, n_ double default NULL;
  set @log_counter_lambda_mu = log(1 - exp(@log_lambda) - exp(@log_mu));
  -- check (w2,c1,c2) and compute (p(w2|c1c2)p(w2|c1)p(w2|c2))
  select nabc, nbc, nab, nac, nb, nc, na, n into nw2c1c2, nc1c2, nw2c1, nw2c2, nc1, nc2, nw2, n_ from ct3 where a=w2 and b=c1 and c=c2 limit 1; -- limit 1 should not be necessary
  if nw2c1c2 is not null then
    return concat(
      "(1-lambda-mu)p(w2|c1c2)+(lambda/2)p(w2|c1)+(lambda/2)p(w2|c2)+(mu)p(w2)",
      log(exp(/*p(w2|c1c2)*/ log(nw2c1c2) - log(nc1c2) + @log_counter_lambda_mu) + exp(/*p(w2|c1)/3*/ log(nw2c1) - log(nc1) + @log_lambda-log(2)) + exp(/*p(w2|c2)/3*/log(nw2c2)-log(nc2) + @log_lambda-log(2)) + exp(/*p(w2)*/ log(nw2) - log(n_) + @log_mu) )
    );
  end if;
  -- check (w2,c1) and compute p(w2|c1)
  select nab, nb, na, n into nw2c1, nc1, nw2, n_ from ct3 where a=w2 and b=c1 limit 1;
  if nw2c1 is not null then -- compute  (p(w2|c1c2)p(w2|c1)p(w2|c2)) / 3
    return concat(
      "(1-lambda-mu)0+(lambda/2)p(w2|c1)+(lambda/2)0+(mu)p(w2)",
      log(exp(/*p(w|c1)*/ log(nw2c1) - log(nc1) + @log_lambda-log(2)) + exp(/*p(w2)*/ log(nw2) - log(n_) + @log_mu) )
    );
  end if;
  -- check (w2,c2) and compute p(w2|c2)
  select nac, nc, na, n into nw2c2, nc2, nw2, n_ from ct3 where a=w2 and c=c2 limit 1;
  if nw2c2 is not null then -- compute  p(w2|c2)
    return concat(
      "(1-lambda-mu)0+(lambda/2)0+(lambda/2)p(w2|c2)+(mu)p(w2)",
      log(exp(/*p(w|c2)*/ log(nw2c2) - log(nc2) + @log_lambda-log(2)) + exp(/*p(w2)*/ log(nw2) - log(n_) + @log_mu) )
    );
  end if;
  -- check w2 and compute p(w2)
  select na, n into nw2, n_ from ct3 where a=w2 limit 1;
  return concat(
    "(1-lambda-mu)0+(lambda/2)0+(lambda/2)0+(mu)p(w2)",
    /*p(w2)*/ log(nw2) - log(n_) + @log_mu
  );
END //
DELIMITER ;



select *, pW2gvnCTX(@w2, b, c) from ct3 where a=@w1;



-- get contexts of w1 and join by b with cts from w2
select @w1, @w2, count(*), sum(sqrt(exp(log(pCgvnW1) + log(pW2gvnC)))) as pW2gvnW1, sum(pW2gvnC), sum(pCgvnW1) from (
  select -- c1.a, c2.a, c1.b, c2.b, c1.c, c2.c, 
  pCgvnW1(c1.b, c2.b, c1.c, c2.c, c1.nabc, c1.na, c1.nab, c1.nac) as pCgvnW1,
  pW2gvnC(c1.b, c2.b, c1.c, c2.c, c2.nabc, c2.nbc, c2.nab, c2.nb, c2.nac, c2.nc) as pW2gvnC
  from 
    (select * from ct3 where a=@w1) c1 
    inner join 
    (select * from ct3 where a=@w2) c2 
    on (c1.b = c2.b and c1.c = c2.c)
    limit 1000
) t;

select * from ct3 where a = 'read' and c = 'book' limit 10;







--
--
--
--
--
--

-- consider this instead of the stored procedures
-- if you create func:
-- 
-- create function p1() returns INTEGER DETERMINISTIC NO SQL return @p1;
-- 
-- and view:
-- 
-- create view h_parm as
-- select * from sw_hardware_big where unit_id = p1() ;
-- 
-- Then you can call a view with a parameter:
-- 
-- select s.* from (select @p1:=12 p) parm , h_parm s;
