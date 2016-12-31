library(RMySQL)

clearDBResults <- function(rs){
  dbClearResult(rs)
  # clear unused result sets in order to avoid out-of-sync exceptions
  while(dbMoreResults(mydb)){
    rs <- dbNextResult(mydb)
    dbClearResult(rs)
  }
}

produceScoresPROB <- function(){
  # get similarity between two words
  getsimPROB = function (w1, w2) {
    rs <- dbSendQuery(mydb, sprintf("call getSimilarityProbInner('%s','%s',1000,10000);", w1, w2))
    d <- fetch(rs)
    sim <- d[1,'p_A2givenA1']
    clearDBResults(rs)
    return(sim)
  }
  # for each word pair get the similarity
  for(i in 1:nrow(sl)){
    cat(i, '/', nrow(sl), sl[i,'word1'], sl[i,'word2'],'')
    sl[i,'p.w2|w1.inner'] = getsimPROB(sl[i,'word1'], sl[i,'word2'])
    sl[i,'p.w1|w2.inner'] = getsimPROB(sl[i,'word2'], sl[i,'word1'])
    sl[i,'p.sim.inner'] =  sqrt(exp(log( sl[i,'p.w1|w2.inner']) + log(sl[i,'p.w2|w1.inner'])))
    cat(sl[i,'p.w1|w2.inner'],  sl[i,'p.w2|w1.inner'], sl[i,'p.sim.innner'], '\n')
  }
  # sl$p.sim <- sqrt(exp(log(sl$p.w1.w2)+log(sl$p.w2.w1)))
  return(sl)
}

produceScoresJBT <- function(){
  # get similarity between two words JBT shared contexts
  getsimJBT = function (w1, w2) {
    rs <- dbSendQuery(mydb, sprintf("call getSimilarity('%s','%s',1000,1000,200);", w1, w2))
    d <- fetch(rs)
    sim <- d[1,'cnt']
    clearDBResults(rs)
    return(sim)
  }
  # for each word pair get the similarity
  for(i in 1:nrow(sl)){
    cat(i, '/', nrow(sl), sl[i,'word1'], sl[i,'word2'],'')
    sl[i,'sharedcontexts'] = getsimJBT(sl[i,'word1'], sl[i,'word2'])
    cat(sl[i,'sharedcontexts'], '\n')
  }
  return(sl)
}

produceScoresJBTRank <- function(){
  # get similarity between two words JBT shared contexts
  getsimJBTRank = function (w1, w2) {
    rs <- dbSendQuery(mydb, sprintf("call getSimilarA('%s',1000,1000,200);", w1))
    d <- fetch(rs)
    rank <- which(d[,2] == w2)
    if(!length(rank)) # if w2 is not found in resultset set rank to 201 (one more than max)
      rank <- 201
    clearDBResults(rs)
    return(1/rank)
  }
  
  # for each word pair get the similarity
  for(i in 1:nrow(sl)){
    cat(i, '/', nrow(sl), sl[i,'word1'], sl[i,'word2'],'')
    sl[i,'jbtrank'] = getsimJBTRank(sl[i,'word1'], sl[i,'word2'])
    cat(sl[i,'jbtrank'], '\n')
  }
  return(sl)
}

#
## public static void main(String[] args)
#

kndiscount = 0.8;
simlex <- '/Users/rem/data/simlex/SimLex-999/SimLex-999.txt'
out <- '/Users/rem/data/simlex/melamud14.tsv'

# init script and db
mydb <- dbConnect(MySQL(), user='root', password='root', dbname='ct', host='0.0.0.0', port=3308)
# test connection
dbGetQuery(mydb, "SELECT 1;")
# set discount factor
dbGetQuery(mydb, sprintf("SET @D = %f;", kndiscount))

# read simlex dataset
# sl = read.table(simlex, header=T, stringsAsFactors=F)
sl <- read.table(out, header=T, stringsAsFactors=F)
# sl <- produceScoresPROB()
# sl <- produceScoresJBT()
# sl <- produceScoresJBTRank()
# write results
#write.table(sl, quote = F, sep = '\t', row.names = F, col.names = T, file = out)

# disconnect all mysql connections
lapply(dbListConnections(MySQL()), dbDisconnect)

# compute correlation
slscores = sl[,-(1:3)]
#slscores = sl[which(sl$POS == 'V'),-(1:3)]
#slscores = sl[which(sl$jbtrank >= 1/200),-(1:3)]
corr <- cor(slscores, use='complete.obs', method='spearman')
corr <- data.frame(corr)

print('correlations:')
print(corr)
print(corr['p.sim','SimLex999'])
print(corr['p.sim.inner','SimLex999'])
print(corr['jbtrank','SimLex999'])
print(corr['sharedcontexts','SimLex999'])
