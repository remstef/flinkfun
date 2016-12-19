library(RMySQL)

kndiscount = 0.8;
simlex <- '/Users/rem/data/simlex/SimLex-999/SimLex-999.txt'
out <- '/Users/rem/data/simlex/melamud14.tsv'

# init script and db
mydb <- dbConnect(MySQL(), user='root', password='root', dbname='ct', host='0.0.0.0', port=3308)
# test connection
dbGetQuery(mydb, "SELECT 1;")
# set discount factor
dbGetQuery(mydb, sprintf("SET @D = %f;", kndiscount))

# get similarity between two words
getsim = function (w1, w2) {
  rs <- dbSendQuery(mydb, sprintf("call getSimilarityProb('%s','%s',1000,10000);", w1, w2))
  d <- fetch(rs)
  sim <- d[1,'p_A2givenA1']
  dbClearResult(rs)
  
  # clear unused result sets in order to avoid out-of-sync exceptions
  while(dbMoreResults(mydb)){
    rs <- dbNextResult(mydb)
    dbClearResult(rs)
  }
  
  return(sim)
}

# read simlex dataset
sl = read.table(simlex, header=T, stringsAsFactors=F)

# for each word pair get the similarity
for(i in 1:nrow(sl)){
  cat(i, '/', nrow(sl), sl[i,'word1'], sl[i,'word2'],'')
  sl[i,'p.w2|w1'] = getsim(sl[i,'word1'], sl[i,'word2'])
  sl[i,'p.w1|w2'] = getsim(sl[i,'word2'], sl[i,'word1'])
  sl[i,'p.sim'] =  sqrt(exp(log( sl[i,'p.w1|w2']) + log(sl[i,'p.w2|w1'])))
  cat(sl[i,'p.w1|w2'],  sl[i,'p.w2|w1'], sl[i,'p.sim'], '\n')
}
# sl$simp = sqrt(exp(log(sl$`p.w1|w2`) + log(sl$`p.w2|w1`)))

# disconnect all mysql connections
lapply(dbListConnections(MySQL()), dbDisconnect)

# write results
write.table(sl, quote = F, sep = '\t', row.names = F, col.names = T, file = out)

# compute correlation
corr <- data.frame(cor(sl[,-(1:3)],use='complete.obs',method='spearman'))
print('correlations:')
print(corr)
print(corr['p.sim','SimLex999'])



