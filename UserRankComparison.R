library(tidyverse)

userRank <- read_tsv("sortedSampleUserRank_orig.txt")
oldlist <- read_tsv("sampleOldUserList.txt")
rawScore <- read_tsv("sampleOldScoreList.txt")

rawScore <- rawScore%>%
  mutate(userScore = as.numeric(ifelse(userScore=='None', 0, as.numeric(userScore))),
         formDescScore = as.numeric(ifelse(formDescScore=='None', 0, as.numeric(formDescScore))))%>%
  mutate(totalScore = userScore+(2*formNameScore)+ formDescScore)%>%
  arrange(desc(totalScore))%>%
  mutate(rank = c(1:length(rawScore$userid)))

oldListCompare <- oldlist%>%
  inner_join(userRank, by = c("userid"="user"))

rawScoreCompare <- rawScore%>%
  inner_join(userRank, by = c("userid"="user"))

userRank <- userRank%>%
  mutate(oldList = ifelse(user %in% oldlist$userid, T, F))

userRank <- userRank%>%
  left_join(rawScore, by = c("user"="userid"))
#initial weighting
#top 5 new are all poets/writers
#--not necessarily unexpected since their connectedness will bring other similar users up

#double weighting
#3/5 top are film, 2 are photo
#no change in top 5 w/ quadruple weighting

#2/5 top raw are film, other 3 are photographers/vizart
    
colnames(userRank) <- c("userid", "newRank", "oldlist", "userScore", "descScore", "nameScore", "rawScore", "rawRank")     

cor.test(log(userRank$newRank), log(userRank$rawScore))
cor.test(log(userRank$newRank), log(userRank$rawRank))

cor.test(userRank$newRank, userRank$rawScore)
cor.test(userRank$newRank, userRank$rawRank)

userRank <- userRank%>%
  mutate(newOrder = c(1:length(userRank$newRank)))

cor.test(userRank$newOrder, userRank$rawRank)
#intial weighting
#no correlations between new rank/order and old score/order >0.18; log transformation does not affect

#double weighting
#no cor above 0.26
#no cor above 0.24 for quadruple


userRank%>%
  ggplot(aes(y = newOrder, x = rawRank))+
  geom_point()+
  labs(title = "SubmitterRank vs. Raw Topic Score Rank Order",
       subtitle = "Random Sample of 5000",
       y = "SubmitterRank Rank Order",
       x = "Raw Topic Score Rank Order")

userRank%>%
  ggplot(aes(x = log(newRank), y = log(rawScore)))+
  geom_point()

#intiial weighting
#only 397 of top 1000 users in original list

#double weighting
#only 369 of top 1000
#367 for quadruple

userRank%>% 
  filter(newOrder<=1000)%>%
  group_by(oldlist)%>%
  count()

#changing damping factor had effect on score values and order
#w/ quadruple weighting quality didn't change very much
#will be interesting to see effect when measurable (with entire dataset)

highd <- read_tsv("sortedSampleUserRank_highd_5000.txt")
lowd <- read_tsv("sortedSampleUserRank_lowd_5000.txt")

highd <- highd%>%
  top_n(1000, rank)

lowd <- lowd%>%
  top_n(1000, rank)

userRank <- userRank%>%
  mutate(inhighd = ifelse(userid %in% highd$user, T, F),
         inlowd = ifelse(userid %in% lowd$user, T, F))

userRank%>% 
  filter(newOrder<=1000)%>%
  group_by(inhighd)%>%
  count()  #942 in both lists

userRank%>% 
  filter(newOrder<=1000)%>%
  group_by(inlowd)%>%
  count()   #898 in both lists
