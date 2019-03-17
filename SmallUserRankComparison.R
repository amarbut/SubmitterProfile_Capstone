library(tidyverse)

userRank <- read_tsv("sortedSmallUserRank.txt")
oldlist <- read_tsv("smallOldUserList.txt")
rawScore <- read_tsv("smallOldScoreList.txt")

rawScore <- rawScore%>%
  mutate(userScore = as.numeric(ifelse(userScore=='None', 0, as.numeric(userScore))))%>%
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

#top ranked user does not appear to be particularly interested in film via admin :-\
    
colnames(userRank) <- c("userid", "newRank", "oldlist", "userScore", "descScore", "nameScore", "rawScore", "rawRank")     

cor.test(userRank$newRank, userRank$rawScore)
cor.test(userRank$newRank, userRank$rawRank)

userRank <- userRank%>%
  mutate(newOrder = c(1:length(userRank$newRank)))

cor.test(userRank$newOrder, userRank$rawRank)
#no correlations between new rank/order and old score/order >0.15; log transformation does not affect


userRank%>%
  ggplot(aes(x = newOrder, y = rawRank))+
  geom_point() 

userRank%>%
  filter(newOrder<=100)%>%
  group_by(oldlist)%>%
  count() #only 27 out of the top 100 ranked users were on the original list
