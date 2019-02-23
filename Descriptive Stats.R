library(RJDBC)
library(tidyverse)
library(scales)
library(psych)
library(countrycode)
library(xtable)

setwd("C:/Users/Anna/Documents/MSBA/Capstone/SubmitterProfile_Capstone/LaTeX_Files")

driver <- JDBC("com.amazon.redshift.jdbc42.Driver",
               "~/RedshiftJDBC42-1.2.12.1017.jar", identifier.quote="`")

url <- "jdbc:redshift://localhost:5439/any?ssl=false&UID=any&PWD=any"

conn <- dbConnect(driver, url)

#users by createdate and subdate
createq <- "select count(distinct u.userid), extract(year from u.createdate) as year
          from submittable_db.smmuser u
          group by 2"

create_users <- dbGetQuery(conn, createq)

subq <- "select count(distinct userid), extract(year from createdon) as year
          from submittable_db.submission
          group by 2"

submit_users <- dbGetQuery(conn, subq)
jpeg("userCreate_plot.jpg")
create_users%>%
  filter(!is.na(year), year != 2019)%>%
  ggplot(aes(x = factor(year), y = count))+
    geom_bar(stat = "identity")+
  labs(title = "New Submitters by Year",
       x = "Year",
       y = "New Users")
dev.off()

total_users <- sum(create_users$count) # 4,276,490 submitters

jpeg("userSubmit_plot.jpg")
submit_users%>%
  filter(!is.na(year), year != 2019, year >=2010)%>%
  ggplot(aes(x = factor(year), y = count))+
  geom_bar(stat = "identity")+
  labs(title = "Number of Active Submitters by Year",
       x = "Year",
       y = "Active Submitters")+
  scale_y_continuous(labels = comma)
dev.off()

#user submission stats

num_subq <- "select s.userid, extract(year from s.createdon) as year,
              extract(month from s.createdon) as month,
              count(s.submissionid)
              from submittable_db.submission s
              join submittable_db.smmuser u on s.userid = u.userid
              where u.email not like '%@submittable.com'
              group by 1,2,3"

subs <- dbGetQuery(conn, num_subq)

avg_subs <- subs%>%
  group_by(userid)%>%
  summarise(total_subs = sum(count))

mean(avg_subs$total_subs)#avg 5 subs per user
sub_describe <- describe(avg_subs$total_subs)

sub_count <- avg_subs%>%
  group_by(total_subs)%>%
  count()

#62% of users only submit once
percent_one_sub <- sub_count$n[sub_count$total_subs == 1]/sum(sub_count$n)
  
#user demographics

genderq <- "select gender, count(userid)
            from submittable_db.smmuser
            group by 1"

gender <- dbGetQuery(conn, genderq)

ageq <- "select extract(year from dob), count(userid)
          from submittable_db.smmuser
          group by 1"

age <- dbGetQuery(conn, ageq)

real_age <- age%>%
  filter(date_part <= 2000, !is.na(date_part), date_part >=1930)

sum(real_age$count)  

age_buckets <- data.frame(bucket = c(replicate(18,"<18"), replicate(7,"19-25"), replicate(10,"26-35"),
                                     replicate(10, "36-45"), replicate(10, "46-55"), replicate(10, "56-65"),
                                     replicate(35, "66+")),
                          age = c(1:18, 19:25,26:35,36:45, 46:55, 56:65,66:100))

real_age <- real_age%>%
  mutate(age = 2019 - date_part)%>%
  inner_join(age_buckets, by = c("age" = "age"))

bucket_order <- c("<18", "19-25", "26-35", "36-45", "46-55", "56-65", "66+")

jpeg("userAge_plot.jpg")
real_age%>%
  group_by(bucket)%>%
  summarize(total = sum(count))%>%
  ggplot(aes(x = factor(bucket, levels = bucket_order), y = total))+
  geom_bar(stat = "identity")+
  labs(title = "Self-Reported User Age Distribution",
       x = "User Age", y = "Number of Users")
dev.off()

addressq <- "select a.country, count(u.userid)
              from submittable_db.smmuser u
              join submittable_db.address a on u.addressid = a.addressid
              where a.country similar to '[A-Z][A-Z]'
              group by 1"
address <- dbGetQuery(conn, addressq)
sum(address$count)

address <- address%>%
  mutate(fullname = countrycode(country, "iso2c", "country.name"))

top_address <- address%>%
  top_n(10, count)%>%
  select(fullname, count)%>%
  arrange(desc(count))

colnames(top_address) <- c("Country", "Number of Submitters")

sink("userCountry.txt")
xtable(top_address)
sink()

top_address$`Number of Submitters`[top_address$Country == 'United States']/sum(address$count)
#user descriptions and interests

descq <- "
select count(userid)
from submittable_db.smmuser
where description is not null and description != ''"

desc <- dbGetQuery(conn, descq)

#forms and submissions

productq <- "select count(distinct p.productid)
from submittable_db.product p
join submittable_db.publisher pub on p.publisherid = pub.publisherid
where pub.accounttypeid not in (11,16,64)"

formCount <- dbGetQuery(conn, productq)

productYearq <- "select extract(year from p.createdate) as year, count(p.productid)
from submittable_db.product p
join submittable_db.publisher pub on p.publisherid = pub.publisherid
where pub.accounttypeid not in (11,16,64)
group by 1"

productYear <- dbGetQuery(conn, productYearq)

jpeg("formsByYear_plot.jpg")
productYear%>%
  filter(year != 2019)%>%
  ggplot(aes(x = year, y = count))+
  geom_bar(stat = "identity")+
  scale_x_continuous(breaks = c(2010:2019))+
  labs(title = "New Opportunities by Year",
       x ="Year",
       y = "Number of New Opportunities")
dev.off()

subcountq <- "select count(s.submissionid)
from submittable_db.submission s
join submittable_db.product p on s.productid = p.productid
join submittable_db.publisher pub on p.publisherid = pub.publisherid
where pub.accounttypeid not in (11,16,64)"

subCount <- dbGetQuery(conn, subcountq)

subYearq <- "select extract(year from s.createdon) as year, count(s.submissionid)
from submittable_db.submission s
join submittable_db.product p on s.productid = p.productid
join submittable_db.publisher pub on p.publisherid = pub.publisherid
where pub.accounttypeid not in (11,16,64)
group by 1"

subYear <- dbGetQuery(conn, subYearq)

jpeg("subsByYear_plot.jpg")
subYear%>%
  filter(year != 2019, year >= 2010)%>%
  ggplot(aes(x = year, y = count))+
  geom_bar(stat = "identity")+
  scale_x_continuous(breaks = c(2010:2019))+
  scale_y_continuous(labels = comma)+
  labs(title ="Total Submissions by Year",
       x ="Year",
       y = "Total Number of Submissions")
dev.off()

subYear <- subYear%>%
  arrange(year)%>%
  mutate(change = count - lag(count))

numSubsq <- "
select s.productid, count(distinct s.submissionid)
from submittable_db.submission s
join submittable_db.product p on s.productid = p.productid
join submittable_db.publisher pub on p.publisherid =pub.publisherid
where pub.accounttypeid not in (11,16,64)
group by 1"

numsubs <- dbGetQuery(conn, numSubsq)

mean(numsubs$count)
range(numsubs$count)

overOne <- numsubs%>%
  filter(count > 1)
overFive <- numsubs%>%
  filter(count >5)
overTwenty <- numsubs%>%
  filter(count >20)

# Giving WAY low numbers--not sure why...
# activeFormq <- "
# select extract(year from s.createdon) as year, count(distinct s.productid)
# from submittable_db.submission s
# left join submittable_db.product p on s.productid = p.productid
# left join submittable_db.publisher pub on p.productid = pub.publisherid
# where pub.accounttypeid not in (11,16,64)
# group by 1
# "
# activeForm <- dbGetQuery(conn, activeFormq)
# 
# 
# activeForm%>%
#   filter(year != 2019, year >= 2010)%>%
#   ggplot(aes(x = year, y = count))+
#   geom_bar(stat = "identity")+
#   scale_x_continuous(breaks = c(2010:2018))+
#   labs(title = "Number of Active Opportunities by Year",
#        x = "Year",
#        y = "Number of Opportunities Receiving at least One Submission")

numSubsYearq <- "
select extract(year from s.createdon) as year, s.productid, count(distinct s.submissionid)
from submittable_db.submission s
join submittable_db.product p on s.productid = p.productid
join submittable_db.publisher pub on p.publisherid =pub.publisherid
where pub.accounttypeid not in (11,16,64)
group by 1, 2"

numSubsYear <- dbGetQuery(conn, numSubsYearq)

jpeg("activeForms_plot.jpg")
numSubsYear%>%
  filter(year != 2019, year >=2010)%>%
  group_by(year)%>%
  count()%>%
  ggplot(aes(x = year, y = n))+
  geom_bar(stat = "identity")+
  scale_x_continuous(breaks = c(2010:2018))+
  labs(title = "Number of Active Opportunities by Year",
       x = "Year", 
       y ="Number of Opportunities Receiving at least One Submission")
dev.off()  

jpeg("avgSubs_plot.jpg")
numSubsYear%>%
  filter(year != 2019, year >= 2010)%>%
  group_by(year)%>%
  summarize(avg = mean(count))%>%
  ggplot(aes(x = year, y = avg))+
  geom_line()+
  geom_point()+
  scale_y_continuous(limits = c(50, 100))+
  scale_x_continuous(breaks = c(2010:2018))+
  labs(title = "Average Submissions per Opportunity by Year",
       x = "Year", 
       y = "Average Number of Submissions per Opportunity")
dev.off()


#usecases
usecaseq <- "
select hd.properties__use_case__value, count(p.productid)
from hubspot.deals hd
join submittable_db.product p on hd.properties__admin_id__value__string = p.publisherid
group by 1"

usecases <- dbGetQuery(conn, usecaseq)

usecases <- usecases%>%
  filter(!grepl("Do Not Use", properties__use_case__value), !is.na(properties__use_case__value))

top_usecases <- usecases%>%
  top_n(10, count)%>%
  arrange(desc(count))

sink("top_usecases.txt")
xtable(top_usecases)
sink()

#discover tags
discoverq <- '
select t.name, count(distinct p.productid)
from submittable_db."tag" t
join submittable_db.producttag2 pt on t.id = pt.tagid
join submittable_db.product p on pt.productid = p.productid
join submittable_db.publisher pub on p.publisherid = pub.publisherid
where pub.accounttypeid not in (11,16,64)
group by 1
'
discover <- dbGetQuery(conn, discoverq)

sum(discover$count)

newtags <- read.csv("discovertags.csv")
newtags <- newtags%>%
  mutate(name = as.character(name),
         newlabel = ifelse(newlabel=='', NA, as.character(newlabel)))%>%
  filter(!is.na(newlabel))

discovertags <- paste(unique(newtags$name))
discovertags <- paste(discovertags, collapse = "','")
discovertags <- paste("'", discovertags, collapse = "")
discovertags <- paste(discovertags, "'", collapse = "")

discoverq2 <- paste(
  'select p.productid, t.name
  from submittable_db."tag" t
  join submittable_db.producttag2 pt on t.id = pt.tagid
  join submittable_db.product p on pt.productid = p.productid
  join submittable_db.publisher pub on p.publisherid = pub.publisherid
  where pub.accounttypeid not in (11,16,64)
  and t.name in (', discovertags,
  ')', collapse = " ")

discover2 <- dbGetQuery(conn, discoverq2)

discover_new <- discover2%>%
  left_join(newtags, by = c("name" = "name"))

discover_grouped <- discover_new%>%
  group_by(newlabel)%>%
  summarise(count = n_distinct(productid))

sum(discover_grouped$count)

colnames(discover_grouped) <- c("Discover Group", "Count")

sink("discovergroup.txt")
xtable(discover_grouped)
sink()

discoverlist <- data.frame(`Discover Label` = discover$name)
usecaselist <- data.frame(Usecase = usecases$properties__use_case__value)

sink("discoverlist.txt")
xtable(discoverlist)
sink()

sink("usecaselist.txt")
xtable(usecaselist)
sink()
