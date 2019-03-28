# load SparkR
spark_path <- '/usr/local/spark'

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

sparkR.session(master = "yarn", sparkConfig = list(spark.driver.memory = "1g"))


#Loading the data
nyc_ticket <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv",source ="csv", header=TRUE,inferSchema=TRUE)

head(nyc_ticket,20)
nrow(nyc_ticket)
ncol(nyc_ticket)
printSchema(nyc_ticket)
str(nyc_ticket)

#Cleaning the data

#Creating the year column to check the data is of 2017 or not
nyc_ticket$Issue_year <- year(nyc_ticket$`Issue Date`)

head(summarize(groupBy(nyc_ticket, nyc_ticket$Issue_year),
               count = n(nyc_ticket$Issue_year)))
#There are other years data as well

#Keeping only the 2017 data
nyc_ticket <- nyc_ticket[nyc_ticket$Issue_year=="2017",]

#Confirming the data is for 2017 only
head(summarize(groupBy(nyc_ticket, nyc_ticket$Issue_year),
               count = n(nyc_ticket$Issue_year)))
# Issue_year   count                                                            
# 1       2017 5431918

#Checking for NA values
count(where(nyc_ticket, isNull(nyc_ticket$`Summons Number`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Plate ID`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Registration State`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Issue Date`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Violation Code`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Vehicle Body Type`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Vehicle Make`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Violation Precinct`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Issuer Precinct`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$`Violation Time`))) #0
count(where(nyc_ticket, isNull(nyc_ticket$Issue_year))) #0

#Changing the columns names,removing spaces
colnames(nyc_ticket)

colna <- colnames(nyc_ticket)

colna <- gsub(pattern = " ", replacement = "_",colna)

colnames(nyc_ticket) <- colna


#Checking if zeroes are present in any other columns
count(where(nyc_ticket, nyc_ticket$Summons_Number==0)) #0 rows
count(where(nyc_ticket, nyc_ticket$Plate_ID==0)) #3 rows 
count(where(nyc_ticket, nyc_ticket$Registration_State==0)) #0 rows
count(where(nyc_ticket, nyc_ticket$Violation_Code==0)) #36 rows
count(where(nyc_ticket, nyc_ticket$Vehicle_Body_Type==0)) #0 rows
count(where(nyc_ticket, nyc_ticket$Vehicle_Make==0)) #0 rows
count(where(nyc_ticket, nyc_ticket$Violation_Precinct==0)) #925596 rows
count(where(nyc_ticket, nyc_ticket$Issuer_Precinct==0)) #1078406 rows
count(where(nyc_ticket, nyc_ticket$Issue_year==0)) #0 rows

#Cleaning the data which has zeroes 
nyc_ticket <- nyc_ticket[nyc_ticket$Violation_Precinct!=0,]
nyc_ticket <- nyc_ticket[nyc_ticket$Issuer_Precinct!=0,]
nyc_ticket <- nyc_ticket[nyc_ticket$Violation_Code!=0,]

head(nyc_ticket)

State_count <- summarize(group_by(nyc_ticket, nyc_ticket$Registration_State),
                         count=n(nyc_ticket$Registration_State))
State_count_head <- head(arrange(State_count,desc(State_count$count)))
State_count_head

#Checking the registration date column which has 99 as value
count(where(nyc_ticket,nyc_ticket$Registration_State=="99")) #12525 no. of rows

#Replacing it with NY which has max count
nyc_ticket$Registration_State <- ifelse(nyc_ticket$Registration_State=="99","NY",nyc_ticket$Registration_State)


#Converting violation time column to time format

#As the column is in character type, we are splitting it as first two digits into a column which is hours,
#next two as minutes and A/P as AM or PM
nyc_ticket$Violation_hours <- (substr(nyc_ticket$Violation_Time,1,2))
nyc_ticket$Violation_mins <- substr(nyc_ticket$Violation_Time,3,4)
nyc_ticket$AM_PM <- substr(nyc_ticket$Violation_Time,5,6)
head(nyc_ticket)

#Creating a new column which has value as 12 hours if its PM and 0 if its AM
nyc_ticket$time_diff <- ifelse(nyc_ticket$AM_PM=="P",12,0)

nyc_ticket$Violation_hours <- nyc_ticket$Violation_hours+nyc_ticket$time_diff

nyc_ticket$Violation_hours <- ifelse(nyc_ticket$Violation_hours==24,0,nyc_ticket$Violation_hours)

nyc_ticket$Violation_hours <- ifelse(nyc_ticket$Violation_hours>=0 & nyc_ticket$Violation_hours<=23,nyc_ticket$Violation_hours,NA)


#As the time is converted to 24 hours , we are removing the AM/PM column and 12 hour column
nyc_ticket$AM_PM <- NULL
nyc_ticket$time_diff <- NULL
head(nyc_ticket)

nyc_ticket$Issue_month <- month(nyc_ticket$Issue_Date)

# Before executing any hive-sql query from RStudio, you need to add a jar file in RStudio 
sql("ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar")

createOrReplaceTempView(x = nyc_ticket,viewName = "SQL_nyc")

distin_summons <- SparkR::sql("select count(distinct(Summons_Number)) from SQL_nyc")
head(distin_summons)
# count(DISTINCT Summons_Number)                                                
# 1                       4337598
# This is equal to the total count , hence there are no duplicates in summon numbers

#No need to find duplicates in Plate ID as multiple tickets can be attached to a plate(vehicle)
#No duplicates should be found in Registration state, Issue date,Violation code,Vehicle body type,
#Vehicle maker,Violation Precinct,Issuer precinct,Violation Time,Issue year


#--- EDA

#Finding the range of dates
Range_issueDate <- SparkR::sql("select min(Issue_Date), max(Issue_Date) from SQL_nyc")
head(Range_issueDate)
# min(Issue_Date) max(Issue_Date)                                               
# 1      2017-01-01      2017-12-31

# 1.Find the total number of tickets for the year.
nrow(nyc_ticket)
# A total of 4337598 tickets were recorded for 2017 year

#2.Find out the number of unique states from where the cars that got parking tickets came from.
#(Hint: Use the column 'Registration State')
unique_states <- SparkR::sql("select count(distinct(Registration_State)) from SQL_nyc")
head(unique_states)
# There are about 64 unique states from where the cars got parking tickets

#Aggregation tasks
#1. How often does each violation code occur? Display the frequency of the top five violation codes.
Freq_Vio_code <- summarize(group_by(nyc_ticket,nyc_ticket$Violation_Code),count=n(nyc_ticket$Violation_Code))
Freq_Vio_code_head <- head(arrange(Freq_Vio_code,desc(Freq_Vio_code$count)))
head(Freq_Vio_code_head)
# Violation_Code  count                                                         
# 1             21 641476
# 2             38 540853
# 3             14 471005
# 4             20 315858
# 5             46 307850
# 6             37 292803
#Most violated code is Code 21, followed by Code 38, Code 14, Code 20, Code 46

Freq_Vio_code_df <- data.frame(head(Freq_Vio_code_head,5))
ggplot2::ggplot(Freq_Vio_code_df, aes(x=as.factor(Freq_Vio_code_df$Violation_Code),y=Freq_Vio_code_df$count,fill=as.factor(Freq_Vio_code_df$Violation_Code)))+geom_bar(stat="identity")

#2. How often does each 'vehicle body type' get a parking ticket? How about the 'vehicle make'? 
Body_type_freq <- SparkR::sql("select Vehicle_Body_Type, count(*) as Count
                              from SQL_nyc
                              group by Vehicle_Body_Type
                              order by count desc")
head(Body_type_freq,5)
# Vehicle_Body_Type   Count                                                     
# 1              SUBN 1458668
# 2              4DSD 1225807
# 3               VAN  682225
# 4              DELV  350406
# 5              PICK  116101
#SUBN body type vehicles are getting high tickets, followed by 3DSD,VAN,DELV,PICK

Body_type_freq_df <- data.frame(head(Body_type_freq))
ggplot2::ggplot(Body_type_freq_df, aes(x=as.factor(Body_type_freq_df$Vehicle_Body_Type), y=Body_type_freq_df$Count, fill=as.factor(Body_type_freq_df$Vehicle_Body_Type)))+geom_bar(stat="identity")

#Low tickets for body type
Body_type_freq_low <- SparkR::sql("select Vehicle_Body_Type, count(*) as Count
                                  from SQL_nyc
                                  group by Vehicle_Body_Type
                                  order by count asc")
head(Body_type_freq_low,5)
# Vehicle_Body_Type Count                                                      
# 1               ILQT     1
# 2               INNI     1
# 3               DUIM     1
# 4               DUMD     1
# 5                SNA     1
#Lowest number of tickets are for ILTQ,INNI,DUIM,DUMD,SNA etc body types

Vehicle_make_freq <- SparkR::sql("select Vehicle_Make, count(*) as Count
                                 from SQL_nyc
                                 group by Vehicle_Make
                                 order by count desc")
head(Vehicle_make_freq,5)
# Vehicle_Make  Count                                                           
# 1         FORD 534976
# 2        TOYOT 448417
# 3        HONDA 408134
# 4        NISSA 340336
# 5        CHEVR 294611
#More tickets getting assigned for FORD, followed by TOYOTA,HONDA,NISSAN,CHEVR
Vehicle_make_freq_df <- data.frame(head(Vehicle_make_freq))
ggplot2::ggplot(Vehicle_make_freq_df, aes(x=as.factor(Vehicle_make_freq_df$Vehicle_Make), y=Vehicle_make_freq_df$Count,fill=as.factor(Vehicle_make_freq_df$Vehicle_Make)))+geom_bar(stat="identity")


Vehicle_make_freq_low <- SparkR::sql("select Vehicle_Make, count(*) as Count
                                     from SQL_nyc
                                     group by Vehicle_Make
                                     order by count asc")
head(Vehicle_make_freq_low,5)
# Vehicle_Make Count                                                            
# 1        HYADA     1
# 2        POLAR     1
# 3          WHC     1
# 4         GLGG     1
# 5          N/V     1
#least tickets are getting assigned for HYDA, POLAR,WHC,GLGG,N/V etc


#3.A precinct is a police station that has a certain zone of the city under its command. 
#Find the (5 highest) frequency of tickets for each of the following:

# a. 'Violation Precinct' (this is the precinct of the zone where the violation occurred). 
# Using this, can you make any insights for parking violations in any specific areas of the city?
Area_parking_vio <- summarize(group_by(nyc_ticket,nyc_ticket$Violation_Precinct),count=n(nyc_ticket$Violation_Precinct))
Area_parking_vio_head <- head(arrange(Area_parking_vio,desc(Area_parking_vio$count)))
Area_parking_vio_head
# Violation_Precinct  count                                                     
# 1                 19 272984
# 2                 14 202860
# 3                  1 169788
# 4                 18 167582
# 5                114 144632
# 6                 13 124042
#Most violations happen in Precinct 19, followed by 14,1,18,114,13 precincts
Area_parking_vio_head_df <- data.frame(Area_parking_vio_head)
ggplot2::ggplot(Area_parking_vio_head_df,aes(x=as.factor(Area_parking_vio_head_df$Violation_Precinct),y=Area_parking_vio_head_df$count,fill=as.factor(Area_parking_vio_head_df$Violation_Precinct)))+geom_bar(stat="identity")

# b. 'Issuer Precinct' (this is the precinct that issued the ticket)
# Here you would have noticed that the dataframe has 'Violating Precinct' or 'Issuing Precinct' as '0'. 
# These are the erroneous entries. 
# Hence, provide the record for five correct precincts. (Hint: Print top six entries after sorting)
Issue_prec <- summarize(group_by(nyc_ticket,nyc_ticket$Issuer_Precinct),count=n(nyc_ticket$Issuer_Precinct))
Issue_prec_head <- head(arrange(Issue_prec,desc(Issue_prec$count)))

# Issuer_Precinct  count                                                        
# 1              19 266595
# 2              14 200074
# 3               1 168207
# 4              18 162675
# 5             114 143680
# 6              13 122269
Issue_prec_head_df <- data.frame(head(Issue_prec_head))
ggplot2::ggplot(Issue_prec_head_df, aes(x=as.factor(Issue_prec_head_df$Issuer_Precinct), y=Issue_prec_head_df$count,fill=as.factor(Issue_prec_head_df$Issuer_Precinct)))+geom_bar(stat="identity")

#4. Find the violation code frequency across three precincts which have issued the most number of tickets
# - do these precinct zones have an exceptionally high frequency of certain violation codes? 
# Are these codes common across precincts? 

#Checking violation code frequency for Issuer Precinct 19
Precinct_19 <- SparkR::sql("select Violation_Code, count(*) as Violation_Code_Count
                           from SQL_nyc
                           where Violation_Precinct=19
                           group by Violation_Code
                           order by Violation_Code_Count desc")
head(Precinct_19)
# Violation_Code Violation_Code_Count                                           
#            46                50514
#            38                37421
#            37                36453
#            14                30314
#            21                28658
#            20                15074
#Most violated code in precicnt 19 is code 46, followed by 38,37
Violation_Precinct_19_df <- data.frame(head(Precinct_19))
ggplot2::ggplot(Violation_Precinct_19_df, aes(x=as.factor(Violation_Precinct_19_df$Violation_Code), y=Violation_Precinct_19_df$Violation_Code_Count,fill=as.factor(Violation_Precinct_19_df$Violation_Code)))+geom_bar(stat = "identity")

#For Precinct 14
Precinct_14 <- SparkR::sql("select Violation_Code, count(*) as Violation_Code_Count
                           from SQL_nyc
                           where Violation_Precinct=14
                           group by Violation_Code
                           order by Violation_Code_Count desc")
head(Precinct_14)
# Violation_Code Violation_Code_Count                                           
#            14                45725
#            69                30453
#            31                22615
#            47                18640
#            42                10022
#            46                 8302
#Most violated code in precicnt 19 is code 14, followed by 69,31

Violation_Precinct_14_df <- data.frame(head(Precinct_14))
ggplot2::ggplot(Violation_Precinct_14_df,aes(x=as.factor(Violation_Precinct_14_df$Violation_Code),y=Violation_Precinct_14_df$Violation_Code_Count,fill=as.factor(Violation_Precinct_14_df$Violation_Code)))+geom_bar(stat="identity")

#For Precinct 1
Precinct_1 <- SparkR::sql("select Violation_Code, count(*) as Violation_Code_Count
                          from SQL_nyc
                          where Violation_Precinct=1
                          group by Violation_Code
                          order by Violation_Code_Count desc")
head(Precinct_1)
# Violation_Code Violation_Code_Count                                           
#           14                38715
#           16                19128
#           20                15244
#           46                13386
#           38                 8538
#           17                 7565
#Most violated code in precicnt 19 is code 14, followed by 16,20

Violation_Precinct_1_df <- data.frame(head(Precinct_1))

ggplot2::ggplot(Violation_Precinct_1_df,aes(x=as.factor(Violation_Precinct_1_df$Violation_Code),y=Violation_Precinct_1_df$Violation_Code_Count,fill=as.factor(Violation_Precinct_1_df$Violation_Code)))+geom_bar(stat="identity")


#Analysis:
#Violation code 14 is the most violated code across the 3 precincts , and code 46 also comes in
# the top 6 violated codes across all 3 precincts
#Codes 38,20 is in top 3 for precincts 19 and 1

#5.
# a.Find a way to deal with missing values, if any.
# Hint: Check for the null values using 'isNull' under the SQL. 
# Also, to remove the null values, check the 'dropna' command in the API documentation.
null_val <- SparkR::sql("select count(*) from SQL_nyc
                        where Violation_Time is NULL")
head(null_val)
#No null values as we removed the null values in violation hours column above

#Violation time field is converted to Violation_hours and Violation_mins. We are going to create
# bins on Violation_hours column

# Divide 24 hours into six equal discrete bins of time. The intervals you choose are at your discretion. 
#For each of these groups, find the three most commonly occurring violations.
time_bins <- SparkR::sql("SELECT Violation_hours,
                         Violation_Code,
                         CASE WHEN Violation_hours>=0 AND Violation_hours<=4
                         THEN 'Midnight'
                         WHEN Violation_hours>4 AND Violation_hours<=6
                         THEN 'Early Morning'
                         WHEN Violation_hours>6 AND Violation_hours<=11
                         THEN 'Morning'
                         WHEN Violation_hours>11 AND Violation_hours<=16
                         THEN 'Afternoon'
                         WHEN Violation_hours>16 AND Violation_hours<=20
                         THEN 'Evening'
                         WHEN Violation_hours>20 AND Violation_hours<=23
                         THEN 'Night'
                         END AS Time_Bins
                         FROM SQL_nyc")


head(time_bins)
printSchema(time_bins)
createOrReplaceTempView(time_bins,"SQL_time_bins")

NA_time_bins <- SparkR::sql("select * from SQL_time_bins where Time_Bins is NULL")
head(NA_time_bins)
#There are few hours which are invalid
#Removing them
time_bins <- dropna(time_bins)
#Creating view again
createOrReplaceTempView(time_bins,"SQL_time_bins")


top_violated_codes_per_time <- SparkR::sql("select Time_Bins, count(Violation_Code) as Vio_count
                                           from SQL_time_bins
                                           group by Time_Bins
                                           order by Vio_count desc")

head(top_violated_codes_per_time)
# Time_Bins Vio_count                                                       
# 1       Morning   1890227
# 2     Afternoon   1359183
# 3      Midnight    516256
# 4       Evening    322411
# 5 Early Morning    154893
# 6         Night    107120
# Most tickets are assigned during the morning time which is from 6 to 11 in the morning, followed by afternoon which is after 11 to 16
#Surprisingly third highest number of tickets are booked during midnight

top_violated_codes_per_time_df <- data.frame(head(top_violated_codes_per_time))
ggplot2::ggplot(top_violated_codes_per_time_df,aes(x=top_violated_codes_per_time_df$Time_Bins,y=top_violated_codes_per_time_df$Vio_count,fill=top_violated_codes_per_time_df$Time_Bins))+geom_bar(stat = "identity")

#finding the three most violated codes during the morning period, which has the highest number of tickets
Top_3_viol_codes <- SparkR::sql("select Violation_Code, count(*) as vio_count
                                from SQL_time_bins
                                where Time_Bins='Morning'
                                group by Violation_Code
                                order by vio_count desc")
head(Top_3_viol_codes)
# Violation_Code vio_count                                                      
# 1             21    549632
# 2             14    194938
# 3             38    177285
# 4             46    115882
# 5             20    110515
# 6             71    108129
#most violated code during morning is Code 21, followed by 14,38,46,20,71 codes

#For afternoon
Top_3_viol_codes_aft <- SparkR::sql("select Violation_Code, count(*) as vio_count
                                    from SQL_time_bins
                                    where Time_Bins='Afternoon'
                                    group by Violation_Code
                                    order by vio_count desc")
head(Top_3_viol_codes_aft)
# Violation_Code vio_count                                                      
# 1             38    234327
# 2             37    166702
# 3             14    151441
# 4             20    112896
# 5             46    112508
# 6             71     98062
#Most violated code during afternoon is 38, followed by 37, 14

#Evening
Top_3_viol_codes_evng <- SparkR::sql("select Violation_Code, count(*) as vio_count
                                     from SQL_time_bins
                                     where Time_Bins='Evening'
                                     group by Violation_Code
                                     order by vio_count desc")
head(Top_3_viol_codes_evng)
# Violation_Code vio_count                                                      
# 1             38     61146
# 2             14     39650
# 3             37     36783
# 4             46     28263
# 5             20     26705
# 6             40     18947
#Most violated code in the evening is 38 followed by 14,37,46,20,40

#Night
Top_3_viol_codes_night <- SparkR::sql("select Violation_Code, count(*) as vio_count
                                      from SQL_time_bins
                                      where Time_Bins='Night'
                                      group by Violation_Code
                                      order by vio_count desc")
head(Top_3_viol_codes_night)
# Violation_Code vio_count                                                      
# 1             40     17435
# 2             14     15553
# 3             38     12290
# 4             20     11098
# 5             78      9323
# 6             46      8280
#Most violated code during night is 40, followed by 14,38

#Midnight
Top_3_viol_codes_midnight <- SparkR::sql("select Violation_Code, count(*) as vio_count
                                         from SQL_time_bins
                                         where Time_Bins='Midnight'
                                         group by Violation_Code
                                         order by vio_count desc")
head(Top_3_viol_codes_midnight)
# Violation_Code vio_count                                                      
# 1             21     85902
# 2             38     56191
# 3             14     45483
# 4             40     42299
# 5             46     36867
# 6             37     36387
#most violated code during midnight is 21, followed by 38,14

#Earlymorning
Top_3_viol_codes_erlymrng <- SparkR::sql("select Violation_Code, count(*) as vio_count
                                         from SQL_time_bins
                                         where Time_Bins='Early Morning'
                                         group by Violation_Code
                                         order by vio_count desc")
head(Top_3_viol_codes_erlymrng)
# Violation_Code vio_count                                                      
# 1             40     39042
# 2             14     24900
# 3             20     20552
# 4             71      8685
# 5             19      8598
# 6             46      8412
#Most violated code during early morning is 40, followed by 14, 20

#c. Now, try another direction. For the three most commonly occurring violation codes, 
#find the most common time of the day (in terms of the bins from the previous part)

three_top_vio_codes <- SparkR::sql("select Violation_Code,count(*) as cou
                                   from SQL_time_bins
                                   group by Violation_Code
                                   order by cou desc 
                                   limit 3")
head(three_top_vio_codes)
# Violation_Code    cou                                                         
# 1             21 641959
# 2             38 541396
# 3             14 471965

#Finding the most common times of day for these codes

comm_time <- SparkR::sql("select Violation_Code,Time_Bins,count(*) as count1
                         from SQL_time_bins
                         where Violation_Code in (21,38,14)
                         group by Violation_Code, Time_Bins
                         order by count1 desc")
head(comm_time)
# Violation_Code Time_Bins count1                                               
# 1             21   Morning 549632
# 2             38 Afternoon 234327
# 3             14   Morning 194938
# 4             38   Morning 177285
# 5             14 Afternoon 151441
# 6             21  Midnight  85902
#It can be infered that the time of day where the top 3 violated codes occur the most are
#Morning for 21
#Afternoon for 38
#Morning for 14

#6. Seasonality across issue years


Seasons <- SparkR::sql("SELECT Issue_Month,
                       Violation_Code,
                       CASE WHEN Issue_Month>=1 AND Issue_Month<=3
                       THEN 'Winter'
                       WHEN Issue_Month>3 AND Issue_Month<=6
                       THEN 'Summer'
                       WHEN Issue_Month>6 AND Issue_Month<=10
                       THEN 'Rainy'
                       WHEN Issue_Month>10 AND Issue_Month<=12
                       THEN 'Winter'
                       END AS Seasons
                       FROM SQL_nyc")

head(Seasons)

createOrReplaceTempView(Seasons,"Seasons_SQL")

Season_vio_codes <- SparkR::sql("select Seasons,count(Violation_Code) as vio_count
                                from Seasons_SQL
                                group by Seasons
                                order by vio_count desc")
head(Season_vio_codes)
# Seasons vio_count                                                             
# 1  Summer   2232049
# 2  Winter   2117032
# 3   Rainy      1042

#Then, find the three most common violations for each of these seasons.
Winter_vio_codes <- SparkR::sql("select Violation_Code,count(*) as vio_count
                                from Seasons_SQL
                                where Seasons='Winter'
                                group by Violation_Code
                                order by vio_count desc")

head(Winter_vio_codes)
# Violation_Code vio_count                                                      
# 1             21    307099
# 2             38    286721
# 3             14    223212
# 4             46    153626
# 5             37    151764
# 6             20    147610

#top 3 violated codes in Winter are  21,38,14

Summer_vio_codes <- SparkR::sql("select Violation_Code,count(*) as vio_count
                                from Seasons_SQL
                                where Seasons='Summer'
                                group by Violation_Code
                                order by vio_count desc")

head(Summer_vio_codes)
# Violation_Code vio_count                                                      
# 1             21    334835
# 2             38    254667
# 3             14    248654
# 4             20    168890
# 5             46    156312
# 6             40    141385
#top 3 violated codes in Summer are  21,38,14

Rainy_vio_codes <- SparkR::sql("select Violation_Code,count(*) as vio_count
                               from Seasons_SQL
                               where Seasons='Rainy'
                               group by Violation_Code
                               order by vio_count desc")
head(Rainy_vio_codes)
# Violation_Code vio_count                                                      
# 1             46       281
# 2             40       138
# 3             14       100
# 4             20        81
# 5             98        59
# 6             19        59
#top 3 violated codes in Raniy season are  46,40,14
#As we can see , the trend is 21,38,14 are the top violated codes in Summer and winter, with only 14 coming in top 3 in rainy season

#7. The fines collected from all the parking violation constitute a revenue source for the NYC police
#department. Let's take an example of estimating that for the three most commonly occurring codes.

#a. Find total occurrences of the three most common violation codes
Top_3_vio_code_count <- SparkR::sql("select Violation_Code, count(*) as count1
                                    from SQL_nyc
                                    group by Violation_Code
                                    order by count1 desc
                                    limit 3")

head(Top_3_vio_code_count)
# Violation_Code count1                                                         
# 1             21 641961
# 2             38 541397
# 3             14 471966
createOrReplaceTempView(Top_3_vio_code_count,"SQL_Top_3_vio_code_count")
printSchema(Top_3_vio_code_count)

total_sum <- SparkR::sql("select sum(count1)
                         from SQL_Top_3_vio_code_count")
head(total_sum)
#1655324

#Average fines for codes 21,28,14 are 55$,50$,115$ respectively

Top_3_vio_code_count_df <- data.frame(head(Top_3_vio_code_count))
Top_3_vio_code_count_df

Average_fines <- c(55,50,115)

Total_fines_per_codes <- Top_3_vio_code_count_df$count1 * Average_fines
Total_fines_per_codes

Top_3_vio_code_count_df$Total_fines <- Total_fines_per_codes
Top_3_vio_code_count_df
# Violation_Code count1 Total_fines
# 1             21 641961    35307855
# 2             38 541397    27069850
# 3             14 471966    54276090
#The highest collection of fines is for the 14th code, due to the fine being 115$ for each ticket

