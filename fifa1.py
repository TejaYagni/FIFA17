from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import DataFrame

conf = SparkConf().setAppName("FIFA")
sc = SparkContext(conf)
sqlContext = SQLContext(sc)

clubNames = sc.textFile('FIFA17/dataset/ClubNames.csv')
fullData = sc.textFile('FIFA17/dataset/FullData.csv')

columnNames = []
for i in fullData.take(1):
	columnNames.append(i)

columnNames = columnNames[0].split(",")
fullData = fullData.filter(lambda x: x.split(",")[1] !='Nationality')
fullData = fullData.map(lambda x: x.split(","))
fullData_df = sqlContext.createDataFrame(fullData,columnNames)			

#Number of players in roaster for each club ordered in decreasing order
playerCountByClub = fullData_df.map(lambda x: (x[4],1)).reduceByKey(lambda x,y : x+y).map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))

#Players by club 
playersByClub = fullData_df.map(lambda x: (x[4],x[0])).groupByKey().map(lambda x: (x[0],list(x[1])))

#Grouping teams, by number of players on roaster
clubsByNumberOfPlayers = playerCountByClub.map(lambda x: (x[1],x[0])).groupByKey().map(lambda x: (x[0],list(x[1])))

#number of clubs by number of players on roaster
numberOfClubsByNumberOfPlayers = clubsByNumberOfPlayers.map(lambda x: (x[0],len(x[1])))

printableInfoClubs = numberOfClubsByNumberOfPlayers.map(lambda x: str(x[1])+" teams have "+str(x[0])+" players. ")



#Number of players in roaster for each country ordered in decreasing order
playerCountByCountry = fullData_df.map(lambda x: (x[1],1)).reduceByKey(lambda x,y : x+y).map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))

#Grouping teams, by number of players on roaster
countriesByNumberOfPlayers = playerCountByCountry.map(lambda x: (x[1],x[0])).groupByKey().map(lambda x: (x[0],list(x[1])))

#number of counries by number of players on roaster
numberOfCountriesByNumberOfPlayers = countriesByNumberOfPlayers.map(lambda x: (x[0],len(x[1])))

printableInfoCountries = numberOfCountriesByNumberOfPlayers.map(lambda x: str(x[1])+" teams have "+str(x[0])+" player/players. ")


#Generates an array with one elemnt
#ageColumn = [i for i in range(0,len(columnNames)) if(columnNames[i] == "Age")]

#Will define a method

def getColumnNumber(colName):
	for i in range(0,len(columnNames)):
		if(columnNames[i] == colName):
			return i

ageColumn = getColumnNumber("Age")
clubColumn = getColumnNumber("Club")

#Average age of players in a club team

averageAge = fullData_df.map(lambda x: (x[clubColumn],int(x[ageColumn]))).map(lambda x: (x[0],(x[1],1))).aggregateByKey((0.0,0.0),lambda x,y: (x[0]+y[0],x[1]+y[1]), lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))

teamsOrderedByAverageAge = averageAge.map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))

#Rating Team based on average ratings of players

ratingColumn = getColumnNumber("Rating")

teamRatingBasedOnPlayerRating = fullData_df.map(lambda x: (x[clubColumn],(int(x[ratingColumn]),1))).aggregateByKey((0.0,0.0),lambda x,y: (x[0]+y[0],x[1]+y[1]), lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))

teamsOrderedByAveragePlayerRating = teamRatingBasedOnPlayerRating.map(lambda x:(x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))


#Most prefered JerseyNumber for club
clubKitColumn = getColumnNumber("Club_Kit")
nationKitColumn = getColumnNumber("National_Kit")

# -1 for the players with no jersey number

mostPreferedClubJersey = fullData_df.map(lambda x: (int(float(x[clubKitColumn])),1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))

mostPreferedNationalJersey = fullData_df.map(lambda x: (int(float(x[nationKitColumn])),1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1],x[0])).sortByKey(False).map(lambda x: (x[1],x[0]))

#Goalkeepers with better ball control i.e sweeper keepers 

nameColumn = getColumnNumber("Name")
clubColumn = getColumnNumber("Club")
ratingColumn = getColumnNumber("Rating")
ballControlColumn = getColumnNumber("Ball_Control")
positionColumn = getColumnNumber("Club_Position")

#List of keepers by sweeper keeper abilities as per FIFA 2017
sweeperKeepers = fullData_df.filter(fullData_df[positionColumn] == 'GK').map(lambda x: (int(x[ballControlColumn]),(x[nameColumn],x[clubColumn],x[ratingColumn]))).sortByKey(False)

#Now will use sql context
fullData_df.registerTempTable("fullData")

sweeperKeeperBySQL = sqlContext.sql("select Name from fullData_df where Club_Position = 'GK' order by GK_Kicking, Short_Pass, Long_pass, Heading, Ball_Control, Sliding_Tackle, Standing_Tackle, Interceptions,Crossing, Rating DESC")




