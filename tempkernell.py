# -*- coding: utf-8 -*-


pip install pyspark

from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
sc = SparkContext(appName="Kernel model")

def haversine(lon1, lat1, lon2, lat2):
  lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
# haversine formula
  dlon = lon2 - lon1
  dlat = lat2 - lat1
  a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
  c = 2 * asin(sqrt(a))
  km = 6367 * c
  return km

h_distance = 170 # Up to you
h_date = 10 # Up to you
h_time =  5 # Up to you
a = 58.4274 # Up to you
b = 14.826 # Up to you
date = "2013-07-04" # Up to you

# LOAD DATA 
#stations = sc.textFile("stations.csv")
stations = sc.textFile("station1.csv")

temps = sc.textFile("testtemp.csv")

# Line Sperate
LinesTemp = temps.map(lambda line: line.split(";"))
LinesStation = stations.map(lambda line: line.split(";"))
### Map the stations get STATION ID , LAT AND LONG 
StationInfo = LinesStation.map(lambda x: (   x[0],   (float(x[3]), float(x[4]))   ))

StationData = StationInfo.collectAsMap()
BroadcastStation = sc.broadcast(StationData)

# From Temp data get STATION ID , DATA , HOUR(Dont care about minutes) , TEMPERATURE , From BroadcastStation GET LONGITUDE and LATITUDE 
TempData = LinesTemp.map(lambda x: (  (x[0], x[1], int(x[2][0:2]))  ,  (float(x[3]), BroadcastStation.value.get(x[0])  )))



DateToFilterBY =  datetime(int(date[0:4]), int(date[5:7]), int(date[8:10]))
FinalTemp = TempData.filter(lambda x: (datetime(int(x[0][1][0:4]),int(x[0][1][5:7]), int(x[0][1][8:10]))<DateToFilterBY))
FinalTemp.cache()

## CALCULATE AND RETURN TIME DIFFERENCE BETWEEN TWO DAYS
# Expect form YEAR-MONTH-DAY
# Use DATETIME to get vector of Year - Month  - day 
# Find Distance between the dates
# Mode 365
# If the  is later then the middle of the year take 365 - Difference , else return the difference
def DayCalculation(day1, day2):
  date_1 = datetime.strptime(day1, "%Y-%m-%d")
  date_2 = datetime.strptime(day2, "%Y-%m-%d")

    
  Difference = (date_1 - date_2).days
  Difference = Difference % 365
  if Difference > 182:
      return 365-Difference
  else:
      return Difference

### Gausian KERNELL

def GausianKernel(U , H):
  return(exp(-((U/H)**2)))

# Count time between two Time points 
def TimeCalculation(time1, time2):
  
### ABS since negative time makes no sense
    TimeDifference = abs(int(time1[0:2]) - time2)
    
    if (TimeDifference > 12):
        return 24 - TimeDifference

    else:

        return TimeDifference

SumResults = []
ProdResults = []
for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00","12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
#for time in [24, 22, 20, 18, 16, 14, 12, 10, 8, 6, 4]:

  

# Take the TEMP data, take ID , Apply the kernl to 
  Estimate1 = FinalTemp.map( lambda x: (x[0], (GausianKernel( haversine(a,b,x[1][1][0], x[1][1][1]) , h_distance), GausianKernel(  DayCalculation(date, x[0][1])  , h_date),  GausianKernel(  TimeCalculation(time, x[0][2])  , h_time), x[1][0])))

  
  Estimate1 = Estimate1.map( lambda x: (1, ((x[1][0]+x[1][1]+x[1][2])*x[1][3], x[1][0]+x[1][1]+x[1][2], x[1][0]*x[1][1]*x[1][2]*x[1][3], x[1][0]*x[1][1]*x[1][2] )))
  Estimate1 = Estimate1.reduceByKey( lambda x1, x2: (x1[0]+x2[0], x1[1]+x2[1], x1[2]+x2[2], x1[3]+x2[3] ))
  Estimate1 = Estimate1.mapValues( lambda x: (x[0]/x[1], x[2]/x[3] ))


  # Separating the different kernel methods
  
  SumEstimate = Estimate1.collectAsMap().get(1)[0]
  ProdEstimate = Estimate1.collectAsMap().get(1)[1]
  SumResults.append((time, SumEstimate))
  ProdResults.append((time, ProdEstimate))

print(SumResults)

print(ProdResults)
