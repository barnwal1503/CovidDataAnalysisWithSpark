from DataFetchFromAPI import getCovid19DataFromAPI
from DataCleaningAndProcessing import cleanAndProcessingData
from CreateAndLoadDataCSVFile import createAndLoadDataInCSV
from DataFrameObject import createDataFrame, readCSVFile
from pyspark.sql.functions import col, lit, min, max, sum
from getQueryOutput import *

fetchedDataFromAPI = getCovid19DataFromAPI()

print(fetchedDataFromAPI)

covidDataCountryBased = cleanAndProcessingData(fetchedDataFromAPI)

for val in covidDataCountryBased:
    print(val)
covidDataCountryBased[0].keys()
csvfile = createAndLoadDataInCSV(covidDataCountryBased)

spark = createDataFrame()

covidData = readCSVFile(spark)

covidDataCountryWise = filterSomeInfo(covidData)

print(covidDataCountryWise.printSchema())
print("Country Wise Covid Data")
print(covidDataCountryWise.show())

print("Query 1 :  Most affected country among all the countries (total death/total covid cases).")
mostAffectedCountryResult = mostAffectedCountryQuery(covidDataCountryWise)
print(mostAffectedCountryResult.show())

print("Query 2 :  Least affected country among all the countries (total death/total covid cases).")
leastAffectedCountryResult = leastAffectedCountryQuery(covidDataCountryWise)
print(leastAffectedCountryResult.show())

print("Query 3 : Country With Most Covid Cases")
countryWithMaxCovidCases = countryWithMostCovidCases(covidDataCountryWise)
print(countryWithMaxCovidCases.show())

print("Query 4 : Country With Least Covid Cases")
countryWithMinCovidCases = countryWithLeastCovidCases(covidDataCountryWise)
print(countryWithMinCovidCases.show())

print("Query 5 : Total Covid Cases")
totalCovidCases = totalCovidCases(covidDataCountryWise)
print(totalCovidCases.show())

print("Query 6 : Country that handled the covid most efficiently(Total Recovery/total covid cases).")
countryWithMostEfficientCovidHandle = countryHandledCovidMostEfficiently(covidDataCountryWise)
print(countryWithMostEfficientCovidHandle.show())

print("Query 7 : Country that handled the covid least efficiently(Total Recovery/Total Covid cases).")
countryWithLeastEfficientCovidHandle = countryHandledCovidLeastEfficiently(covidDataCountryWise)
print(countryWithLeastEfficientCovidHandle.show())

print("Query 8 : Country With Most Critical Cases")
countryWithMinCriticalCases = mostNoOfCriticalCaseCountryQuery(covidDataCountryWise)
print(countryWithMinCriticalCases.show())

print("Query 9 : Country With Least Critical Cases")
countryWithMaxCriticalCases = leastNoOfCriticalCaseCountryQuery(covidDataCountryWise)
print(countryWithMaxCriticalCases.show())
