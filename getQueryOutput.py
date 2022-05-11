from DataFrameObject import createDataFrame
from pyspark.sql.functions import col, lit, round, sum, min, max


def filterSomeInfo(covidData):
    filterContinent = covidData.filter((col("Country Name") != "All") & (col("Country Name") != "Europe")
                                       & (col("Country Name") != "Asia") & (col("Country Name") != "North-America")
                                       & (col("Country Name") != "South-America")
                                       & (col("Country Name") != "South Africa"))
    covidDataByMaxCases = filterContinent.orderBy(col("Total Covid Cases").desc()).limit(30)
    return covidDataByMaxCases


def mostAffectedCountryQuery(covidDataFile):
    res = covidDataFile.withColumn("Total Deaths/Total Covid Cases",
                                   lit(round(col("Total Deaths") / col("Total Covid Cases"), 4)))
    res = res.select(col("Country Name"),col("Total Deaths/Total Covid Cases")).orderBy(col("Total Deaths/Total Covid Cases").desc())
    res.write.mode("overwrite").csv("QueryOutput/Query1/output", header=True)
    return res


def leastAffectedCountryQuery(covidDataFile):
    res = covidDataFile.withColumn("Total Deaths/Total Covid Cases",
                                   lit(round(col("Total Deaths") / col("Total Covid Cases"), 4)))
    res = res.select(col("Country Name"),col("Total Deaths/Total Covid Cases")).orderBy(col("Total Deaths/Total Covid Cases"))
    res.write.mode("overwrite").csv("QueryOutput/Query2/output",header=True)
    return res


def countryWithMostCovidCases(covidDataFile):
    res = covidDataFile.select(col("Country name"), col("Total Covid Cases")).orderBy(
        col("Total Covid Cases").desc()).limit(1)
    res.write.mode("overwrite").csv("QueryOutput/Query3/output", header=True)
    return res


def countryWithLeastCovidCases(covidDataFile):
    res = covidDataFile.select(col("Country name"), col("Total Covid Cases")).orderBy(col("Total Covid Cases")).limit(1)
    res.write.mode("overwrite").csv("QueryOutput/Query4/output",header=True)
    return res


def totalCovidCases(covidDataFile):
    res = covidDataFile.select(sum('Total Covid Cases'))
    res.write.mode("overwrite").csv("QueryOutput/Query5/output",header=True)
    return res


def countryHandledCovidMostEfficiently(covidDataFile):
    res = covidDataFile.withColumn("Total Recovery/Total Covid Cases",
                                   lit(round(col("Total Recovered Covid Cases") / col("Total Covid Cases"), 4)))
    res = res.select(col("Country Name"),col("Total Recovery/Total Covid Cases")).orderBy(col("Total Recovery/Total Covid Cases").desc())
    res.write.mode("overwrite").csv("QueryOutput/Query6/output",header=True)
    return res


def countryHandledCovidLeastEfficiently(covidDataFile):
    res = covidDataFile.withColumn("Total Recovery/Total Covid Cases",
                                   lit(round(col("Total Recovered Covid Cases") / col("Total Covid Cases"), 4)))
    res = res.select(col("Country Name"),col("Total Recovery/Total Covid Cases")).orderBy(col("Total Recovery/Total Covid Cases"))
    res.write.mode("overwrite").csv("QueryOutput/Query7/output",header=True)
    return res


def mostNoOfCriticalCaseCountryQuery(covidDataFile):
    res = covidDataFile.select(col("Country name"), col("Total Critical Cases")).orderBy(
        col("Total Covid Cases").desc()).limit(1)
    res.write.mode("overwrite").csv("QueryOutput/Query8/output",header=True)
    return res


def leastNoOfCriticalCaseCountryQuery(covidDataFile):
    res = covidDataFile.select(col("Country name"), col("Total Critical Cases")).orderBy(
        col("Total Critical Cases")).limit(1)
    res.write.mode("overwrite").csv("QueryOutput/Query9/output",header=True)
    return res
