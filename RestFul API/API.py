from ConversionAndStructure import *
from DataCleaningAndProcessing import cleanAndProcessingData
from DataFetchFromAPI import getCovid19DataFromAPI
from DataFrameObject import createDataFrame, readCSVFile
from DataFrameToList import dfToRDD
from getQueryOutput import *
from flask import Flask, jsonify

str = """--------------------------------------------------------------------------------------------------------------------Type Following to get the results------------------------------------------------------------------------------------------------------------- 
        Type Query0 : Show the data fetched from API------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query1 : Most affected country among all the countries ( total death/total covid cases)-------------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query2 : Least affected country among all the countries ( total death/total covid cases)-----------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query3 : Country with highest Covid Cases------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query4 : Country with Least Covid Cases-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query5 : Total cases.--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query6 : Country that handled the covid most efficiently( total recovery/ total covid cases)-------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query7 : Country that handled the covid least efficiently( total recovery/ total covid cases)-----------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query8 : Country least suffering from covid ( least critical cases)---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Type Query9 : Country still suffering from covid (highest critical cases)--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        
        """
 
spark = createDataFrame()
covidData = spark.read.options(header='True', inferSchema='True').csv(
    "/Users/vikashkumarbarnwal/PycharmProjects/CovidAnalysisWithSpark/InputData/covid_csv_file.csv")

covidData = filterSomeInfo(covidData)

# print(type(response), response)

app = Flask(__name__)


@app.route('/')
def index():
    return str


@app.route('/Query1')
def mostAffectedCountry():
    queryDF = mostAffectedCountryQuery(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForCovidAffectedCountries(dataList)
    return response


@app.route('/Query2')
def leastAffectedCountry():
    queryDF = leastAffectedCountryQuery(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForCovidAffectedCountries(dataList)
    return response


@app.route('/Query3')
def countryHasMostCovidCase():
    queryDF = countryWithMostCovidCases(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForMostAndLeastNumbercases(dataList, "Maximum Covid Case")
    return response


@app.route('/Query4')
def countryHasLeastCovidCase():
    queryDF = countryWithLeastCovidCases(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForMostAndLeastNumbercases(dataList, "Minimum Covid Case")
    return response


@app.route('/Query5')
def totalCovidCasesAcrossCountries():
    queryDF = totalCovidCases(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForTotalCovidCases(dataList, "Total Covid Cases")
    return response


@app.route('/Query6')
def mostEfficientlyWorkedCountries():
    queryDF = countryHandledCovidMostEfficiently(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForEfficientHandlingCountries(dataList)
    return response


@app.route('/Query7')
def leastEfficientlyWorkedCountries():
    queryDF = countryHandledCovidLeastEfficiently(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForEfficientHandlingCountries(dataList)
    return response


@app.route('/Query8')
def countryHasMostCriticalCase():
    queryDF = mostNoOfCriticalCaseCountryQuery(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForMostAndLeastNumberCriricalCases(dataList, "Country With Max Critical Cases")
    return response


@app.route('/Query9')
def countryHasLeastCriticalCase():
    queryDF = leastNoOfCriticalCaseCountryQuery(covidData)
    dataList = DFToRDDToList(queryDF)
    response = responseDataForMostAndLeastNumberCriricalCases(dataList, "Country With Min Critical Cases")
    return response


if __name__ == "__main__":
    app.run(debug=True)
