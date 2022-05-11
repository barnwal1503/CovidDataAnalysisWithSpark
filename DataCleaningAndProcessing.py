import json


def cleanAndProcessingData(fetcheddatadromapi):
    data = json.loads(fetcheddatadromapi)  # Data will be converted to dictionary format

    countryWiseCovidInformation = data["response"]

    headers = ["Country Name", "Population", "Continent", "Total Covid Cases", "Total Active Covid Cases",
               "Total Recovered Covid Cases", "Total Deaths", "Total Critical Cases"]
    countriesCovidData = []

    for val in countryWiseCovidInformation:
        covidinfo = val
        countryName = covidinfo["country"]

        countryPop = covidinfo['population']

        countryTotalCases = covidinfo["cases"]["total"]
        if countryTotalCases is None: countryTotalCases = 0

        countryContinent = covidinfo["continent"]

        countryNewCases = covidinfo["cases"]["new"]
        if countryName is None: countryName = 0

        countryActiveCases = covidinfo["cases"]["active"]
        if countryActiveCases is None: countryActiveCases = 0

        countryCriticalCases = covidinfo["cases"]["critical"]
        if countryCriticalCases is None: countryCriticalCases = 0

        countryRecoveredCases = covidinfo["cases"]["recovered"]
        if countryRecoveredCases is None: countryRecoveredCases = 0

        countryNewDeaths = covidinfo["deaths"]["new"]

        countryTotalDeaths = covidinfo["deaths"]["total"]
        if countryTotalDeaths is None: countryTotalDeaths = 0
        dictFormatData = {headers[0]: countryName, headers[1]: countryPop, headers[2]: countryContinent,
                          headers[3]: countryTotalCases, headers[4]: countryActiveCases,
                          headers[5]: countryRecoveredCases,
                          headers[6]: countryTotalDeaths, headers[7]: countryCriticalCases
                          }
        # print(countryName, countryPop, countryTotalCases, countryContinent, countryNewCases, countryActiveCases,
        # countryCriticalCases, countryRecoveredCases, countryNewDeaths, countryTotalDeaths)
        countriesCovidData.append(dictFormatData)

    return countriesCovidData
