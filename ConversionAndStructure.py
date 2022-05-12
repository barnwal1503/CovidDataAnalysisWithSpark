def DFToRDDToList(dataDF):
    rdd1 = dataDF.rdd.map(lambda x: list(x))
    print(rdd1.collect())
    return rdd1


def responseDataForCovidAffectedCountries(rdd):
    i = 1
    response = {}
    for val in rdd.collect():
        response[i] = {"Country": val[0], "Total Deaths/Total Cases": val[1]}
        i = i + 1
    return response


def responseDataForMostAndLeastNumbercases(rdd, str):
    response = {}
    for val in rdd.collect():
        response[str] = {"Country": val[0], "Total Covid Cases": val[1]}
    return response


def responseDataForTotalCovidCases(rdd, str):
    response = {}
    for val in rdd.collect():
        response[str] = val[0]
    return response


def responseDataForEfficientHandlingCountries(rdd):
    i = 1
    response = {}
    for val in rdd.collect():
        response[i] = {"Country": val[0], "Total Recovery/Total Cases": val[1]}
        i = i + 1
    return response


def responseDataForMostAndLeastNumberCriricalCases(rdd, str):
    response = {}
    for val in rdd.collect():
        response[str] = {"Country": val[0], "# Critical Cases :": val[1]}
    return response


def responseDataCollectedFromAPI(rdd):
    i = 1
    response = {}
    for val in rdd.collect():
        response[i] = {"Country": val[0], "Population": val[1], "Continent": val[2], "Total Covid Cases": val[3],
                       "Total Active Covid Cases": val[4], "Total Recovered Covid Cases": val[5],
                       "Total Deaths": val[6], "Total Critical Cases": val[7]}
        i = i + 1
    return response
