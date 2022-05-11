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

