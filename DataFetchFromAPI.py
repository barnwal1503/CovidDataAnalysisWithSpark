import json
import requests


def getCovid19DataFromAPI():
    url = "https://covid-193.p.rapidapi.com/statistics"

    headers = {
        "X-RapidAPI-Host": "covid-193.p.rapidapi.com",
        "X-RapidAPI-Key": "420194fe1fmshbd66af44739ba1fp1b983djsn17ab9bf7c389"
    }
    url = "https://covid-193.p.rapidapi.com/statistics"

    headers = {
        "X-RapidAPI-Host": "covid-193.p.rapidapi.com",
        "X-RapidAPI-Key": "420194fe1fmshbd66af44739ba1fp1b983djsn17ab9bf7c389"
    }

    response = requests.request("GET", url, headers=headers)
    return response.text
