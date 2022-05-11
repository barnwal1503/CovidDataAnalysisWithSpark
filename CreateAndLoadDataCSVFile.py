import csv


def createAndLoadDataInCSV(data):
    new_file = open("InputData/covid_csv_file.csv", 'w')
    csv_writer = csv.writer(new_file, delimiter=',')

    csv_writer.writerow(data[0].keys())

    for i in range(1, len(data)):
        csv_writer.writerow(data[i].values())
