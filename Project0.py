from bs4 import BeautifulSoup
import re
import urllib.request
import pandas as pd
import sqlite3 as sq
import math
import matplotlib.pyplot as plt
website = "https://worldpopulationreview.com/country-rankings/pollution-by-country"

class webScrape:
    def __init__(self, url):
        self.url = url
        self.htmlsource = urllib.request.urlopen(self.url)
        self.soup = BeautifulSoup(self.htmlsource, "html.parser")
    def getData(self):

        myregex = "\{\"\w*\"\:\"(\w*\s?\w*?)\"\,\"\w*\"\:(\d*\.\d*)\,\"\w*\"\:(\w*?\d*?\.?\d*?)\,\"\w*\"\:(\d*)\,\"\w*\"\:(\d*)\}"
        self.data = re.findall(myregex, str(self.soup))

    def topTen(self):
        self.getData()
        self.tptenlist = list()
        for i in range(0, 10):
            self.tptenlist.append(self.data[i])
    def toDF(self):
        self.topTen()
        self.df = pd.DataFrame(self.tptenlist)


def sqliteconector(z):
    conn = sq.connect("4lab2.db")
    create_sql = "CREATE TABLE IF NOT EXISTS COUNTRIES (countries text,CO2_2020 REAL, CO2_2019 REAL,CO2mt2019 REAL, CO2Tons integer) "
    cursor = conn.cursor()
    cursor.execute(create_sql)
    for row in z.df.itertuples():

        insert_sql = f"INSERT INTO COUNTRIES (countries,CO2_2020,CO2_2019,CO2mt2019,CO2Tons) VALUES ('{row[1]}', {row[2]}, {row[3]}, {row[4]}, {row[5]})"
        cursor.execute(insert_sql)
    conn.commit()
    toDFQuery = pd.read_sql_query('''
                                       SELECT
                                       *
                                       FROM COUNTRIES
                                       ''', conn)
    df = pd.DataFrame(toDFQuery)
    countries = df["countries"].tolist()
    countries2 = [i for i in countries]
    lst1 = df["CO2_2020"].tolist()
    lst2 = df["CO2_2019"].tolist()
    lst3 = df["CO2mt2019"].tolist()
    lst4 = df["CO2Tons"].tolist()
    removelist = list()
    removelistcount = list()
    for r in range(len(lst2) - 1):
        if math.isnan(lst2[r]):
            removelist.append(lst2[r])
            del countries2[r]
        else:
            continue
    for i in removelist:
        lst2.remove(i)

    plotitems(countries, countries2, lst1, lst2, lst3, lst4)


def plotitems(countrylist, country2, firstplotlist, secondplotlist, thirdplotlist, fourthplotlist):
    plt.pie(firstplotlist, labels=countrylist)
    plt.show()
    plt.pie(secondplotlist, labels=country2)
    plt.show()
    plt.pie(thirdplotlist, labels=countrylist)
    plt.show()
    plt.pie(fourthplotlist, labels = countrylist)


def main():
    c = webScrape(website)
    c.toDF()
    sqliteconector(c)


main()
