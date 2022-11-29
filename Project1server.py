import socket
import sqlite3 as sq
import pandas as pd

import pickle
statesList = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
              'Delaware', "District of Columbia", 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
              'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan',
              'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New-Hampshire',
              'New-Jersey', 'New-Mexico', 'New-York', 'North-Carolina', 'North-Dakota', 'Ohio', 'Oklahoma',
              'Oregon', 'Pennsylvania', 'Rhode-Island', 'South-Carolina', 'South-Dakota', 'Tennessee', 'Texas',
              'Utah', 'Vermont', 'Virginia', 'Washington', 'West-Virginia', 'Wisconsin', 'Wyoming']

def getDatafrmCSV():
    getdata = pd.read_csv("USAStatesCO2.csv", encoding="cp1252", skiprows=4)
    getdata = getdata.drop([51,52,53, 54, 55, 56, 57, 58])
    for i in range(0, 51):
        getdata.at[i, "Percent.1"] = getdata["Percent.1"].values[i][0:-1]
        getdata.at[i, "Percent"] = getdata["Percent"].values[i][0:-1]
    return getdata

def sqliteconector(z):
    conn = sq.connect("exdb.db")
    create_sql = "CREATE TABLE IF NOT EXISTS STATES (State text, a1970 REAL,a1971 REAL,a1972 REAL,a1973 " \
                 "REAL,a1974 REAL,a1975 REAL,a1976 REAL,a1977 REAL, a1978 REAL, a1979 REAL, a1980 REAL, " \
                 "a1981 REAL, a1982 REAL, a1983 REAL, a1984 REAL, a1985 REAL, a1986 REAL, a1987 REAL, " \
                 "a1988 REAL, a1989 REAL, a1990 REAL, a1991 REAL, a1992 REAL, a1993 REAL, a1994 REAL, " \
                 "a1995 REAL, a1996 REAL, a1997 REAL, a1998 REAL, a1999 REAL, a2000 REAL, a2001 REAL, " \
                 "a2002 REAL, a2003 REAL, a2004 REAL, a2005 REAL, a2006 REAL, a2007 REAL, a2008 REAL, " \
                 "a2009 REAL, a2010 REAL, a2011 REAL, a2012 REAL, a2013 REAL, a2014 REAL, a2015 REAL, " \
                 "a2016 REAL, a2017 REAL, a2018 REAL, a2019 REAL, a2020 REAL, Percent1 REAL, Absolute1 REAl, " \
                 "Percent2 Real, Absolute2 REAL )"
    cursor = conn.cursor()
    cursor.execute(create_sql)
    for row in z.itertuples():

        insert_sql = f"INSERT INTO STATES (State, a1970, a1971, a1972, a1973, a1974, a1975, a1976, a1977, a1978, a1979, " \
                     f"a1980, a1981, a1982, a1983, a1984, a1985, a1986, a1987, a1988, a1989, a1990, a1991, a1992, a1993, a1994, " \
                     f"a1995, a1996, a1997, a1998, a1999, a2000, a2001, a2002, a2003, a2004, a2005, a2006, a2007, a2008, a2009, " \
                     f"a2010, a2011, a2012, a2013, a2014, a2015, a2016, a2017, a2018, a2019, a2020, Percent1, Absolute1, " \
                     f"Percent2, Absolute2) VALUES ('{row[1]}', " \
                     f"{row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, " \
                     f"{row[11]}, {row[12]}, {row[13]}, {row[14]}, {row[15]}, {row[16]}, {row[17]}, {row[18]}, {row[19]}, " \
                     f"{row[20]}, {row[21]}, {row[22]}, {row[23]}, {row[24]}, {row[25]}, {row[26]}, {row[27]}, {row[28]}, " \
                     f"{row[29]}, {row[30]}, {row[31]}, {row[32]}, {row[33]}, {row[34]}, {row[35]}, {row[36]}, {row[37]}, " \
                     f"{row[38]}, {row[39]}, {row[40]}, {row[41]}, {row[42]}, {row[43]}, {row[44]}, {row[45]}, {row[46]}, " \
                     f"{row[47]}, {row[48]}, {row[49]}, {row[50]}, {row[51]}, {row[52]}, {row[53]}, {row[54]}, {row[55]}, {row[56]})"
        cursor.execute(insert_sql)
    conn.commit()

def socketfunc():
    HEADERSIZE = 10
    conn = sq.connect("exdb.db")
    cursor = conn.cursor()
    host = socket.gethostname()
    port = 1024

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(3)

    while True:
        connection, address = s.accept()
        data = connection.recv(1024).decode()
        if not data:
            break
        index = statesList.index(str(data))
        query = "SELECT * FROM STATES"
        cursor.execute(query)
        data = cursor.fetchall()
        data = data[index]
        data = pickle.dumps(data)
        data = bytes(f"{len(data):<{HEADERSIZE}}", 'utf-8')+data
        connection.send(data)
    conn.close()




def main():
    dataframe = getDatafrmCSV()
    sqliteconector(dataframe)
    socketfunc()
main()
