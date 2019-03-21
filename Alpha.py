from alpha_vantage.timeseries import TimeSeries
from pprint import pprint
import time
from datetime import date, datetime
import nltk
import sqlite3
from sqlalchemy import create_engine
import array

ts = TimeSeries(key='4MA98S67RU1VKMYX', output_format='pandas')
connection = sqlite3.connect("/home/Phil/store/Stock")
companies = ['dji', 'mmm', 'axp', 'aapl', 'ba', 'cat', 'cvx', 'csco', 'ko', 'dis', 'dwdp', 'xom', 'gs', 'hd',
             'ibm', 'intc', 'jnj', 'jpm', 'mcd', 'mrk', 'msft', 'nke', 'pfe', 'pg', 'trv', 'utx', 'unh', 'vzv', 'wmt', 'wba']

for company in companies:
    print(company)
    # Get json object with the intraday data and another with  the call's metadata
    x = 0
    while x is 0:
        try:
            data, meta_data = ts.get_intraday(symbol=company, interval='30min', outputsize='full')
            x = 1
        except:
            print("polling")
            time.sleep(60)


    print("Got data")
    time.sleep(30)

    for entry in data.iterrows():

        items = array.array('f', entry[1])
        x = 0;

        for item in entry[1].iteritems():
            items[x] = item[1]
            x += 1
        # try adding this row to the db or update with new values if its already there
        try:
            query = "INSERT INTO  VALUES (?,?,?,?,?,?)"
            query = query[:12] + company + query[12:]
            connection.execute(query, [entry[0], items[0], items[1], items[2], items[3], items[4]])
            #print("added to db")

        except sqlite3.IntegrityError:
            #print("update hit")
            query = "UPDATE  SET '1. open'=(?), '2. high'=(?), '3. low'=(?), '4. close'=(?)," \
                    "'5. volume'=(?)  WHERE 'date' = (?)"
            query = query[:7] + company + query[7:]
            connection.execute(query, [items[0], items[1], items[2], items[3], items[4], entry[0]])

        connection.commit()

connection.commit()
connection.close()
