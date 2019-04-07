from alpha_vantage.timeseries import TimeSeries
from pprint import pprint
import time
from datetime import date, datetime
import nltk
import sqlite3
from sqlalchemy import create_engine
import array
import pymysql

ts = TimeSeries(key='4MA98S67RU1VKMYX', output_format='pandas')
connection = sqlite3.connect("/home/Phil/store/Stock")
companies = ['dji', 'mmm', 'axp', 'aapl', 'ba', 'cat', 'cvx', 'csco', 'ko', 'dis', 'dwdp', 'xom', 'gs', 'hd',
             'ibm', 'intc', 'jnj', 'jpm', 'mcd', 'mrk', 'msft', 'nke', 'pfe', 'pg', 'trv', 'utx', 'unh', 'wmt', 'wba']

gcloudcon = pymysql.connect(host='127.0.0.1',
                             database='store',
                             user='SQLUser',
                             password='3dWHUFePz9dHkFn')
cur = gcloudcon.cursor()
data, meta_data = ts.get_intraday(symbol="dji", interval='30min', outputsize='full')

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
            time.sleep(5)


    print("Got data")
    time.sleep(2)

    for entry in data.iterrows():

        items = array.array('f', entry[1])
        x = 0;

        for item in entry[1].iteritems():
            items[x] = item[1]
            x += 1
        # try adding this row to the local db or update with new values if its already there
        try:
            query = "INSERT INTO {0} VALUES (?,?,?,?,?,?)".format(company)
            connection.execute(query, [entry[0], items[0], items[1], items[2], items[3], items[4]])

        except sqlite3.IntegrityError:
            query = "UPDATE  SET '1. open'=(?), '2. high'=(?), '3. low'=(?), '4. close'=(?)," \
                    "'5. volume'=(?)  WHERE 'date' = (?)"
            query = query[:7] + company + query[7:]
            connection.execute(query, [items[0], items[1], items[2], items[3], items[4], entry[0]])

        #cloud db
        try:
            query = "INSERT INTO {0} VALUES ('{1}','{2}','{3}','{4}','{5}','{6}')"\
                .format(company, entry[0], items[0], items[1], items[2], items[3], items[4])
            cur.execute(query)
        except:
            query = "UPDATE {0} SET open = '{1}', high = '{2}', low = '{3}', close = '{4}'," \
                    "volume = '{5}' WHERE date = '{6}'"\
                .format(company, items[0], items[1], items[2], items[3], items[4], entry[0])
            cur.execute(query)

        gcloudcon.commit()
        connection.commit()


connection.commit()
connection.close()
