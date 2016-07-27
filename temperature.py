import datetime
import sqlite3 as lite
import requests
import pandas as pd
from collections import defaultdict

end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=30)

api_key = "3c2c05f7bb4af88f75f72dfa3c9540ee"
url = 'https://api.forecast.io/forecast/' + api_key

cities = { "Atlanta": '33.762909,-84.422675',
            "LV": '36.229214,-115.26008',
            "SF": '37.727239,-123.032229',
            "Buffalo": '42.886448,-78.878372',
            "NYC": '40.663619,-73.938589'
        }
        
con = lite.connect('weather.db')
cur = con.cursor()

cities.keys()
with con:
    cur.execute('DROP TABLE IF EXISTS daily_temp')
    cur.execute('CREATE TABLE daily_temp (day_of_reading INT, Atlanta REAL, LV REAL, SF REAL, Buffalo REAL, NYC REAL);')
    
query_date = start_date

with con:
    while query_date < end_date:
        cur.execute('INSERT INTO daily_temp(day_of_reading) VALUES (?)', (int(query_date.strftime('%d')),))
        query_date += datetime.timedelta(days=1)

r = requests.get(url+'/42.886448,-78.878372'+','+query_date.strftime('%Y-%m-%dT12:00:00'))

for k,v in cities.items():
    query_date = start_date
    while query_date < end_date:
        #API query
        r = requests.get(url+'/'+v+','+query_date.strftime('%Y-%m-%dT12:00:00'))
        max_temp = r.json()['daily']['data'][0]['temperatureMax']
        
        with con:
            #insert the temperature max to the database
            cur.execute('UPDATE daily_temp SET ' + k + ' = ' + str(max_temp) + ' WHERE day_of_reading = ' + query_date.strftime('%d'))
            
        query_date += datetime.timedelta(days=1)
        
con.close()

#Now the analysis

con = lite.connect('weather.db')
cur = con.cursor()

#Reading sql query and putting it in a dataframe
df = pd.read_sql_query("SELECT * FROM daily_temp ORDER BY day_of_reading",con,index_col='day_of_reading')

city_list = []
for i in df.columns:
    city_list.append(i)
for city in city_list:
    temp_diff = df[city].max() - df[city].min()
    print ("Maximum temperature difference for "+ city +" in the recent month is " + str(temp_diff)+" F.\n")