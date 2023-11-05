import pandas as pd
from datetime import date,datetime,timedelta
import random
import pyodbc

now = datetime.now()
now = datetime.timestamp(now)
now = datetime.fromtimestamp(now)
date_time = now.strftime('%Y-%m-%d %H:%M')



base = pd.read_csv('/home/b3r/Desktop/base_final.csv', sep = ",")

counts_id_alarme = base['id_alarme'].value_counts(normalize=True)
counts_severity = base['severity'].value_counts(normalize=True)
counts_code_fail = base['code_fail'].value_counts(normalize=True)
counts_location = base['location'].value_counts(normalize=True)

weights_id_alarme = counts_id_alarme.values
weights_severity = counts_severity.values
weights_code_fail = counts_code_fail.values
weights_location = counts_location.values

random_id_alarme = random.choices(counts_id_alarme.index, weights=weights_id_alarme)[0]
random_severity = random.choices(counts_severity.index, weights=weights_severity)[0]
random_code_fail = random.choices(counts_code_fail.index, weights=weights_code_fail)[0]
random_location = random.choices(counts_location.index, weights=weights_location)[0]

simulator = [{'date_time': date_time,'id_alarme': random_id_alarme,'status': "True",'severity':random_severity,'code_fail': random_code_fail, 'location':random_location}]

df = pd.DataFrame(simulator)
df['date_time'] = pd.to_datetime(df['date_time'])
df['id_alarme'] = df['id_alarme'].astype(str)

#conn = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}')
