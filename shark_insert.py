
import csv
from datetime import datetime, timedelta
import pyodbc


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()

sharkfile = r'c:\data\GSAF5.csv'

attack_dates = []
case = []
country = []
activity = []
age = []
gender = []
isfatal = []

with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader:
        case.append(row['Case Number'])
        attack_dates.append(row['Date'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])

data = zip(attack_dates, case, country, activity, age, gender, isfatal)

cur.execute('truncate table mvh.shark')

q = 'insert into mvh.shark (attack_date, case_number, country, activity, age, gender, isfatal) values (?, ?, ?, ?, ?, ?, ?)'

for d in data:
    try:
        cur.execute(q, d)
        conn.commit()
    except:
        conn.rollback()

