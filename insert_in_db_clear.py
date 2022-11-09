import csv
import json
import datetime
import sqlite3
from functions import csv_to_json
from functions import milliseconds_to_date
import datetime

file_names=list()
'''file_names.append('..\dataset\logFreePerson1.csv')
file_names.append('..\dataset\logFreePerson2.csv')
file_names.append('..\dataset\logFreePerson3.csv')
file_names.append('..\dataset\logFreePerson4.csv')
file_names.append('..\dataset\logFreePerson5.csv')
file_names.append('..\dataset\logFreePerson6.csv')
file_names.append('..\dataset\logFreePerson7.csv')
file_names.append('..\dataset\logFreePerson8.csv')
file_names.append('..\dataset\logFreePerson9.csv')
file_names.append('..\dataset\logFreePerson10.csv')
file_names.append('..\dataset\logPerson1.csv')
file_names.append('..\dataset\logPerson2.csv')
file_names.append('..\dataset\logPerson3.csv')
file_names.append('..\dataset\logPerson4.csv')
file_names.append('..\dataset\logPerson5.csv')
file_names.append('..\dataset\logPerson6.csv')
file_names.append('..\dataset\logPerson7.csv')
file_names.append('..\dataset\logPerson8.csv')
file_names.append('..\dataset\logPerson9.csv')
file_names.append('..\dataset\logPerson10.csv')
file_names.append('..\dataset\logPerson1.1.csv')
file_names.append('..\dataset\logPerson2.1.csv')
file_names.append('..\dataset\logPerson3.1.csv')
file_names.append('..\dataset\logPerson4.1.csv')
file_names.append('..\dataset\logPerson5.1.csv')
file_names.append('..\dataset\logPerson6.1.csv')
file_names.append('..\dataset\logPerson7.1.csv')
file_names.append('..\dataset\logPerson8.1.csv')
file_names.append('..\dataset\logPerson9.1.csv')
file_names.append('..\dataset\logPerson10.1.csv')'''

for i in range(0, 30):
	file_names.append('..\dataset\logDay'+str(i+1)+'.csv')

for j in file_names:
	#json_obj=csv_to_json(j)
	json_obj=csv_to_json(j, group=False)

	conn=sqlite3.connect('encrypted.db')
	c=conn.cursor()

	for i in json_obj["activities"]:
		tuple_=str(i)

		type_=i["type"]
		start=milliseconds_to_date(float(i["start"]))
		end=milliseconds_to_date(float(i["end"]))
		
		start_date=datetime.date(start.year, start.month, start.day)
		start_hour=datetime.time(start.hour, start.minute, start.second, start.microsecond)
		end_date=datetime.date(end.year, end.month, end.day)
		end_hour=datetime.time(end.hour, end.minute, end.second, end.microsecond)

		#query di inserimento nel DB
		query_tuple=[tuple_, type_, start_date, start_hour, end_hour]
		tuple_string=', '.join('?'*len(query_tuple))

		"""print query_tuple[1:]
		print start_date
		print start_hour
		print type(start_date)
		print type(start_hour)"""

		#query="INSERT INTO activities(tuple,type,[start date],[start time],[end date],[end time]) VALUES (%s);" %tuple_string
		#query='INSERT INTO activities_grouped(tuple, type, date, start_time, end_time) VALUES ('+\
		#	'"{}", "{}", date("{}"), time("{}"), time("{}"));'.format(tuple_, type_, start_date, start_hour, end_hour)
		
		query='INSERT INTO activities(tuple, type, date, start_time, end_time) VALUES ('+\
			'"{}", "{}", date("{}"), time("{}"), time("{}"));'.format(tuple_, type_, start_date, start_hour, end_hour)

		#print query
		#print query_tuple
		c.execute(query)

	conn.commit()

conn.close()