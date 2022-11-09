#!/usr/bin/python2
#-*- coding: utf-8 -*-

from flask import Flask, request
from flask_cors import CORS
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from json import dumps, loads
#from flask.ext.jsonify import jsonify
from functions import decrypt_tuple, encrypt, group_activity, split_time, time_to_milliseconds
import hashlib
from datetime import datetime
import pprint

pp=pprint.PrettyPrinter(indent=2)

PORT=1337

rand_key='\x13\xe3`X\x06J\x0b\x04\xeb\xc4\x82\xbdV\\\x83\xb7'

DB_NAME=hashlib.sha1('activities_flattened').hexdigest()
DB_TYPE=hashlib.sha1('type').hexdigest()

type_list=['Preparing Breakfast', 'Eating', 'Clean Up', 'Preparing Table', 'Preparing Meal', 'Taking Medicine', 'Watering Plants']
threshold_list=list()
for t in type_list:
	threshold_dict=dict()

	threshold_dict['activity']=str(t)
	threshold_dict['threshold']=1800000*2

	threshold_list.append(threshold_dict)

db_connect=create_engine('sqlite:///encrypted.db')

app=Flask(__name__)
CORS(app)
api=Api(app)

class Activities(Resource):
	def post(self):
		conn=db_connect.connect()

		query_param=dict()

		activityQuery = ""

		if(request.data):
			activityQuery = request.data
		else:
			activityQuery = request.form['data']

		if len(activityQuery)>0:
			query_param=loads(activityQuery)

		print 'parametri della query in chiaro:'
		pp.pprint(query_param)

		'''try:
			query_param=loads(activityQuery)
			print "try"
			print query_param
		except:
			query_param=dict()
			print "except"'''

		result_list=list()

		query='SELECT * FROM [{}]'.format(DB_NAME)

		if len(query_param)>0:
			#se nella richiesta sono specificati dei parametri aggiungo la clausola WHERE
			#if 'activityTypes' in query_param['query'].keys() or 'timeRange' in query_param['query'].keys() or 'dateRange' in query_param['query'].keys():
			if len(query_param['query']['activityTypes'])>0 or query_param['query']['dateRange'][0]['dateStart']!='' or query_param['query']['dateRange'][0]['dateEnd']!='':
				query=query+' WHERE	'

			#se è specificato l'array dei tipi richiesti, aggiungo pezzi alla query
			#if 'activityTypes' in query_param['query'].keys():
			if len(query_param['query']['activityTypes'])>0:
				for i in query_param['query']['activityTypes']:
					type_index=hashlib.sha1(encrypt(i, rand_key)).hexdigest()
					print 'type index per l\'attivita\' '+i+': '+type_index

					if i==query_param['query']['activityTypes'][0]:
						query=query+'({}="{}"'.format(DB_TYPE, type_index)
					else:
						query=query+' OR {}="{}"'.format(DB_TYPE, type_index)

					if i==query_param['query']['activityTypes'][-1]:
						query=query+')'

				#se oltre al tipo viene richiesto anche un range temporale, aggiungo AND
				#if 'dateRange' in query_param['query'].keys():
				if query_param['query']['dateRange'][0]['dateStart']!='' or query_param['query']['dateRange'][0]['dateEnd']!='':
					query=query+' AND '

			#timeRange per ora non mi interessa, ma servirà per il post processing
			#se viene specificato anche l'array delle date aggiungo i pezzi relativi alla query
			#if 'dateRange' in query_param['query'].keys():
			if query_param['query']['dateRange'][0]['dateStart']!='' or query_param['query']['dateRange'][0]['dateEnd']!='':
				for i in query_param['query']['dateRange']:
					#pero ora considero un array dotato di solo un elemento
					start=''
					end=''

					#TODO: modificare il significato di queste query
					#solo data di inizio voglio solo quel giorno
					#se dateStart è vuoto allora considero dateStart la epoch
					if query_param['query']['dateRange'][0]['dateStart']=='':
						start='1970-01-01'
					else:
						start=i['dateStart']
					#se dateEnd è vuoto allora considero come dateEnd oggi
					#cambio: se dateEnd è vuoto allora voglio solo quel giorno
					if query_param['query']['dateRange'][0]['dateEnd']=='':
						today=today=datetime.now().date()
						#end=str(today)
						end=i['dateStart']
					else:
						end=i['dateEnd']

					'''if query_param['query']['dateRange'][0]['dateStart']!='' and query_param['query']['dateRange'][0]['dateEnd']!='':
						start=i['dateStart']
						end=i['dateEnd']'''

					query=query+'date>="{}" AND date<="{}"'.format(start, end)

		print 'query cifrata: '+query
		query_exec=conn.execute(query)

		result_list.extend([dict(zip(tuple (query_exec.keys()) ,i)) for i in query_exec.cursor])
		
		print 'query cifrata in esecuzione'
		print 'la query ha restituito '+str(len(result_list))+' entry cifrate'

		#POST-PROCESSING
		#1. decifro i dati
		clear_list=list()
		fake_counter=0
		for i in result_list:
			clear_data=decrypt_tuple(i['etuple'], rand_key)

			if 'fake' not in clear_data.keys():
				clear_list.append(clear_data)
			else:
				fake_counter=fake_counter+1

		print 'decifratura dei risultati'
		print 'sono state trovate '+str(fake_counter)+' attivita\' fake'

		#2. raggruppo le attività che in precedenza erano sate divise
		grouped_list=group_activity(clear_list, threshold_list)

		print 'le attivita\' divise vengono riunite tra loro'
		print 'sono state trovate '+str(len(clear_list)-len(grouped_list))+' attivita\' divise'

		#3. filtro per gli orari, poiché non c'è un indice per quello
		filtered_list=list()

		if len(query_param)>0:
			#if 'timeRange' in query_param['query'].keys():
			if query_param['query']['timeRange'][0]['timeStart']!='' or query_param['query']['timeRange'][0]['timeEnd']!='':
				#considero ancora l'array dei tempi sempre di lunghezza uguale ad 1
				start=0
				end=0

				#se non c'è start considero start l'orario 00.00 -> 0
				if query_param['query']['timeRange'][0]['timeStart']=='':
					#start=0
					start=time_to_milliseconds(datetime.strptime('00:00:00', '%H:%M:%S'))
				else:
					start=time_to_milliseconds(datetime.strptime(query_param['query']['timeRange'][0]['timeStart'], '%H:%M:%S'))
					
				#se non c'è end considero end le 23.59 -> 82799000-3600000
				if query_param['query']['timeRange'][0]['timeEnd']=='':
					#end=82799000-3600000
					end=time_to_milliseconds(datetime.strptime('23:59:59', '%H:%M:%S'))
				else:
					end=time_to_milliseconds(datetime.strptime(query_param['query']['timeRange'][0]['timeEnd'], '%H:%M:%S'))

				'''if query_param['query']['timeRange'][0]['timeStart']!='' and query_param['query']['timeRange'][0]['timeEnd']!='':
					start=time_to_milliseconds(datetime.strptime(query_param['query']['timeRange'][0]['timeStart'], '%H:%M:%S'))
					end=time_to_milliseconds(datetime.strptime(query_param['query']['timeRange'][0]['timeEnd'], '%H:%M:%S'))'''

				print 'le attivita\' vengono filtrate per l\'indice temporale: inizio: '+str(start)+', fine: '+str(end)

				#controllo per ogni risultato ottenuto se soddisfa questa condizione
				for i in grouped_list:
					activity_start=split_time(int(i['start']))
					start_date=activity_start[0]
					start_time=activity_start[1]

					activity_end=split_time(int(i['end']))
					end_date=activity_end[0]
					end_time=activity_end[1]

					if start_time>=start and end_time<=end:
						filtered_list.append(i)

			else:
				filtered_list=grouped_list

			print 'alla fine del post processing si ottengono '+str(len(filtered_list))+' attivita\''

		else:
			filtered_list=grouped_list

		print 'le attivita\' in chiaro vengono inviate al client'
		return {'activities': filtered_list}

api.add_resource(Activities, '/activities')

if __name__=='__main__':
	app.run(debug=True, port=PORT, host='0.0.0.0')
	 #app.run(debug=True, port=PORT)
	print "Server started at port: "+PORT

