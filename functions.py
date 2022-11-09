#-*- coding: utf-8 -*-
import csv
import json
import datetime
from Crypto.Cipher import AES
import os
import hashlib
import sqlite3
import base64
import random
from Crypto import Random
import time
from pyope.ope import OPE, ValueRange
import pprint
import yaml

pp=pprint.PrettyPrinter(indent=2)

#lettura del csv e trasformazione in json
def csv_to_json(filepath, group=True):
	with open(filepath, 'r') as file:
		file_reader=csv.reader(file, delimiter=',')
		f=list(file_reader)[1:] #la prima riga contiene i nomi delle colonne

	activities=dict()
	activity_list=list()
	activity=dict()
	event_list=list()
	manipulation_list=list()

	for row in f:    
		if row[2]=='Annotation':        
			if row[4]=='START':
				activity=dict()
				activity['type']=row[3]
				activity['start']=row[1]
				
				#ad ogni attivita' corrisponde una lista di eventi ed una di manipolazioni
				event_list=list()
				manipulation_list=list()
			
			if row[4]=='END':
				activity['end']=row[1]
				activity['events']=event_list
				activity['manipulations']=manipulation_list
				activity_list.insert(len(activity_list), activity)
				
		#enviroment -> events
		if row[2]=='Environment':
			event=dict()
			event['sensor']=row[3]
			event['state']=row[4]
			event['time']=row[1]
			
			event_list.insert(len(event_list), event)

		#sticker -> manipulation
		manipulation=dict()

		if row[2]=='Sticker':
			if row[4]=='START':
				#START e END di uno stesso sensore non e' detto che siano su righe consecutive del csv
				#ad ogni START creo un nuovo oggetto manipulation e lo inserisco subito
				manipulation=dict()
				manipulation['sensor']=row[3]
				manipulation['start']=row[1]

				manipulation_list.insert(len(manipulation_list), manipulation)

			if row[4]=='END':
				#cerco all'interno del json a quale START corrisponde questa END e vi aggiungo il tempo di fine
				for i in manipulation_list:
					if row[3]==i['sensor'] and row[1]>=i['start'] and 'end' not in i: #le righe dei csv sono in ordine temporale
						i['end']=row[1]		

	activities['activities']=activity_list

	#considero due attivita' appartenenti alla stessa istanza se distanti mezz'ora l'una dall'altra
	difference_time=1800000*2

	if group:
		group_list=list()

		for i in activities['activities']:
			group_entry=list()

			for j in activities['activities']:
				if i['type']==j['type'] and abs(int(j['start'])-int(i['start']))<=difference_time:
					group_entry.append(j)

			if group_entry not in group_list:
				group_list.append(group_entry)

		activities=dict()
		activity_list=list()

		for i in group_list:
			new_entry=dict()

			if len(i)==1:
				new_entry=i[0]
				
				segment_list=list()
				time_dict=dict()
				time_dict['start']=i[0]['start']
				time_dict['end']=i[0]['end']
				segment_list.append(time_dict)

				new_entry['segments']=segment_list

			else:
				new_entry['type']=i[0]['type']

				start_time=i[0]['start']
				end_time=i[0]['end']

				for j in i:
					if int(j['start'])<=int(start_time):
						start_time=j['start']
					if int(j['end'])>=int(end_time):
						end_time=j['end']

				new_entry['start']=start_time
				new_entry['end']=end_time

				new_event_list=list()

				for j in i:
					new_event_list=new_event_list+j['events']

				new_entry['events']=new_event_list

				new_manipulation_list=list()

				for j in i:
					new_manipulation_list=new_manipulation_list+j['manipulations']

				new_entry['manipulations']=new_manipulation_list

				segment_list=list()

				for j in i:
					time_dict=dict()
					time_dict['start']=j['start']
					time_dict['end']=j['end']

					segment_list.append(time_dict)

				new_entry['segments']=segment_list

			activity_list.append(new_entry)

		activities['activities']=activity_list

	return activities

#traduzione della data dai millisecondi al timestamp
def milliseconds_to_date(ms):
	time=datetime.datetime.fromtimestamp(ms/1000.0)
	return time

#divisione del tempo in data (POSIX) e tempo (millisecondi dalla mezzanotte)
def split_time(ms):
	t=milliseconds_to_date(ms)

	date=datetime.date(t.year, t.month, t.day)
	hour=datetime.time(t.hour, t.minute, t.second, t.microsecond)

	date_posix=time.mktime(date.timetuple()) #in secondi dal primo gennaio 1970 (POSIX)
	time_milliseconds=float(ms)-(date_posix*1000)#-3600000 #in millisecondi dalla mezzanotte

	return (date_posix, time_milliseconds)

#traduzione del tempo in millisecondi dalla mezzanotte
def time_to_milliseconds(t):
	return t.second*1000+t.minute*60*1000+t.hour*60*60*1000

#cifratura e decifratura dei dati
def pad(s):
	return s+((16-len(s)%16)*'*')

def encrypt_tuple(tupla, key):
	iv=Random.new().read(AES.block_size)
	cipher=AES.new(key, AES.MODE_CBC, iv)
	return base64.b64encode(iv+cipher.encrypt(pad(str(tupla))))

def decrypt_tuple(ciphertext, key):
	text=base64.b64decode(ciphertext)
	iv=text[:16]
	cipher=AES.new(key, AES.MODE_CBC, iv)
	dec=cipher.decrypt(text[16:]).decode('utf-8')
	l=dec.count('*')

	cleartext=""

	try:
		#se e' un json lo trasformo in tale
		cleartext=json.loads(dec[:len(dec)-l].replace("'", '"'))
	except:
		cleartext=dec[:len(dec)-l]

	return cleartext

def encrypt(text, key):
	chiper=AES.new(key)
	return base64.b64encode(chiper.encrypt(pad(str(text))))

def decrypt(ciphertext, key):
	chiper=AES.new(key)
	dec=chiper.decrypt(base64.b64decode(ciphertext)).decode('utf-8')
	l=dec.count('*')

	cleartext=""

	try:
		#se e' un json lo trasformo in tale
		cleartext=json.loads(dec[:len(dec)-l].replace("'", '"'))
	except:
		cleartext=dec[:len(dec)-l]

	return cleartext

#fase della giornata
def daytime(hour):
	daytime=''

	if 6<=hour<12:
		daytime='morning'
	elif 12<=hour<18:
		daytime='afternoon'
	elif 18<=hour<24:
		daytime='evening'
	else:
		daytime='night'

	return daytime

#frequenze ed occorrenze per il flattened index
def flat_frequencies(counter, m):
	m1=m-1
	m2=m
	m3=m+1

	freq=list() #valori numerici delle frequenze
	names=list() #tipo di attivita' che corrisponde alle varie frequenze

	for i in counter:
		freq.append(counter[i])
		names.append(i)

	key_list=list()

	for i in range(0, len(freq)):
		limit=min(freq)

		a=limit
		b=limit
		c=limit

		f=freq[i]
		n=names[i]

		for x in range(0, f):
			for y in range(0, f):
				for z in range(0, f):
					if (f==x*m1+y*m2+z*m3):
						#se trovo una somma piu' bassa la sostituisco
						if (x+y+z<a+b+c):
							a, b, c=x, y, z
						#se trovo una combinazione che da lo stesso risultato decido in maniera casuale se sostituire i valori oppure no
						elif (x+y+z==a+b+c):
							rand=random.choice([0, 1])
							if rand==1:
								a, b, c=x, y, z

		freq_split=dict()
		freq_split["tipo"]=n
		freq_split["frequenza"]=f
		freq_split["valori"]=(a, b, c)

		#bug da risolvere nel codice sopra
		if f==1:
			freq_split["valori"]=(1, 0, 0)			

		key_list.append(freq_split)

	return key_list

def flat_frequencies_2(counter, m):
	m1=m-1
	m2=m

	freq=list() #valori numerici delle frequenze
	names=list() #tipo di attivita' che corrisponde alle varie frequenze

	for i in counter:
		freq.append(counter[i])
		names.append(i)

	key_list=list()

	for i in range(0, len(freq)):
		a=min(freq)
		b=min(freq)

		f=freq[i]
		n=names[i]

		for x in range(0, f):
			for y in range(0, f):
				if (f==x*m1+y*m2):
					#se trovo una somma piu' bassa la sostituisco
					if (x+y<a+b):
						a, b=x, y
					#se trovo una combinazione che da lo stesso risultato decido in maniera casuale se sostituire i valori oppure no
					elif (x+y==a+b):
						rand=random.choice([0, 1])
						if rand==1:
							a, b=x, y

		freq_split=dict()
		freq_split["tipo"]=n
		freq_split["frequenza"]=f
		freq_split["valori"]=(a, b)

		key_list.append(freq_split)

	return key_list

def find_m(counter):
	freq=list() #valori numerici delle frequenze
	names=list() #tipo di attivita' che corrisponde alle varie frequenze

	for i in counter:
		freq.append(counter[i])
		names.append(i)

	limit=min(freq)

	for i in range(3, limit+3): #sommo 1 per includere l'estremo ed ancora 1 perche' posso fare combinazione lineare di m-1 
		indexes=flat_frequencies(counter, i)

		for j in indexes:
			if j["valori"]==(limit, limit, limit):
				return i-1

def order_encrypt(number, key):
	cipher=OPE(key, in_range=ValueRange(0, 86400), out_range=ValueRange(-999999999, 999999999))
	return cipher.encrypt(int(number/1000))

def order_decrypt(ciphernumber, key):
	cipher=OPE(key, in_range=ValueRange(0, 86400), out_range=ValueRange(-999999999, 999999999))
	return cipher.decrypt(ciphernumber)

def flattened_index(activity_list, type_list, group_threshold, key, d, index_frequency=0, block_size=0, padding=True):
	original_activity_list=activity_list

	flattened_activity_list=group_activity(activity_list, group_threshold)

	#contiene la frequenza di ogni indice all'interno della lista di attività
	frequency_per_type=list()

	for t in type_list:
		counter=0

		for a in flattened_activity_list:
			if a['type']==t:
				counter+=1

		frequency_dict=dict()
		frequency_dict['activity']=t
		frequency_dict['frequency']=counter
		frequency_per_type.append(frequency_dict)

	#posso impostare o meno la frequenza target
	if index_frequency==0:
		f_target=0

		for f in frequency_per_type:
			if f['frequency']>f_target:
				f_target=f['frequency']

	else:
		f_target=index_frequency

	for t in type_list:
		f_type=0

		for f in frequency_per_type:
			if f['activity']==t:
				f_type=f['frequency']

		while f_type<f_target:
			#controllo se in flattened_activity_list c'è un'attività di tipo t con una lista segments > 1
			divisible_activity=dict()

			for a in flattened_activity_list:
				if a['type']==t and len(a['segments'])>1:
					divisible_activity=a
			
			#se c'è un'attività con 2 o più segmenti allora la posso spezzare, altrimenti aggiungo delle attività false
			if len(divisible_activity)!=0:
				#rimuovo a dalla flattened_activity_list
				flattened_activity_list.remove(divisible_activity)

				#aggiungo alla flattened_activity_list le due nuove attività calcolate
				a, b=divide_activity(divisible_activity, original_activity_list, group_threshold)
				flattened_activity_list.append(a)
				flattened_activity_list.append(b)
			
			else:
				fake_activity=dict()
				fake_activity['type']=str(t)
				fake_activity['segments']=list()
				fake_activity['start']=0
				fake_activity['fake']='True'
				#non ha valori per gli altri indici perché ancora bisogna decidere come farli e se serve appiattimento

				flattened_activity_list.append(fake_activity)

			f_type=0

			for i in flattened_activity_list:
				if i['type']==t:
					f_type+=1

	#padding delle attività e creazione degli indici
	encrypted_activity_list=list()

	if block_size==0:
		max_size=0

		for i in flattened_activity_list:
			if len(str(i))>max_size:
				max_size=len(str(i))

	else:
		max_size=block_size

	#tutti i blocchi sono relativi allo stesso giorno: basta che guardo il primo
	#(altrimenti è complicato calcolare la data per quelli fake)
	#date=milliseconds_to_date(int(flattened_activity_list[0]['start'])).date()
	date=d

	for i in flattened_activity_list:
		encrypted_activity=dict()

		pad_len=0
		if padding:
			pad_len=max_size-len(str(i))
		else:
			pad_len=0

		if pad_len<0:
			pp.pprint(i)
			print d
			raise Exception('Dimensione del blocco insufficiente: '+str(pad_len))
		else:
			activity=str(i)
			activity=activity+('*'*pad_len)

			en=encrypt_tuple(activity, key)
			encrypted_activity['etuple']=en

			encrypted_activity['date']=str(date)

			#per l'indice di tipo devo prima creare un indice cifrando col la chiave e passando per SHA1
			type_hash=hashlib.sha1('type').hexdigest()
			type_index=hashlib.sha1(encrypt(i['type'], key)).hexdigest()
			encrypted_activity[type_hash]=type_index

			#l'indice del tempo per ora lo metto in chiaro, poi andrà nascosto
			time_hash=hashlib.sha1('time').hexdigest()
			start=milliseconds_to_date(int(i['start']))
			time_index=datetime.time(start.hour, start.minute, start.second)

			encrypted_activity[time_hash]=time_index

		encrypted_activity_list.append(encrypted_activity)


	return encrypted_activity_list

'''def group_activity(activity_list, group_threshold):
	group_list=list()

	for a in activity_list:
		group=list()

		for b in activity_list:
			if a['type']==b['type']:
				t=a['type']
				threshold=0

				for j in group_threshold:
					if j['activity']==t:
						threshold=j['threshold']

				if abs(int(a['start'])-int(b['start']))<=threshold:
					group.append(b)

		if group not in group_list:
			group_list.append(group)

	grouped_activity_list=list()

	for g in group_list:
		grouped_entry=dict()

		#il type è lo stesso di un qualsiasi elemento del gruppo
		grouped_entry['type']=g[0]['type']

		#lo start è il più basso tra gli start del gruppo
		#l'end è il più alto tra gli end del gruppo
		start_time=g[0]['start']
		end_time=g[0]['end']

		for i in g:
			if int(i['start'])<=int(start_time):
				start_time=i['start']
			if int(i['end'])>=int(end_time):
				end_time=i['end']

		grouped_entry['start']=start_time
		grouped_entry['end']=end_time

		#la lista degli eventi è la concatenazione delle liste degli eventi di ciascun elemento
		event_list=list()

		for i in g:
			event_list=event_list+i['events']

		grouped_entry['events']=event_list

		#la lista delle manipolazioni è la concatenazione delle manipolazioni di ciascun elemento
		manipulation_list=list()

		for i in g:
			manipulation_list=manipulation_list+i['manipulations']

		grouped_entry['manipulations']=manipulation_list

		#segments è la lista dei dizionari start-end di ciascun elemento
		segment_list=list()

		for i in g:
			time_dict=dict()
			time_dict['start']=i['start']
			time_dict['end']=i['end']

			segment_list.append(time_dict)

		grouped_entry['segments']=segment_list

		grouped_activity_list.append(grouped_entry)

	return grouped_activity_list'''

'''def group_activity(activity_list, group_threshold):
	#IDEA: new_activity_list=activity_list, faccio poi i miei conti e se non c'è la chiave 'segments la aggiungo'
	new_activity_list=list()
	to_remove_list=list()
	to_add_list=list()

	for a in activity_list:
		group_found=False

		for b in new_activity_list:
			#if b['start']=='1464775200813' or b['start']=='1464776639812' or b['start']=='1464777596109':
			
			if a['type']==b['type']:
				t=a['type']
				threshold=0

				for i in group_threshold:
					if i['activity']==t:
						threshold=i['threshold']

				if abs(int(a['start'])-int(b['start']))<=threshold and abs(int(a['start'])-int(b['start']))>0:
					group_found=True

					#tolgo b per sostituirlo con la sua versione a cui viene aggiunto a
					#se trovo un con len di segmnets > 1 sicuro c'è anche la versione con solo 1 e posso eliminare solo quella
					
					modify=b

					#cerco se in to_add_list c'è un'attività dello stesso tipo di b il sui start e end sono vicini a b
					#significa che devo modificare quella e non b
					for j in to_add_list:
						if j['type']==t and abs(int(j['start'])-int(b['start']))<=threshold:
							modify=j
							to_add_list.remove(j)

					if len(b['segments'])==1:
						to_remove_list.append(modify)

					old=modify

					for j in a['events']:
						if j not in modify['events']:
							modify['events'].append(j)

					for j in a['manipulations']:
						if j not in modify['manipulations']:
							modify['manipulations'].append(j)

					if a['start']<modify['start']:
						modify['start']=a['start']
					if a['end']>a['end']:
						modify['end']=a['end']

					segment_dict=dict()
					segment_dict['start']=a['start']
					segment_dict['end']=a['end']

					modify['segments'].append(segment_dict)

					#controllo in to_add_list se c'era già la sua versione senza a
					if old in to_add_list:
						to_add_list.remove(old)	

					to_add_list.append(modify)		

		if not group_found:
			segment_dict=dict()
			segment_dict['start']=a['start']
			segment_dict['end']=a['end']

			segment_list=list()
			segment_list.append(segment_dict)
			a['segments']=segment_list

			new_activity_list.append(a)

	for i in to_remove_list:
		new_activity_list.remove(i)
	for i in to_add_list:
		new_activity_list.append(i)

	return new_activity_list'''

def group_activity(activity_list, group_threshold):
	grouped_activity_list=list()

	for i in activity_list:
		group_found=False
		add_list=list()
		remove_list=list()

		for j in grouped_activity_list:
			if i['type']==j['type']:
				t=i['type']
				threshold=0

				for z in group_threshold:
					if z['activity']==t:
						threshold=z['threshold']

				time_difference=abs(int(i['start'])-int(j['start']))

				if time_difference<=threshold and time_difference>0:
					#le attività sono accorpabili
					group_found=True

					#rimuovo j dalla lista delle attività raggruppate per sostituirla con la sua versione accorpata
					remove_list.append(j)

					#accorpo j con i
					merged_activity=dict()

					start=0
					if i['start']<j['start']:
						start=i['start']
					else:
						start=j['start']
					merged_activity['start']=start

					end=0
					if i['end']>j['end']:
						end=i['end']
					else:
						end=j['end']
					merged_activity['end']=end

					merged_activity['type']=t

					event_list=list()
					event_list.extend(i['events'])
					event_list.extend(j['events'])
					merged_activity['events']=event_list

					manipulations_list=list()
					manipulations_list.extend(i['manipulations'])
					manipulations_list.extend(j['manipulations'])
					merged_activity['manipulations']=manipulations_list

					#j ha la lista dei segments, mentre a no
					segment_list=list()
					segment_list.extend(j['segments'])
					segment_dict=dict()
					segment_dict['start']=i['start']
					segment_dict['end']=i['end']
					segment_list.append(segment_dict)
					merged_activity['segments']=segment_list

					#aggiungo merged_activity alle attività da inserire nella lista
					add_list.append(merged_activity)

		if not group_found:
			new_activity=dict()
			new_activity['start']=i['start']
			new_activity['end']=i['end']
			new_activity['type']=i['type']
			new_activity['events']=i['events']
			new_activity['manipulations']=i['manipulations']

			#ci sono casi in cui mi arrivano attività che già hanno una lista segments: devo mantenere quella
			if 'segments' not in i.keys():
				segment_list=list()
				segment_dict=dict()
				segment_dict['start']=i['start']
				segment_dict['end']=i['end']
				segment_list.append(segment_dict)
				new_activity['segments']=segment_list
			else:
				new_activity['segments']=i['segments']

			grouped_activity_list.append(new_activity)

		else:
			for r in remove_list:
				grouped_activity_list.remove(r)
			for a in add_list:
				grouped_activity_list.append(a)

	return grouped_activity_list

def divide_activity(a, original_activity_list, group_threshold):
	segment_list=list()

	for i in original_activity_list:
		for segment in a['segments']:
			if segment['start']==i['start'] and segment['end']==i['end'] and a['type']==i['type']:
				segment_list.append(i)

	#pp.pprint(segment_list)
	#print '---'

	#trovo qual è il segmento di lunghezza maggiore
	max_segment=dict()

	for i in segment_list:
		if len(str(i))>len(str(max_segment)):
			max_segment=i

	new_segment_list=list()
	segment_dict=dict()
	segment_dict['start']=max_segment['start']
	segment_dict['end']=max_segment['end']
	new_segment_list.append(segment_dict)
	max_segment['segments']=new_segment_list

	#rimuovo tutte le sue informazioni da a
	#rigenero a come raggruppamento degli altri segmenti escluso max_segment
	new_group=list()

	for i in segment_list:
		if i!=max_segment:
			for j in original_activity_list:
				if j['start']==i['start'] and j['end']==i['end']:
					new_group.append(j)

	new_a=group_activity(new_group, group_threshold)

	#restituisco a modificato ed il segmento individuato
	return new_a[0], max_segment