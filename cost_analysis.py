import sqlite3
import pprint
import hashlib
import time
from functions import group_activity, milliseconds_to_date, encrypt_tuple, encrypt, flattened_index
import json
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import yaml

'''
Storage Requirements for Date and Time Types
Column type  Storage required
DATE         3 bytes
DATETIME     8 bytes
TIMESTAMP    4 bytes
TIME         3 bytes
YEAR         1 byte
'''

def retrive_data(date):
	get_data_query='SELECT tuple FROM activities WHERE date=?'

	data=list() #contiene tutti i json delle attivita' svolte nel giorno d
	for row in c.execute(get_data_query, (date,)):
		data.append(yaml.safe_load(row[0].replace("'", '"')))

	return data


def insert_in_db_clear(activity_list, date):
	#timer di inizio pre-processing
	processing_start=datetime.utcnow()

	#raggruppo i dati
	grouped_activity_list=group_activity(activity_list, threshold_list)

	n_byte=0

	insert_list=list()
	for i in grouped_activity_list:
		start_time=milliseconds_to_date(int(i['start'])).time().strftime('%H:%M:%S')
		insert_list.append((str(i), i['type'], date, start_time))

		n_byte=n_byte+len(str(i))+len(i['type'])+3
		#per ora ignoro il tempo perche' altrimenti non avrebbe senso il confronto con le altre implementazioni

	#timer di fine pre-processing ed inizio query
	processing_end=datetime.utcnow()

	insert_query='INSERT INTO activities_clear (tuple, type, date, time) VALUES (?, ?, ?, ?)'

	for i in insert_list:
		c.execute(insert_query, i)
	conn.commit()

	#timer di fine query
	query_end=datetime.utcnow()

	'''
	timer_list=list()
	timer_list.append(processing_start)
	timer_list.append(processing_end)
	timer_list.append(query_end)
	'''

	timer_tuple=(processing_start, processing_end, query_end)

	return timer_tuple, n_byte

def insert_in_db_direct(activity_list, date, key):
	#timer di inizio pre-processing
	processing_start=datetime.utcnow()

	#raggruppo i dati
	grouped_activity_list=group_activity(activity_list, threshold_list)

	n_byte=0

	insert_list=list()
	for i in grouped_activity_list:
		encrypted_data=encrypt_tuple(str(i), key)
		type_index=hashlib.sha1(encrypt(i['type'], key)).hexdigest()

		insert_list.append((encrypted_data, type_index, date))

		n_byte=n_byte+len(encrypted_data)+len(type_index)+3

	#timer di fine pre-processing ed inizio query
	processing_end=datetime.utcnow()

	hash_activities=hashlib.sha1('activities_direct').hexdigest()
	hash_type=hashlib.sha1('type').hexdigest()

	insert_query='INSERT INTO {} (etuple, {}, date) VALUES (?, ?, ?)'.format(hash_activities, hash_type)

	for i in insert_list:
		c.execute(insert_query, i)
	conn.commit()

	#timer di fine query
	query_end=datetime.utcnow()

	timer_tuple=(processing_start, processing_end, query_end)

	return timer_tuple, n_byte

def insert_in_db_direct_padding(activity_list, date, key, fixed_block=False):
	#timer di inizio pre-processing
	processing_start=datetime.utcnow()

	#raggruppo i dati
	grouped_activity_list=group_activity(activity_list, threshold_list)

	#calcolo la dimensione del blocco cifrato
	max_len=0

	if not fixed_block:
		for i in grouped_activity_list:
			if len(str(i))>max_len:
				max_len=len(str(i))
	else:
		max_len=2201

	n_byte=0

	insert_list=list()
	for i in grouped_activity_list:
		pad_len=max_len-len(str(i))
		padded_data=str(i)+('*'*pad_len)

		encrypted_data=encrypt_tuple(padded_data, key)
		type_index=hashlib.sha1(encrypt(i['type'], key)).hexdigest()

		insert_list.append((encrypted_data, type_index, date))

		n_byte=n_byte+len(encrypted_data)+len(type_index)+3

	#timer di fine pre-processing ed inizio query
	processing_end=datetime.utcnow()

	hash_activities=''
	hash_type=hashlib.sha1('type').hexdigest()
	insert_query=''

	if not fixed_block:
		hash_activities=hashlib.sha1('activities_direct_changing').hexdigest()
		insert_query='INSERT INTO [{}] (etuple, {}, date) VALUES (?, ?, ?)'.format(hash_activities, hash_type)
	else:
		hash_activities=hashlib.sha1('activities_direct_fixed').hexdigest()
		insert_query='INSERT INTO {} (etuple, {}, date) VALUES (?, ?, ?)'.format(hash_activities, hash_type)
	
	for i in insert_list:
		c.execute(insert_query, i)
	conn.commit()

	#timer di fine query
	query_end=datetime.utcnow()

	timer_tuple=(processing_start, processing_end, query_end)

	return timer_tuple, n_byte

def insert_in_db_flattened(activity_list, date, type_list, threshold_list, key, fixed_block=False, padding=True):
	#timer di inizio pre-processing
	processing_start=datetime.utcnow()

	encrypted_activity_list=list()
	if not padding:
		encrypted_activity_list=flattened_index(activity_list, type_list, threshold_list, key, d=date, padding=False)
		hash_activities=hashlib.sha1('activities_flattened').hexdigest()
	else:
		if not fixed_block:
			encrypted_activity_list=flattened_index(activity_list, type_list, threshold_list, key, d=date)
			hash_activities=hashlib.sha1('activities_changing_block').hexdigest()
		else:
			encrypted_activity_list=flattened_index(activity_list, type_list, threshold_list, key, d=date, block_size=1896)
			hash_activities=hashlib.sha1('activities_fixed_block').hexdigest()

	hash_type=hashlib.sha1('type').hexdigest()

	n_byte=0

	insert_list=list()
	for i in encrypted_activity_list:
		insert_list.append((str(i['etuple']), i[hash_type], date))

		n_byte=n_byte+len(i['etuple'])+len(i[hash_type])+3

	#timer di fine pre-processing ed inizio query
	processing_end=datetime.utcnow()

	insert_query=''
	if fixed_block or not padding:
		insert_query='INSERT INTO [{}] (etuple, {}, date) VALUES (?, ?, ?)'.format(hash_activities, hash_type)
	else:
		insert_query='INSERT INTO {} (etuple, {}, date) VALUES (?, ?, ?)'.format(hash_activities, hash_type)

	for i in insert_list:
		c.execute(insert_query, i)
	conn.commit()

	#timer di fine query
	query_end=datetime.utcnow()

	timer_tuple=(processing_start, processing_end, query_end)

	return timer_tuple, n_byte

def insert_in_db_encrypted(activity_list, date, key):
	#timer di inizio pre-processing
	processing_start=datetime.utcnow()

	#raggruppo i dati
	grouped_activity_list=group_activity(activity_list, threshold_list)

	n_byte=0

	insert_list=list()
	for i in grouped_activity_list:
		encrypted_data=encrypt_tuple(str(i), key)
		insert_list.append((encrypted_data, date))

		n_byte=n_byte+len(encrypted_data)+3

	#timer di fine pre-processing ed inizio query
	processing_end=datetime.utcnow()

	hash_activities=hashlib.sha1('activities_encrypted').hexdigest()

	insert_query='INSERT INTO [{}] (etuple, date) VALUES (?, ?)'.format(hash_activities)

	for i in insert_list:
		c.execute(insert_query, i)
	conn.commit()

	#timer di fine query
	query_end=datetime.utcnow()

	timer_tuple=(processing_start, processing_end, query_end)

	return timer_tuple, n_byte

def insert_in_db_encrypted_padding(activity_list, date, key, fixed_block=False):
	#timer di inizio pre-processing
	processing_start=datetime.utcnow()

	#raggruppo i dati
	grouped_activity_list=group_activity(activity_list, threshold_list)

	#aggiungo il padding
	max_len=0

	if not fixed_block:
		for i in grouped_activity_list:
			if len(str(i))>max_len:
				max_len=len(str(i))
	else:
		max_len=2368

	padded_activity_list=list()
	for i in grouped_activity_list:
		pad_len=max_len-len(str(i))

		activity=str(i)
		activity=activity+('*'*pad_len)
		encrypted_activity=encrypt_tuple(activity, key)

		padded_activity_list.append(encrypted_activity)

	n_byte=0

	insert_list=list()
	for i in padded_activity_list:
		insert_list.append((i, date))

		n_byte=n_byte+len(i)+3

	#timer di fine pre-processing ed inizio query
	processing_end=datetime.utcnow()

	if fixed_block:
		hash_activities=hashlib.sha1('activities_encrypted_fixed').hexdigest()
	else:
		hash_activities=hashlib.sha1('activities_encrypted_changing').hexdigest()

	insert_query='INSERT INTO [{}] (etuple, date) VALUES (?, ?)'.format(hash_activities)

	for i in insert_list:
		c.execute(insert_query, i)
	conn.commit()

	#timer di fine query
	query_end=datetime.utcnow()

	timer_tuple=(processing_start, processing_end, query_end)

	return timer_tuple, n_byte

def mean_cost(timer_list, byte_list):
	processing_time_list=list()
	query_time_list=list()

	for i in timer_list:
		processing_time_list.append((i[1]-i[0]).microseconds)
		query_time_list.append((i[2]-i[1]).microseconds)

	return np.mean(processing_time_list), np.mean(query_time_list), np.mean(byte_list)

def clear_tables():
	table_name=list()
	table_name.append('activities_clear') #clear
	table_name.append(hashlib.sha1('activities_direct').hexdigest()) #direct
	table_name.append(hashlib.sha1('activities_direct_changing').hexdigest()) #direct fixed block
	table_name.append(hashlib.sha1('activities_direct_fixed').hexdigest()) #direct changing block
	table_name.append(hashlib.sha1('activities_flattened').hexdigest()) #flattened
	table_name.append(hashlib.sha1('activities_changing_block').hexdigest()) #flattened changing block
	table_name.append(hashlib.sha1('activities_fixed_block').hexdigest()) #flattened fixed block
	table_name.append(hashlib.sha1('activities_encrypted').hexdigest()) #encrypted
	table_name.append(hashlib.sha1('activities_encrypted_changing').hexdigest()) #encrypted changing block
	table_name.append(hashlib.sha1('activities_encrypted_fixed').hexdigest()) #encrypted fixed block

	for i in table_name:
		clear_query=''

		if i[0].isalpha():
			clear_query='DELETE FROM {}'.format(i)
		else:
			clear_query='DELETE FROM [{}]'.format(i)

		c.execute(clear_query)
	conn.commit()


####################################################################################################


pp=pprint.PrettyPrinter(indent=2)

conn=sqlite3.connect('encrypted.db')
c=conn.cursor()

rand_key='\x13\xe3`X\x06J\x0b\x04\xeb\xc4\x82\xbdV\\\x83\xb7'

date_query='SELECT DISTINCT date FROM activities'

date_list=list()
for row in c.execute(date_query):
	date_list.append(row[0])

type_query='SELECT DISTINCT type FROM activities'

type_list=list()
for row in c.execute(type_query):
	type_list.append(row[0])

threshold_list=list()
for t in type_list:
	threshold_dict=dict()

	threshold_dict['activity']=t
	threshold_dict['threshold']=1800000*2

	threshold_list.append(threshold_dict)

clear_timer_list=list()
direct_timer_list=list()
changing_direct_timer_list=list()
fixed_direct_timer_list=list()
flattened_timer_list=list()
changing_block_timer_list=list()
fixed_block_timer_list=list()
encrypted_timer_list=list()
changing_encrypted_timer_list=list()
fixed_encrypted_timer_list=list()

clear_bytes_list=list()
direct_bytes_list=list()
changing_direct_bytes_list=list()
fixed_direct_bytes_list=list()
flattened_bytes_list=list()
changing_block_bytes_list=list()
fixed_block_bytes_list=list()
encrypted_bytes_list=list()
changing_encrypted_bytes_list=list()
fixed_encrypted_bytes_list=list()

for i in range(0, 30):
	print i

	#RIPULISCO TUTTE LE TABELLE PER REINSERIRE I DATI
	clear_tables()

	for d in date_list:
		#scarico i dati in chiaro non raggruppati (json: stessa cosa di se leggo il csv)
		#per ogni giorno, che e' come ricevere i dati di quello stesso giorno

		data_per_day=retrive_data(d)

		clear_timer, clear_bytes=insert_in_db_clear(data_per_day, d)
		clear_timer_list.append(clear_timer)
		clear_bytes_list.append(clear_bytes)

		#data_per_day=retrive_data(d)

		direct_timer, direct_bytes=insert_in_db_direct(data_per_day, d, rand_key)
		direct_timer_list.append(direct_timer)
		direct_bytes_list.append(direct_bytes)

		#data_per_day=retrive_data(d)

		changing_direct_timer, changing_direct_bytes=insert_in_db_direct_padding(data_per_day, d, rand_key)
		changing_direct_timer_list.append(changing_direct_timer)
		changing_direct_bytes_list.append(changing_direct_bytes)

		#data_per_day=retrive_data(d)

		fixed_direct_timer, fixed_direct_bytes=insert_in_db_direct_padding(data_per_day, d, rand_key, fixed_block=True)
		fixed_direct_timer_list.append(fixed_direct_timer)
		fixed_direct_bytes_list.append(fixed_direct_bytes)

		#data_per_day=retrive_data(d)

		flattened_timer, flattened_bytes=insert_in_db_flattened(data_per_day, d, type_list, threshold_list, rand_key, padding=False)
		flattened_timer_list.append(flattened_timer)
		flattened_bytes_list.append(flattened_bytes)

		#data_per_day=retrive_data(d)

		changing_block_timer, changing_block_bytes=insert_in_db_flattened(data_per_day, d, type_list, threshold_list, rand_key)
		changing_block_timer_list.append(changing_block_timer)
		changing_block_bytes_list.append(changing_block_bytes)

		#data_per_day=retrive_data(d)
		
		fixed_block_timer, fixed_block_bytes=insert_in_db_flattened(data_per_day, d, type_list, threshold_list, rand_key, fixed_block=True)
		fixed_block_timer_list.append(fixed_block_timer)
		fixed_block_bytes_list.append(fixed_block_bytes)

		#data_per_day=retrive_data(d)

		encrypted_timer, encrypted_bytes=insert_in_db_encrypted(data_per_day, d, rand_key)
		encrypted_timer_list.append(encrypted_timer)
		encrypted_bytes_list.append(encrypted_bytes)
		#print ((encrypted_timer[1] - encrypted_timer[0]).microseconds)
		#print encrypted_timer

		#data_per_day=retrive_data(d)

		changing_encrypted_timer, changing_encrypted_bytes=insert_in_db_encrypted_padding(data_per_day, d, rand_key)
		changing_encrypted_timer_list.append(changing_encrypted_timer)
		changing_encrypted_bytes_list.append(changing_encrypted_bytes)

		#data_per_day=retrive_data(d)

		fixed_encrypted_timer, fixed_encrypted_bytes=insert_in_db_encrypted_padding(data_per_day, d, rand_key, fixed_block=True)
		fixed_encrypted_timer_list.append(fixed_encrypted_timer)
		fixed_encrypted_bytes_list.append(fixed_encrypted_bytes)

#per ogni lista di timer: l[1]-l[0] e' il tempo di pre-processing, l[2]-l[1] e' il tempo di esecuzione della query
#calcolo una media di entrambi questi tempi per ciascun tipo di database
clear_cost=mean_cost(clear_timer_list, clear_bytes_list)
direct_cost=mean_cost(direct_timer_list, direct_bytes_list)
changing_direct_cost=mean_cost(changing_direct_timer_list, changing_direct_bytes_list)
fixed_direct_cost=mean_cost(fixed_direct_timer_list, fixed_direct_bytes_list)
flattened_cost=mean_cost(flattened_timer_list, flattened_bytes_list)
changing_block_cost=mean_cost(changing_block_timer_list, changing_block_bytes_list)
fixed_block_cost=mean_cost(fixed_block_timer_list, fixed_block_bytes_list)
encrypted_cost=mean_cost(encrypted_timer_list, encrypted_bytes_list)
changing_encrypted_cost=mean_cost(changing_encrypted_timer_list, changing_encrypted_bytes_list)
fixed_encrypted_cost=mean_cost(fixed_encrypted_timer_list, fixed_encrypted_bytes_list)

#istogrammi per rappresentare i risultati
#tempo di pre-procesing
pre_processing_time=list()
pre_processing_time.append(clear_cost[0])
pre_processing_time.append(direct_cost[0])
pre_processing_time.append(changing_direct_cost[0])
pre_processing_time.append(fixed_direct_cost[0])
pre_processing_time.append(flattened_cost[0])
pre_processing_time.append(changing_block_cost[0])
pre_processing_time.append(fixed_block_cost[0])
pre_processing_time.append(encrypted_cost[0])
pre_processing_time.append(changing_encrypted_cost[0])
pre_processing_time.append(fixed_encrypted_cost[0])

dpoints=np.array(pre_processing_time) #y

N=len(dpoints)
x=range(N) #x

plt.bar(x, pre_processing_time)
fig=plt.gcf()

plt.title('Pre-processing time')
plt.xlabel('Solutions')
plt.ylabel('Time (Microseconds)')

barlist=plt.bar(x, pre_processing_time)
color_list=['#246B62', '#2E8A62', '#3EBB64', '#76C247', '#C8D152', '#C8B25B', '#C88A5B', '#C86B5B', '#C75C67', '#C75C8A']
label_list=['Clear data', 'Direct index', 'Direct index with encrypted block size not fixed', 'Direct index with encrypted block size fixed', 'Flattened index', 'Flattened index with encrypted block size not fixed', 'Flattened index with encrypted block size fixed', 'Encrypted data', 'Encrypted data with encrypted block size not fixed', 'Encrypted data with encrypted block size fixed']

patch_list=list()
for i in range(0, len(pre_processing_time)):
	barlist[i].set_color(color_list[i])
	patch_list.append(mpatches.Patch(color=color_list[i], label=label_list[i]))

legend=plt.legend(handles=patch_list, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.savefig('pre_processing_time.png', bbox_extra_artists=(legend,), bbox_inches='tight')
plt.close(fig)

#tempo di esecuzione della query di inserimento
query_time=list()
query_time.append(clear_cost[1])
query_time.append(direct_cost[1])
query_time.append(changing_direct_cost[1])
query_time.append(fixed_direct_cost[1])
query_time.append(flattened_cost[1])
query_time.append(changing_block_cost[1])
query_time.append(fixed_block_cost[1])
query_time.append(encrypted_cost[1])
query_time.append(changing_encrypted_cost[1])
query_time.append(fixed_encrypted_cost[1])

dpoints=np.array(query_time) #y

N=len(dpoints)
x=range(N) #x

plt.bar(x, query_time)
fig=plt.gcf()

plt.title('Insert query execution time')
plt.xlabel('Solutions')
plt.ylabel('Time (Microseconds)')

barlist=plt.bar(x, query_time)

patch_list=list()
for i in range(0, len(query_time)):
	barlist[i].set_color(color_list[i])
	patch_list.append(mpatches.Patch(color=color_list[i], label=label_list[i]))

legend=plt.legend(handles=patch_list, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.savefig('insert_query_time.png', bbox_extra_artists=(legend,), bbox_inches='tight')
plt.close(fig)

#quantita' di byte inviati al database
byte_number=list()
byte_number.append(clear_cost[2])
byte_number.append(direct_cost[2])
byte_number.append(changing_direct_cost[2])
byte_number.append(fixed_direct_cost[2])
byte_number.append(flattened_cost[2])
byte_number.append(changing_block_cost[2])
byte_number.append(fixed_block_cost[2])
byte_number.append(encrypted_cost[2])
byte_number.append(changing_encrypted_cost[2])
byte_number.append(fixed_encrypted_cost[2])

dpoints=np.array(byte_number) #y

N=len(dpoints)
x=range(N) #x

plt.bar(x, byte_number)
fig=plt.gcf()

plt.title('Number of bytes sent to the server')
plt.xlabel('Solutions')
plt.ylabel('Bytes')

barlist=plt.bar(x, byte_number)

patch_list=list()
for i in range(0, len(byte_number)):
	barlist[i].set_color(color_list[i])
	patch_list.append(mpatches.Patch(color=color_list[i], label=label_list[i]))

legend=plt.legend(handles=patch_list, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.savefig('byte_number.png', bbox_extra_artists=(legend,), bbox_inches='tight')
plt.close(fig)

conn.close()