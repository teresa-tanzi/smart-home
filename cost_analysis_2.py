#-*- coding: utf-8 -*-
import sqlite3
import pprint
import hashlib
from functions import decrypt_tuple, decrypt, encrypt, group_activity
import json
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import yaml
import time

def retrive_from_clear(type_list):
	timer_list=list()
	sensitivity_list=list()
	byte_list=list()

	for t in type_list:
		#timer di inizio query
		query_start=datetime.utcnow()

		retrive_query='SELECT tuple FROM activities_clear WHERE type=?'

		result_list=list()

		n_byte=0
		for row in c.execute(retrive_query, (t,)):
			result_list.append(json.loads(row[0].replace("'", '"')))
			n_byte=n_byte+len(row[0])

		byte_list.append(n_byte)

		#timer di fine query
		query_end=datetime.utcnow()

		#non c'è post processing
		processing_end=datetime.utcnow()

		all_positives=len(result_list)
		true_positives=all_positives
		sensitivity=true_positives/float(all_positives)
		sensitivity_list.append(sensitivity)

		timer_tuple=(query_start, query_end, processing_end)
		timer_list.append(timer_tuple)

	return timer_list, sensitivity_list, byte_list

def retrive_from_direct(type_list, key, padding=False, fixed=False):
	timer_list=list()
	sensitivity_list=list()
	byte_list=list()

	for t in type_list:
		#timer di inizio query
		query_start=datetime.utcnow()

		hash_activities=''
		if not padding:
			hash_activities=hashlib.sha1('activities_direct').hexdigest()
		elif not fixed:
			hash_activities=hashlib.sha1('activities_direct_changing').hexdigest()
		else:
			hash_activities=hashlib.sha1('activities_direct_fixed').hexdigest()

		hash_type=hashlib.sha1('type').hexdigest()
		type_index=hashlib.sha1(encrypt(t, key)).hexdigest()

		retrive_query='SELECT etuple FROM [{}] WHERE {}=?'.format(hash_activities, hash_type)

		result_list=list()

		n_byte=0
		for row in c.execute(retrive_query, (type_index,)):
			result_list.append(row[0])
			n_byte=n_byte+len(row[0])

		byte_list.append(n_byte)

		#timer di fine query
		query_end=datetime.utcnow()

		clear_result_list=list()

		#decifratura dei risultati
		for i in result_list:
			clear_result_list.append(decrypt_tuple(i, key))

		#timer di fine post-processing
		processing_end=datetime.utcnow()

		all_positives=len(result_list)
		#non ci sono falsi positivi
		true_positives=all_positives
		sensitivity=true_positives/float(all_positives)
		sensitivity_list.append(sensitivity)

		timer_tuple=(query_start, query_end, processing_end)
		timer_list.append(timer_tuple)

	return timer_list, sensitivity_list, byte_list

def retrive_from_flattened(type_list, key, threshold_list, padding=False, fixed=False):
	timer_list=list()
	sensitivity_list=list()
	byte_list=list()

	for t in type_list:
		#timer di inizio query
		query_start=datetime.utcnow()

		hash_activities=''
		if not padding:
			hash_activities=hashlib.sha1('activities_flattened').hexdigest()
		elif not fixed:
			hash_activities=hashlib.sha1('activities_changing_block').hexdigest()
		else:
			hash_activities=hashlib.sha1('activities_fixed_block').hexdigest()

		hash_type=hashlib.sha1('type').hexdigest()
		type_index=hashlib.sha1(encrypt(t, key)).hexdigest()

		retrive_query='SELECT etuple FROM [{}] WHERE {}=?'.format(hash_activities, hash_type)

		result_list=list()

		n_byte=0
		for row in c.execute(retrive_query, (type_index,)):
			result_list.append(row[0])
			n_byte=n_byte+len(row[0])

		byte_list.append(n_byte)

		#timer di fine query
		query_end=datetime.utcnow()

		all_positives=len(result_list)

		clear_result_list=list()
		for i in result_list:
			clear_data=decrypt_tuple(i, key)

			if 'fake' not in clear_data.keys():
				clear_result_list.append(clear_data)

		true_positives=len(clear_result_list)

		grouped_result_list=group_activity(clear_result_list, threshold_list)

		#timer di fine post-processing
		processing_end=datetime.utcnow()

		sensitivity=true_positives/float(all_positives)
		sensitivity_list.append(sensitivity)

		timer_tuple=(query_start, query_end, processing_end)
		timer_list.append(timer_tuple)

	return timer_list, sensitivity_list, byte_list

def retrive_from_encrypted(type_list, key, padding=False, fixed=False):
	timer_list=list()
	sensitivity_list=list()
	byte_list=list()

	for t in type_list:
		#timer di inizio query
		query_start=datetime.utcnow()

		hash_activities=''
		if not padding:
			hash_activities=hashlib.sha1('activities_encrypted').hexdigest()
		elif not fixed:
			hash_activities=hashlib.sha1('activities_encrypted_changing').hexdigest()
		else:
			hash_activities=hashlib.sha1('activities_encrypted_fixed').hexdigest()

		type_index=hashlib.sha1(encrypt(t, key)).hexdigest()

		retrive_query='SELECT etuple FROM [{}]'.format(hash_activities)

		result_list=list()

		n_byte=0
		for row in c.execute(retrive_query):
			result_list.append(row[0])
			n_byte=n_byte+len(row[0])

		byte_list.append(n_byte)

		#timer di fine query
		query_end=datetime.utcnow()

		all_positives=len(result_list)

		clear_result_list=list()
		for i in result_list:
			clear_data=decrypt_tuple(i, key)

			if clear_data['type']==t:
				clear_result_list.append(clear_data)

		true_positives=len(clear_result_list)

		#timer di fine post-processing
		processing_end=datetime.utcnow()

		sensitivity=true_positives/float(all_positives)
		sensitivity_list.append(sensitivity)

		timer_tuple=(query_start, query_end, processing_end)
		timer_list.append(timer_tuple)

	return timer_list, sensitivity_list, byte_list

def mean_cost(timer_list, sensitivity_list, byte_list):
	query_time_list=list()
	processing_time_list=list()
	
	for i in timer_list:
		query_time_list.append((i[1]-i[0]).microseconds)
		processing_time_list.append((i[2]-i[1]).microseconds)

	return np.mean(query_time_list), np.mean(processing_time_list), np.mean(sensitivity_list), np.mean(byte_list)


#############################################################################################


pp=pprint.PrettyPrinter(indent=2)

conn=sqlite3.connect('encrypted.db')
c=conn.cursor()

rand_key='\x13\xe3`X\x06J\x0b\x04\xeb\xc4\x82\xbdV\\\x83\xb7'

type_query='SELECT DISTINCT type FROM activities'

type_list=list()
for row in c.execute(type_query):
	type_list.append(row[0])

threshold_list=list()
for t in type_list:
	threshold_dict=dict()

	threshold_dict['activity']=str(t)
	threshold_dict['threshold']=1800000*2

	threshold_list.append(threshold_dict)

tot_clear_timer_list=list()
tot_direct_timer_list=list()
tot_changing_direct_timer_list=list()
tot_fixed_direct_timer_list=list()
tot_flattened_timer_list=list()
tot_changing_flattened_timer_list=list()
tot_fixed_flattened_timer_list=list()
tot_encrypted_timer_list=list()
tot_changing_encrypted_timer_list=list()
tot_fixed_encrypted_timer_list=list()

tot_clear_sensitivity_list=list()
tot_direct_sensitivity_list=list()
tot_changing_direct_sensitivity_list=list()
tot_fixed_direct_sensitivity_list=list()
tot_flattened_sensitivity_list=list()
tot_changing_flattened_sensitivity_list=list()
tot_fixed_flattened_sensitivity_list=list()
tot_encrypted_sensitivity_list=list()
tot_changing_encrypted_sensitivity_list=list()
tot_fixed_encrypted_sensitivity_list=list()

tot_clear_byte_list=list()
tot_direct_byte_list=list()
tot_changing_direct_byte_list=list()
tot_fixed_direct_byte_list=list()
tot_flattened_byte_list=list()
tot_changing_flattened_byte_list=list()
tot_fixed_flattened_byte_list=list()
tot_encrypted_byte_list=list()
tot_changing_encrypted_byte_list=list()
tot_fixed_encrypted_byte_list=list()

for i in range(0, 30):
	print i

	#db in chiaro
	clear_timer_list, clear_sensitivity_list, clear_byte_list=retrive_from_clear(type_list)
	tot_clear_timer_list.extend(clear_timer_list)
	tot_clear_sensitivity_list.extend(clear_sensitivity_list)
	tot_clear_byte_list.extend(clear_byte_list)

	#db direct
	direct_timer_list, direct_sensitivity_list, direct_byte_list=retrive_from_direct(type_list, rand_key)
	tot_direct_timer_list.extend(direct_timer_list)
	tot_direct_sensitivity_list.extend(direct_sensitivity_list)
	tot_direct_byte_list.extend(direct_byte_list)

	#db direct changing block
	changing_direct_timer_list, changing_direct_sensitivity_list, changing_direct_byte_list=retrive_from_direct(type_list, rand_key, padding=True)
	tot_changing_direct_timer_list.extend(changing_direct_timer_list)
	tot_changing_direct_sensitivity_list.extend(changing_direct_sensitivity_list)
	tot_changing_direct_byte_list.extend(changing_direct_byte_list)

	#db direct fixed block
	fixed_direct_timer_list, fixed_direct_sensitivity_list, fixed_direct_byte_list=retrive_from_direct(type_list, rand_key, padding=True, fixed=True)
	tot_fixed_direct_timer_list.extend(fixed_direct_timer_list)
	tot_fixed_direct_sensitivity_list.extend(fixed_direct_sensitivity_list)
	tot_fixed_direct_byte_list.extend(fixed_direct_byte_list)

	#db flattened
	flattened_timer_list, flattened_sensitivity_list, flattened_byte_list=retrive_from_flattened(type_list, rand_key, threshold_list)
	tot_flattened_timer_list.extend(flattened_timer_list)
	tot_flattened_sensitivity_list.extend(flattened_sensitivity_list)
	tot_flattened_byte_list.extend(flattened_byte_list)

	#db flattened changing block
	changing_flattened_timer_list, changing_flattened_sensitivity_list, changing_flattened_byte_list=retrive_from_flattened(type_list, rand_key, threshold_list, padding=True)
	tot_changing_flattened_timer_list.extend(changing_flattened_timer_list)
	tot_changing_flattened_sensitivity_list.extend(changing_flattened_sensitivity_list)
	tot_changing_flattened_byte_list.extend(changing_flattened_byte_list)

	#db flattened fixed block
	fixed_flattened_timer_list, fixed_flattened_sensitivity_list, fixed_flattened_byte_list=retrive_from_flattened(type_list, rand_key, threshold_list, padding=True, fixed=True)
	tot_fixed_flattened_timer_list.extend(fixed_flattened_timer_list)
	tot_fixed_flattened_sensitivity_list.extend(fixed_flattened_sensitivity_list)
	tot_fixed_flattened_byte_list.extend(fixed_flattened_byte_list)

	#db cifrato
	encrypted_timer_list, encrypted_sensitivity_list, encrypted_byte_list=retrive_from_encrypted(type_list, rand_key)
	tot_encrypted_timer_list.extend(encrypted_timer_list)
	tot_encrypted_sensitivity_list.extend(encrypted_sensitivity_list)
	tot_encrypted_byte_list.extend(encrypted_byte_list)

	#db cifrato changing block
	changing_encrypted_timer_list, changing_encrypted_sensitivity_list, changing_encrypted_byte_list=retrive_from_encrypted(type_list, rand_key, padding=True)
	tot_changing_encrypted_timer_list.extend(changing_encrypted_timer_list)
	tot_changing_encrypted_sensitivity_list.extend(changing_encrypted_sensitivity_list)
	tot_changing_encrypted_byte_list.extend(changing_encrypted_byte_list)

	#db cifrato fixed block
	fixed_encrypted_timer_list, fixed_encrypted_sensitivity_list, fixed_encrypted_byte_list=retrive_from_encrypted(type_list, rand_key, padding=True, fixed=True)
	tot_fixed_encrypted_timer_list.extend(fixed_encrypted_timer_list)
	tot_fixed_encrypted_sensitivity_list.extend(fixed_encrypted_sensitivity_list)
	tot_fixed_encrypted_byte_list.extend(fixed_encrypted_byte_list)

clear_cost=mean_cost(tot_clear_timer_list, tot_clear_sensitivity_list, tot_clear_byte_list)
direct_cost=mean_cost(tot_direct_timer_list, tot_direct_sensitivity_list, tot_direct_byte_list)
changing_direct_cost=mean_cost(tot_changing_direct_timer_list, tot_changing_direct_sensitivity_list, tot_changing_direct_byte_list)
fixed_direct_cost=mean_cost(tot_fixed_direct_timer_list, tot_fixed_direct_sensitivity_list, tot_fixed_direct_byte_list)
flattened_cost=mean_cost(tot_flattened_timer_list, tot_flattened_sensitivity_list, tot_flattened_byte_list)
changing_flattened_cost=mean_cost(tot_changing_flattened_timer_list, tot_changing_flattened_sensitivity_list, tot_changing_flattened_byte_list)
fixed_flattened_cost=mean_cost(tot_fixed_flattened_timer_list, tot_fixed_flattened_sensitivity_list, tot_fixed_flattened_byte_list)
encrypted_cost=mean_cost(tot_encrypted_timer_list, tot_encrypted_sensitivity_list, tot_encrypted_byte_list)
changing_encrypted_cost=mean_cost(tot_changing_encrypted_timer_list, tot_changing_encrypted_sensitivity_list, tot_changing_encrypted_byte_list)
fixed_encrypted_cost=mean_cost(tot_fixed_encrypted_timer_list, tot_fixed_encrypted_sensitivity_list, tot_fixed_encrypted_byte_list)

#istogrammi per rappresentare i risultati
#tempo di query
query_time=list()
query_time.append(clear_cost[0])
query_time.append(direct_cost[0])
query_time.append(changing_direct_cost[0])
query_time.append(fixed_direct_cost[0])
query_time.append(flattened_cost[0])
query_time.append(changing_flattened_cost[0])
query_time.append(fixed_flattened_cost[0])
query_time.append(encrypted_cost[0])
query_time.append(changing_encrypted_cost[0])
query_time.append(fixed_encrypted_cost[0])

#RISULTATI ASSURDI
pp.pprint(query_time)

dpoints=np.array(query_time) #y

N=len(dpoints)
x=range(N) #x

plt.bar(x, query_time)
fig=plt.gcf()

plt.title('Request query execution time')
plt.xlabel('Solutions')
plt.ylabel('Time (Microseconds)')

barlist=plt.bar(x, query_time)
color_list=['#246B62', '#2E8A62', '#3EBB64', '#76C247', '#C8D152', '#C8B25B', '#C88A5B', '#C86B5B', '#C75C67', '#C75C8A']
label_list=['Clear data', 'Direct index', 'Direct index with encrypted block size not fixed', 'Direct index with encrypted block size fixed', 'Flattened index', 'Flattened index with encrypted block size not fixed', 'Flattened index with encrypted block size fixed', 'Encrypted data', 'Encrypted data with encrypted block size not fixed', 'Encrypted data with encrypted block size fixed']

patch_list=list()
for i in range(0, len(query_time)):
	barlist[i].set_color(color_list[i])
	patch_list.append(mpatches.Patch(color=color_list[i], label=label_list[i]))

legend=plt.legend(handles=patch_list, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.savefig('request_query_time.png', bbox_extra_artists=(legend,), bbox_inches='tight')
plt.close(fig)

#tempo di esecuzione della query di inserimento
processing_time=list()
processing_time.append(clear_cost[1])
processing_time.append(direct_cost[1])
processing_time.append(changing_direct_cost[1])
processing_time.append(fixed_direct_cost[1])
processing_time.append(flattened_cost[1])
processing_time.append(changing_flattened_cost[1])
processing_time.append(fixed_flattened_cost[1])
processing_time.append(encrypted_cost[1])
processing_time.append(changing_encrypted_cost[1])
processing_time.append(fixed_encrypted_cost[1])

dpoints=np.array(processing_time) #y

N=len(dpoints)
x=range(N) #x

plt.bar(x, processing_time)
fig=plt.gcf()

plt.title('Post-processing time')
plt.xlabel('Solutions')
plt.ylabel('Time (Microseconds)')

barlist=plt.bar(x, processing_time)

patch_list=list()
for i in range(0, len(processing_time)):
	barlist[i].set_color(color_list[i])
	patch_list.append(mpatches.Patch(color=color_list[i], label=label_list[i]))

legend=plt.legend(handles=patch_list, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.savefig('post_processing_time.png', bbox_extra_artists=(legend,), bbox_inches='tight')
plt.close(fig)

#frazione dei veri positivi su tutti i positivi
sensitivity=list()
sensitivity.append(clear_cost[2])
sensitivity.append(direct_cost[2])
sensitivity.append(changing_direct_cost[2])
sensitivity.append(fixed_direct_cost[2])
sensitivity.append(flattened_cost[2])
sensitivity.append(changing_flattened_cost[2])
sensitivity.append(fixed_flattened_cost[2])
sensitivity.append(encrypted_cost[2])
sensitivity.append(changing_encrypted_cost[2])
sensitivity.append(fixed_encrypted_cost[2])

dpoints=np.array(sensitivity) #y

N=len(dpoints)
x=range(N) #x

plt.bar(x, sensitivity)
fig=plt.gcf()

plt.title('Fraction of true positives')
plt.xlabel('Solutions')
plt.ylabel('TP/AP')

barlist=plt.bar(x, sensitivity)

patch_list=list()
for i in range(0, len(sensitivity)):
	barlist[i].set_color(color_list[i])
	patch_list.append(mpatches.Patch(color=color_list[i], label=label_list[i]))

legend=plt.legend(handles=patch_list, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.savefig('true_positives.png', bbox_extra_artists=(legend,), bbox_inches='tight')
plt.close(fig)

#byte scaricati dal server
byte_number=list()
byte_number.append(clear_cost[3])
byte_number.append(direct_cost[3])
byte_number.append(changing_direct_cost[3])
byte_number.append(fixed_direct_cost[3])
byte_number.append(flattened_cost[3])
byte_number.append(changing_flattened_cost[3])
byte_number.append(fixed_flattened_cost[3])
byte_number.append(encrypted_cost[3])
byte_number.append(changing_encrypted_cost[3])
byte_number.append(fixed_encrypted_cost[3])

dpoints=np.array(byte_number) #y

N=len(dpoints)
x=range(N) #x

plt.bar(x, byte_number)
fig=plt.gcf()

plt.title('Number of bytes downloaded from the server')
plt.xlabel('Solutions')
plt.ylabel('Bytes')

barlist=plt.bar(x, byte_number)

patch_list=list()
for i in range(0, len(byte_number)):
	barlist[i].set_color(color_list[i])
	patch_list.append(mpatches.Patch(color=color_list[i], label=label_list[i]))

legend=plt.legend(handles=patch_list, bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

plt.savefig('byte_download.png', bbox_extra_artists=(legend,), bbox_inches='tight')
plt.close(fig)

conn.close()