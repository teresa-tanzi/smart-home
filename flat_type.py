from functions import decrypt_tuple
from functions import encrypt_tuple
from functions import csv_to_json
from functions import find_m
from functions import flat_frequencies
from functions import encrypt
from functions import decrypt
import sqlite3
import hashlib
import collections
import os
import random
import pprint

pp=pprint.PrettyPrinter(indent=4)

rand_key='\x13\xe3`X\x06J\x0b\x04\xeb\xc4\x82\xbdV\\\x83\xb7'

#scarico tutti i type e ne calcolo le frequenze
conn=sqlite3.connect('encrypted.db')
c=conn.cursor()

h_activities=hashlib.sha224('activities').hexdigest()
h_type=hashlib.sha224('type').hexdigest()

query_get_all_types="SELECT * FROM %s" %(h_activities)

types=list()

for row in c.execute(query_get_all_types):
	types.append(decrypt(row[2], rand_key))

counter=collections.Counter(types)
occurrencies=counter.values()

#commento perche' funzioni lente e non ottimizzate
#m=find_m(counter)
#print m
#freq=flat_frequencies(counter, m)
#print freq

#inserisco a mano i risultati delle funzioni sopra
m=10
freq=[{'frequenza': 101, 'valori': (4, 1, 5), 'tipo': u'Clean Up'}, {'frequenza': 84, 'valori': (2, 0, 6), 'tipo': u'Taking Medicine'}, {'frequenza': 98, 'valori': (0, 1, 8), 'tipo': u'Eating'}, {'frequenza': 109, 'valori': (0, 1, 9), 'tipo': u'Preparing Meal'}, {'frequenza': 78, 'valori': (4, 2, 2), 'tipo': u'Preparing Table'}, {'frequenza': 38, 'valori': (2, 2, 0), 'tipo': u'Preparing Breakfast'}, {'frequenza': 30, 'valori': (1, 1, 1), 'tipo': u'Watering Plants'}]

#genero le chiavi
k=0

for i in freq:
	if sum(i['valori'])>k:
		k=sum(i['valori'])

key_list=list()

#for i in range(0, k):
#	key_list.append(os.urandom(16))

#tengo le chiavi generate una sola volta per evitare di creare inconsistenza ogni volta che rieseguo
key_list.append('\xf1\xf7\xfd\x98\xa4\x0f5\x15\x07\xfb\xbc`\x1cGTH')
key_list.append('a\xf0\x0fv\xcd\xb0z=\x99\x9e\xaf\xe0\x98\x94I\x84')
key_list.append('\x95U\x95\x0e.\xc5\x87\xfa\xfc\x0f&2\x90\xfcK\xaa')
key_list.append('\x851\x13\x8c$\xe6Kx\x08\xc8 \xb5\x11\x0b\xd8d')
key_list.append('\xb5a<4\n?\xb6{\x81\x13:\xd1\xf8n\x96D')
key_list.append('C\x19\x8e\xa4\xf1]\x07\x94\x87~\xae9\x8f\xaf\x06\xe9')
key_list.append('\xa5*=\xbb5\x84!\x12(+\xb7u\xc9\x1f\x07\x8c')
key_list.append('\xac\x08]\x10\xca\xc4\n3\x17\xca\x82\x99\x0c/4\xd2')
key_list.append('N\xdf\x1a\xf9e\xb2\n\xbd\x07\x16\xa8M\xae\xf9/w')
key_list.append('\xff\x18\xbb [\x06\xf6\x0e\xc2\x06\xd1\x99\x15\x99\xcdg')

#per ogni istanza di type
for f in freq:
	activity=f['tipo']
	frequency=f['frequenza']
	chunk=f['valori']

	#print (activity, frequency, chunk)

	#scarico tutti i counter dell'istanza type
	en_value=encrypt(f['tipo'], rand_key)
	query_type="SELECT counter FROM %s WHERE [%s]='%s'" %(h_activities, h_type, en_value)

	index_list=list()

	for row in c.execute(query_type):
		index_list.append(row[0])

	#randomizzo la lista di indici in modo tale da non cifrare con la stessa chiave quelli consecutivi l'un l'altro
	random.shuffle(index_list)

	#lista di liste ciascuna delle quali va salvata sul DB con una chiave diversa
	different_key_list=list()
	#sottolista di lunghezza m-1, m o m+1
	same_key_list=list()

	for i in range(0, len(index_list)):
		val=index_list.pop(0)
		same_key_list.append(val)

		#se ho meno di chunk[0] liste
		if len(different_key_list)<chunk[0]:
			#se la sottolista e' piena la inserisco in quella grande e la svuoto
			if len(same_key_list)==m-1:
				different_key_list.append(same_key_list)
				same_key_list=list()

		elif len(different_key_list)<chunk[0]+chunk[1]:
			if len(same_key_list)==m:
				different_key_list.append(same_key_list)
				same_key_list=list()

		else:
			if len(same_key_list)==m+1:
				different_key_list.append(same_key_list)
				same_key_list=list()

	#pp.pprint(different_key_list)

	#per ogni sottolista devo cifrare il type con la stessa chiave
	for i in range(0, len(different_key_list)):
		en_type=hashlib.sha1(encrypt(activity, key_list[i])).hexdigest()
		index_tuple=tuple(different_key_list[i])

		query_change_type="UPDATE %s SET [%s]='%s' WHERE counter IN %s" %(h_activities, h_type, en_type, index_tuple)
		#print query_change_type
		c.execute(query_change_type)
		conn.commit()

conn.close()