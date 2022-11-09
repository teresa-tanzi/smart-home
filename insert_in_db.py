import csv
import json
import datetime
from Crypto.Cipher import AES
import os
import hashlib
import sqlite3
from functions import csv_to_json
from functions import encrypt_tuple
from functions import encrypt

file_names=list()
file_names.append('..\dataset\logFreePerson1.csv')
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
file_names.append('..\dataset\logPerson10.1.csv')

for f in file_names:
	json_obj=csv_to_json(f)

	#rand_key=os.urandom(16)
	rand_key='\x13\xe3`X\x06J\x0b\x04\xeb\xc4\x82\xbdV\\\x83\xb7'

	conn=sqlite3.connect('encrypted.db')
	#conn.text_factory=str #serve per accettare caratteri 8-bit
	c=conn.cursor()

	for i in json_obj["activities"]:
		#cifratura della tupla
		en_tuple=encrypt_tuple(i, rand_key)

		#cifratura di type, start, end
		en_type=encrypt(i["type"], rand_key)
		en_start=encrypt(i["start"], rand_key)
		en_end=encrypt(i["end"], rand_key)

		#mappaggio dei nomi delle colonne con SHA224
		h_activities=hashlib.sha224('activities').hexdigest()
		h_type=hashlib.sha224('type').hexdigest()
		h_start=hashlib.sha224('start').hexdigest()
		h_end=hashlib.sha224('end').hexdigest()

		#query di inserimento nel DB
		query_tuple=[en_tuple, en_type, en_start, en_end]
		tuple_string=', '.join('?'*len(query_tuple))

		query="INSERT INTO "+h_activities+"('etuple','"+h_type+"','"+h_start+"','"+h_end+"') VALUES (%s);"%tuple_string
		
		c.execute(query, query_tuple)

	conn.commit()

conn.close()