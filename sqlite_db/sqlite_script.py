#Sqlite fields
# NULL		NULL values mean missing information or unknown.
# INTEGER	Integer values are whole numbers (either positive or negative). An integer can have variable sizes such as 1, 2, 3, 4, or 8 bytes.
# REAL		Real values are real numbers with decimal values that use 8-byte floats.
# TEXT		TEXT is used to store character data. The maximum length of TEXT is unlimited. SQLite supports various character encodings.
# BLOB		BLOB stands for a binary large object that can be used to store any kind of data. The maximum size of BLOBs is unlimited.

import os
import sys
#adds the top level directory to the path
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)
import sqlite3
from functools import wraps
from templates import query_string as qs

class Sqlite_Connection:
	def __init__(self, db_path):
		self.conn = sqlite3.connect(db_path)
		self.cur = self.conn.cursor()

	@property
	def data_types(self):
		return ('NULL', 'INTEGER', 'REAL', 'TEXT', 'BLOB')

	def __enter__(self):
		return self

	def __exit__(self, exec_type, exc_val, traceback):
		self.conn.commit()
		self.conn.close()

	def _yield_row(f):
		'''Decorator to be used with functions that reuturn db table rows'''
		@wraps(f)
		def wrapper(*args, **kwars):
			rows = f(*args, **kwars)
			for row in rows:
				yield row
		return wrapper

	@_yield_row
	def all_tables(self):
		sql = qs.all_sqlite_tables()
		return self.cur.execute(sql)

	@_yield_row
	def db_fields(self, table):
		sql = qs.table_fields(table=table)
		return self.cur.execute(sql)

	def create_table(self, table_name='', fields=[], custom_sql=None):
		'''
		fields: a list of dicts like [{'field':'age', 'data_type':'INTEGER', 'extra':'NOT NULL'}, ...], 
				or a list of tuples like [('age', 'INTEGER', 'NOT NULL'), ('name', 'TEXT'), ...]
		custom_query_str: define a custom SQL query string
		'''
		if custom_sql:
			sql = custom_sql
		else:
			fields = self._check_fields(fields)
			sql = qs.create_table(table_name, fields)
			print(sql)
		self.cur.execute(sql)
		self.conn.commit()

	def drop_table(self, table):
		sql = qs.drop_table(table)
		try:
			self.cur.execute(sql)
		#if the table doesnt exist an operational error will occer
		except sqlite3.OperationalError:
			print(f'{table} does not exist')		

	def _check_fields(self, fields):
		#if its a list of tuples
		if {type(item) for item in fields} == {tuple}:
			return self._convert_tuple_to_dict(fields)
		#if its a list of dicts
		elif{type(item) for item in fields} == {dict}:
			return self._validate_dict_keys(fields)
		else:
			raise TypeError(("fields must be a list of dicts like [{'field':_, 'data_type':_, 'extra':_}, ...]"
							" or a list of tuples like [('field', 'data_type', 'extra'), ...]"))

	def _validate_dict_keys(self, fields):
		for item in fields:
			for key in item.keys():
				if key not in ['field', 'data_type', 'extra']:
					raise ValueError(f'Unknown key: {key}. must be either field, data_type, or extra')
			if item['data_type'].upper() not in self.data_types:
				raise ValueError(f'data_types must be one of {self.data_types}')
		return fields

	@staticmethod
	def _convert_tuple_to_dict(fields):
		'''fields: a list of tuples, where the first index of each tuple is a table field
					the second index is the data type, and the 
					(optional) third index includes any other info like 'NOT NULL or PRIMARY KEY'
					ex) [('age', 'INTEGER', 'NOT NULL'), ('name', 'TEXT'), ...]
		'''
		new_list = []
		for item in fields:
			if len(item) == 2:
				new_list.append({'field':item[0], 'data_type':item[1]})
			elif len(item) == 3:
				new_list.append({'field':item[0], 'data_type':item[1], 'extra':item[2]})
			else:
				raise ValueError('Ensure that each tuple containse 2-3 elements (field, data_type, extra)')
		return new_list

	def insert_into(self, table, fields=(), data=(), data_dict=None):
		'''
		table : the db table to insert into
		fields: tuple defining all sql fields to insert into the table
		data: single tuple, or list of tuples, where each index of the tuple coresponds to a field.
				field order and tuple order must be the same
		data_dict:	An Alternate way to supply data to the function. 
				   	If a value is provided, fields and data will be ignored 
					a dict of field,value pairs: {field1: [value1, value2, value3, ...], field2:[...],...}
					where each field should be a list of values (even if there is just one value)
		'''
		if data_dict:
			fields, data = self._dict_to_tuple_list(data_dict)
		else:
			fields, data = self._valid_field_data(fields, data)
		sql = qs.insert_into(table, fields, place_holder='?')
		# determin weather to execute one or many depending on the type
		if type(data) == tuple:
			self.cur.execute(sql, data)
		else:
			self.cur.executemany(sql, data)

	@staticmethod
	def _valid_field_data(fields, data):
		'''
		cheks that fields and data are the appropriate type before using them
		this does not check that types match up with the db schema
		'''
		if (type(fields) != tuple) or (type(data) != tuple and type(data) != list):
			raise TypeError('fields must be a tupel, while data can be a tuple or a list of tuples')
		if type(data) == tuple:
			if len(fields) != len(data):
				raise ValueError('Each field must correspond with a single data point')	
		else:
			if {type(item) for item in data} != {tuple}:
				raise TypeError('Data can be a tuple or a list of tuples')
			error_msg = ''
			for i, item in enumerate(data):
				if len(fields) != len(data[i]):
					error_msg += f'\nrow {i} has {len(fields)} feild(s) and {len(data[i])} inputs'
			if error_msg != '':
				raise ValueError(error_msg)
		return fields, data

	@staticmethod
	def _dict_to_tuple_list(data: dict):
		'''
		data: a dict of field, value pairs: {field1: [value1, value2, value3, ...], field2:...}
				where each field should be a list of values (even if there is only just one)
		'''
		fields = tuple(data.keys())
		#check that all the fields are the same length:
		for i, field in  enumerate(fields):
			if i == 0:
				lenght = len(data[field])
			if lenght != len(data[field]):
				raise ValueError('Ensure that each field has a list of data of the same lenght')
		all_data = []
		for row in range(len(data[fields[0]])):
			row_data = []
			for field in fields:
				row_data.append(data[field][row])
			all_data.append(tuple(row_data))
		return fields, all_data


	def select(self, custom_sql=None):
		'''
		would be nice to pass a list of tuples condition :
				--->['age > 25',...], [('age', '>', '25')], ['age', '25']

		'''
		pass

	def table_to_csv(self, row, csv_path):
		pass

	def csv_to_table(self, ):
		pass










if __name__ == '__main__':
	pass
	

