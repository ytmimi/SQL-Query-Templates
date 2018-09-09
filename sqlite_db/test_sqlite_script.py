import os
import sys
#adds the top level directory to the path
PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH)
import sqlite3
import pytest
from sqlite_db.sqlite_script import Sqlite_Connection
from shutil import rmtree

@pytest.fixture(scope='module')
def set_up_db(request):
	temp_test_folder = os.path.join(PATH, 'temp')
	# adds the folder to the path
	os.makedirs(temp_test_folder)
	
	def tear_down():
		rmtree(temp_test_folder)

	request.addfinalizer(tear_down)
	return os.path.join(temp_test_folder, 'test.db')

@pytest.fixture(scope='module')
def drop_table_sql():
	def drop_table(table):
		return f'DROP TABLE {table}'
	return drop_table

@pytest.fixture(scope='module')
def select_all():
	def select_table(table):
		return f'SELECT * FROM {table}'
	return select_table

@pytest.fixture(scope='module')
def new_table_fields():
	#returns a list of dicts
	return [
				{
				'field':'id', 
				'data_type':'INTEGER',
				'extra':'NOT NULL'},
				{
				'field':'age',
				'data_type':'INTEGER',
				}
			]

@pytest.fixture(scope='function')
def db_conn(set_up_db):
	#db is initalized on first connection
	return Sqlite_Connection(set_up_db)


@pytest.fixture(scope='function')
def test_table(request, set_up_db, drop_table_sql):
	table = 'test_table'
	def tear_down_table():
		with Sqlite_Connection(set_up_db) as conn:
			conn.drop_table(table)
			

	def new_conn(fields):
		#fields need to a list of dicts like [{'field':_, 'data_type':_, 'extra':_}, ...]
		conn = Sqlite_Connection(set_up_db)
		conn.create_table(table, fields)
		return conn, table


	request.addfinalizer(tear_down_table)	
	return new_conn


class Test_Creat_Table:
	def test_create_table(self, db_conn, new_table_fields, drop_table_sql):
		table = 'test_create'
		with db_conn:
			db_conn.create_table(table, new_table_fields)
			assert [table] == [table[0] for table in db_conn.all_tables()]
			#tear down: remove the table
			db_conn.cur.execute(drop_table_sql(table))
			#assert that all tables have been removed
			assert len([table[0] for table in db_conn.all_tables()]) == 0

	def test_custom_sql(self, db_conn, drop_table_sql):
		table = 'test_custom'
		custom_sql = f'CREATE TABLE IF NOT EXISTS {table} (id integer primary key, name text not null);'
		with db_conn:
			db_conn.create_table(custom_sql=custom_sql)
			assert [table] == [table[0] for table in db_conn.all_tables()]
			#tear down: remove the table
			db_conn.cur.execute(drop_table_sql(table))
			#assert that all tables have been removed
			assert len([table[0] for table in db_conn.all_tables()]) == 0

	def test_create_from_tuple(self, db_conn, drop_table_sql):
		table = 'test_tuples'
		fields = [('id', 'INTEGER'), ('name', 'TEXT', 'NOT NULL')]
		with db_conn:
			db_conn.create_table(table, fields)
			assert [table] == [table[0] for table in db_conn.all_tables()]
			#tear down: remove the table
			db_conn.cur.execute(drop_table_sql(table))
			#assert that all tables have been removed
			assert len([table[0] for table in db_conn.all_tables()]) == 0

class Test_Create_Table_Errors:
	@pytest.mark.skip('fill in test case')
	def test_wrong_data_type(self):
		# _validate_dict_keys in create_table
		pass

	@pytest.mark.skip('fill in test case')
	def test_wrong_key(sefl):
		# _validate_dict_keys in create_table
		pass

	@pytest.mark.skip('fill in test case')
	def test_incorrect_data_tuple(self):
		# _convert_tuple_to_dict in create_table
		pass

	@pytest.mark.skip('fill in test case')
	def test_not_tuple_or_dict_list(self):
		# _check_fields in create_table
		pass


class Test_Drop_Table:
	def test_drop(self, test_table):
		fields = [('id', 'INTEGER')]
		new_conn, table_name = test_table(fields)
		assert [table_name] == [table[0] for table in new_conn.all_tables()]
		with new_conn:
			#removes the table
			new_conn.drop_table(table_name)
			assert len([table[0] for table in new_conn.all_tables()]) == 0


class Test_Insert_Data_Default:
	def test_single_field_single_insert(self, test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		data = [(9,),]
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: 9
			new_conn.insert_into(table_name, (fields[0]['field'],), data[0])
			#asserts that the select statemtn returns only [(9,)]
			assert data == [row for row in new_conn.cur.execute(select_all(table_name))]

	def test_single_field_multi_insert(self,test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		data = [(9,),(10,), (11,), (12,)]
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: [(9,),(10,), (11,), (12,)]
			new_conn.insert_into(table_name, (fields[0]['field'],), data)
			#asserts that the select statemtn returns [(9,),(10,), (11,), (12,)]
			assert data == [row for row in new_conn.cur.execute(select_all(table_name))]


	def test_multi_field_single_insert(self, test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'}, {'field':'name', 'data_type':'TEXT'}]
		data = [(9, 'Tom')]
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: 9
			new_conn.insert_into(table_name, (fields[0]['field'], fields[1]['field']), data[0])
			#asserts that the select statemtn returns only [(9, 'Tom')]
			assert data == [row for row in new_conn.cur.execute(select_all(table_name))]

	def test_multi_field_multi_insert(self,test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'}, {'field':'name', 'data_type':'TEXT'}]
		data = [(9, 'Tom'),(10, 'Bob'), (11, 'Jack'), (12, 'Yacin')]
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: 9
			new_conn.insert_into(table_name, (fields[0]['field'], fields[1]['field']), data)
			#asserts that the select statemtn returns [(9, 'Tom'),(10, 'Bob'), (11, 'Jack'), (12, 'Yacin')]
			assert data == [row for row in new_conn.cur.execute(select_all(table_name))]


class Test_Insert_Data_From_Dict:
	def test_single_field_single_insert(self, test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		data_dict = {'age':[9,]}
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: 9
			new_conn.insert_into(table_name, data_dict=data_dict)
			#asserts that the select statemtn returns only [(9,)]
			assert [(9,)] == [row for row in new_conn.cur.execute(select_all(table_name))]

	def test_single_field_multi_insert(self,test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		data_dict = {'age':[9,10, 11, 12]}
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: [(9,),(10,), (11,), (12,)]
			new_conn.insert_into(table_name, data_dict=data_dict)
			#asserts that the select statemtn returns [(9,),(10,), (11,), (12,)]
			assert [(9,),(10,), (11,), (12,)] == [row for row in new_conn.cur.execute(select_all(table_name))]


	def test_multi_field_single_insert(self, test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'}, {'field':'name', 'data_type':'TEXT'}]
		data_dict = {'age':[9,], 'name':['Tom',]}
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: 9
			new_conn.insert_into(table_name, data_dict=data_dict)
			#asserts that the select statemtn returns only [(9, 'Tom')]
			assert [(9, 'Tom')] == [row for row in new_conn.cur.execute(select_all(table_name))]

	def test_multi_field_multi_insert(self,test_table, select_all):
		fields = [{'field':'age', 'data_type':'INTEGER'}, {'field':'name', 'data_type':'TEXT'}]
		data_dict = {'age':[9,10, 11, 12], 'name':['Tom','Bob', 'Jack', 'Yacin']}
		new_conn, table_name = test_table(fields)		
		with new_conn:
			#adds data to the table: field: age, data: 9
			new_conn.insert_into(table_name, data_dict=data_dict)
			#asserts that the select statemtn returns [(9, 'Tom'),(10, 'Bob'), (11, 'Jack'), (12, 'Yacin')]
			assert [(9, 'Tom'),(10, 'Bob'), (11, 'Jack'), (12, 'Yacin')] == [row for row in new_conn.cur.execute(select_all(table_name))]



class Test_Insert_Data_Erros:
	def test_feild_error_non_tuple(self, test_table):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		new_conn, table_name = test_table(fields)	
		with pytest.raises(TypeError) as err:
			with new_conn:
				new_conn.insert_into(table_name, fields[0]['field'], (9,))
		assert str(err.value) == 'fields must be a tupel, while data can be a tuple or a list of tuples'


	def test_data_error_non_tuple_or_list(self, test_table):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		new_conn, table_name = test_table(fields)	
		with pytest.raises(TypeError) as err:
			with new_conn:
				new_conn.insert_into(table_name, (fields[0]['field'],), 9)
		assert str(err.value) == 'fields must be a tupel, while data can be a tuple or a list of tuples'

	def test_data_list_non_tuple(self, test_table):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		new_conn, table_name = test_table(fields)	
		with pytest.raises(TypeError) as err:
			with new_conn:
				new_conn.insert_into(table_name, (fields[0]['field'],), [9,])
		assert str(err.value) == 'Data can be a tuple or a list of tuples'

	def test_uneven_size_field_and_data(self, test_table):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		new_conn, table_name = test_table(fields)	
		with pytest.raises(ValueError) as err:
			with new_conn:
				new_conn.insert_into(table_name, (fields[0]['field'],), (9,2))
		assert str(err.value) == 'Each field must correspond with a single data point'

	def test_uneven_size_field_and_data_list(self, test_table):
		fields = [{'field':'age', 'data_type':'INTEGER'},]
		new_conn, table_name = test_table(fields)	
		with pytest.raises(ValueError) as err:
			with new_conn:
				new_conn.insert_into(table_name, (fields[0]['field'],), [(9,), (2,), (12, 7), (12, 12, 14),])
		assert str(err.value) == ('\nrow 2 has 1 feild(s) and 2 inputs'
								'\nrow 3 has 1 feild(s) and 3 inputs')






if __name__ == '__main__':
	pytest.main()