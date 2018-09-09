import os
import sys
#adds the top level directory to the path
path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(path)

import pytest
import templates.query_string as qs


@pytest.fixture(scope='module')
def new_values():
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


@pytest.fixture(scope='module')
def test_fields():
	return {
		'table':'test',
		'field': ('id'),
		'wild': ('*'),
		'fields':('id', 'age', 'name',),
		'sing_search':('age',),
		'multi_search':('age', 'name',),
		'tri_search':('age', 'first', 'last',)
	}

class Test_Select_Basic:
	def test_single_field(self):
		assert qs.select('test', ('name',)) == 'SELECT name FROM test;'

	def test_multiple_fields(self, test_fields):
		query = qs.select(test_fields['table'], test_fields['fields'])
		assert query == 'SELECT id, age, name FROM test;'

	def test_wildcard(self):
		query = qs.select('test', ('*',))
		assert query == 'SELECT * FROM test;'


class Test_Select_WHERE:
	def test_single_search_conditions(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition)
		assert query == 'SELECT * FROM test WHERE age = ?;'

	def test_multi_search_conditions(self, test_fields):
		table = test_fields['table']
		condition = test_fields['multi_search']
		query = qs.select(table, ('*',), condition, operator=('=', '>',))
		assert query == 'SELECT * FROM test WHERE age = ? AND name > ?;'

	def test_multi_search_conditions_new_placeholder(self, test_fields):
		table = test_fields['table']
		condition = test_fields['multi_search']
		query = qs.select(table, ('*',), condition, operator=('=', '=',), 
			logic_operator=('OR',), place_holder='%s')
		assert query == 'SELECT * FROM test WHERE age = %s OR name = %s;'

	
class Test_Select_Single_Operator:
	def test_greater_than(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('>',),)
		assert query == 'SELECT * FROM test WHERE age > ?;'

	def test_greater_than_or_equal(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('>=',),)
		assert query == 'SELECT * FROM test WHERE age >= ?;'

	def test_less_than(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('<',),)
		assert query == 'SELECT * FROM test WHERE age < ?;'

	def test_less_than_or_equal(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('<=',),)
		assert query == 'SELECT * FROM test WHERE age <= ?;'

	def test_not_equal_1(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('!=',),)
		assert query == 'SELECT * FROM test WHERE age != ?;'

	def test_not_equal_2(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('<>',),)
		assert query == 'SELECT * FROM test WHERE age <> ?;'

	def test_like(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('LIKE',),)
		assert query == 'SELECT * FROM test WHERE age LIKE ?;'

	def test_in(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('IN',),)
		assert query == 'SELECT * FROM test WHERE age IN ?;'

	def test_in(self, test_fields):
		table = test_fields['table']
		condition = test_fields['sing_search']
		query = qs.select(table, ('*',), condition, operator=('BETWEEN',),)
		assert query == 'SELECT * FROM test WHERE age BETWEEN ? AND ?;'


class Test_Select_Multi_Operator:
	def test_in(self, test_fields):
		table = test_fields['table']
		condition = test_fields['tri_search']
		query = qs.select(table, ('*',), condition, operator=('<','LIKE', '='),logic_operator=('AND', 'OR'))
		assert query == 'SELECT * FROM test WHERE age < ? AND first LIKE ? OR last = ?;'


class Test_Select_Order_By:
	def test_default(self,test_fields):
		table = test_fields['table']
		order_by = test_fields['sing_search']
		query = qs.select(table, ('*',), order_by=order_by)
		assert query == 'SELECT * FROM test ORDER BY age;'

	def test_explicit_ASC(self, test_fields):
		table = test_fields['table']
		order_by = test_fields['sing_search']
		query = qs.select(table, ('*',), order_by=order_by, order=('ASC',))
		assert query == 'SELECT * FROM test ORDER BY age ASC;'

	def test_explicit_DESC(self, test_fields):
		table = test_fields['table']
		order_by = test_fields['sing_search']
		query = qs.select(table, ('*',), order_by=order_by, order=('DESC',))
		assert query == 'SELECT * FROM test ORDER BY age DESC;'

	def test_multi_order(self, test_fields):
		order_by = test_fields['multi_search']
		query = qs.select(test_fields['table'], ('*',), order_by=order_by,)
		assert query == 'SELECT * FROM test ORDER BY age, name;'		

	def test_multi_explicit_ASC(self, test_fields):
		order_by = test_fields['multi_search']
		query = qs.select(test_fields['table'], ('*',), order_by=order_by, order=('ASC', 'ASC'))
		assert query == 'SELECT * FROM test ORDER BY age ASC, name ASC;'		

	def test_multi_explicit_DESC(self, test_fields):
		order_by = test_fields['multi_search']
		query = qs.select(test_fields['table'], ('*',), order_by=order_by,order=('DESC', 'DESC'))
		assert query == 'SELECT * FROM test ORDER BY age DESC, name DESC;'

	def test_multi_mixed_order(self, test_fields):
		order_by = test_fields['multi_search']
		query = qs.select(test_fields['table'], ('*',), order_by=order_by, order=('ASC', 'DESC'))
		assert query == 'SELECT * FROM test ORDER BY age ASC, name DESC;'

	def test_order_with_Where(self, test_fields):
		order_by = test_fields['sing_search']
		condition = test_fields['multi_search']
		query = qs.select(test_fields['table'], ('*',), search_condition=condition, 
							operator=('=', 'LIKE'), order_by=order_by, order=('DESC',))
		assert query == 'SELECT * FROM test WHERE age = ? AND name LIKE ? ORDER BY age DESC;'


class Test_Select_Limit:
	def test_no_offset(self, test_fields):
		limit = {'limit':5}
		query = qs.select(test_fields['table'], ('*',), limit=limit)
		assert query == 'SELECT * FROM test LIMIT 5;'

	def test_with_offset(self, test_fields):
		limit = {'limit':5, 'offset':7}
		query = qs.select(test_fields['table'], ('*',), limit=limit)
		assert query == 'SELECT * FROM test LIMIT 5 OFFSET 7;'

	def test_after_order(self, test_fields):
		order_by = test_fields['sing_search']
		limit = {'limit':5}
		query = qs.select(test_fields['table'], ('*',), order_by=order_by, order=('DESC',), limit=limit)
		assert query == 'SELECT * FROM test ORDER BY age DESC LIMIT 5;'



@pytest.fixture(scope='module')
def agregate():
	def avg(val):
		return f'avg({val})'

	def count(val):
		return f'count({val})'	

	def max_(val):
		return f'max({val})'

	def min_(val):
		return f'min({val})'

	def sum_(val):
		return f'sum({val})'

	return {
		'avg':avg,
		'count':count,
		'max':max_,
		'min':min_,
		'sum':sum_,
	}


class Test_Select_Aggregate_Functions:
	def test_average(self, test_fields, agregate):
		avg = agregate['avg']('age')
		test_fields['sing_search']
		query = qs.select(test_fields['table'], (avg,))
		assert query == f'SELECT avg(age) FROM test;'

	def test_count(self, test_fields, agregate):
		count = agregate['count']('age')
		test_fields['sing_search']
		query = qs.select(test_fields['table'], (count,))
		assert query == f'SELECT count(age) FROM test;'

	def test_max(self, test_fields, agregate):
		max_ = agregate['max']('age')
		test_fields['sing_search']
		query = qs.select(test_fields['table'], (max_,))
		assert query == f'SELECT max(age) FROM test;'

	def test_min(self, test_fields, agregate):
		min_ = agregate['min']('age')
		test_fields['sing_search']
		query = qs.select(test_fields['table'], (min_,))
		assert query == f'SELECT min(age) FROM test;'

	def test_average(self, test_fields, agregate):
		sum_ = agregate['sum']('age')
		test_fields['sing_search']
		query = qs.select(test_fields['table'], (sum_,))
		assert query == f'SELECT sum(age) FROM test;'


@pytest.mark.skip('fill in test case')
class Test_Select_Sub_Query:
	def test_sub_query(self):
		pass



def test_foriegn_key():
	value = qs.foreign_key('supplier_groups', 'group_id')
	assert value == 'FOREIGN KEY (group_id) REFERENCES supplier_groups (group_id)'

class Test_Create_Table:
	def test_single_field(self, new_values):
		fields = [new_values[0]]
		execute = qs.create_table('test', fields)
		assert execute == 'CREATE TABLE IF NOT EXISTS test (id INTEGER NOT NULL);'

	def test_multi_fied(self, new_values):
		execute = qs.create_table('test', new_values)
		assert execute == 'CREATE TABLE IF NOT EXISTS test (id INTEGER NOT NULL, age INTEGER);'

class Test_Insert_Into:
	def test_questing_placeholder(self):
		execute = qs.insert_into('test', ['id', 'name'], place_holder='?')
		assert execute == 'INSERT INTO test(id, name) VALUES (?, ?);'

	def test_percent_s_placeholder(self):
		execute = qs.insert_into('test', ['id', 'name'], place_holder='%s')
		assert execute == 'INSERT INTO test(id, name) VALUES (%s, %s);'

class Test_Update_Table:
	def test_questing_placeholder(self):
		value = qs.update_table('tasks', ('priority', 'begin_date', 'end_date'), 'id', place_holder='?')
		assert value == 'UPDATE tasks SET priority = ?, begin_date = ?, end_date = ? WHERE id = ?'

	def test_percent_s_placeholder(self):
		value = qs.update_table('tasks', ('priority', 'begin_date', 'end_date'), 'id', place_holder='%s')
		assert value == 'UPDATE tasks SET priority = %s, begin_date = %s, end_date = %s WHERE id = %s'


def test_drop_table():
	assert qs.drop_table('test') == 'DROP TABLE test;'



if __name__ == '__main__':
	pytest.main()







