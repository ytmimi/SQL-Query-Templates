'''
Several pre defined query strings to use when interacting with an SQL database
'''
import os
from jinja2 import Environment, FileSystemLoader, select_autoescape, Template
PATH = os.path.dirname(__file__)
env = Environment(loader=FileSystemLoader(os.path.join(PATH, 'templates')))

# =	Equal to
# <> or !=	Not equal to
# <	Less than
# >	Greater than
# <=	Less than or equal to
# >=	Greater than or equal to
# ALL	returns 1 if all expressions are 1.
# AND	returns 1 if both expressions are 1, and 0 if one of the expressions is 0.
# ANY	returns 1 if any one of a set of comparisons is 1.
# BETWEEN	returns 1 if a value is within a range.
# EXISTS	returns 1 if a subquery contains any rows.
# IN	returns 1 if a value is in a list of values.
# LIKE	returns 1 if a value matches a pattern
# NOT	reverses the value of other operators such as NOT EXISTS, NOT IN, NOT BETWEEN, etc.
# OR	returns true if either expression is 1

OPERATORS = ['=', '<>', '!=', '<', '>', '<=', '>=', 'ALL', 'AND', 'ANY', 'BETWEEN', 'EXISTS', 'IN', 'LIKE', 'NOT', 'OR']


def all_sqlite_tables():
	return "SELECT name FROM sqlite_master WHERE type='table';"

def select(table: str, fields, search_condition=None, operator=('=',),logic_operator=('AND',), 
			order_by=None, order=None, having=None, place_holder='?', limit=None, subquery=None):
	'''
	table_name: name of the db table to search or name of a subquery
	fields: an iterable with the db fields for the given table
	search_condition: an iterable of search fields. Used with the WHERE clause
	operators: an iterable containg valid operators ex) '=', '>=' 'LIKE', 'NOT', 'OR'
				should be the same length as search condition
	logic_operator: if len(search_condition) > 1 a logic operator like 'AND', 'OR'  must be specified
	order_by: an iterable of fields to order the data by
	order: an iterable containing 'ASC' or 'DESC' to indicate how each field in order_by should be ordered
			if left as None, 'ASC' will be the default behavior of the query
	having: a list of having conditions
	place_holder: symbol to use to represent the placeholder variable in the sql string
	limit: a dict {'limit': _, 'offset': _}, where each value is an integer
	subquery: a string representing a nested query string.
	'''
	temp = env.get_template('select.txt')
	return temp.render(table_name=table, fields=fields, condition=search_condition,
						operator=operator, logic_operator=logic_operator, 
						order_by=order_by, order=order, having=having, p=place_holder, 
						limit=limit, subquery=subquery)

def foreign_key(foreign_table, foreign_key):
	temp = env.get_template('foreign_key.txt')
	return temp.render(f_table=foreign_table, f_key=foreign_key)

def create_table(table_name: str, values: list):
	'''
	values: a list of dicts like [{'field':_, 'data_type':_, 'extra':_}, ...]
		where field is a db column,
		      data_type: is one of NULL, INTEGER, REAL, TEXT, BLOB
			  extra: is an sql identifier PRIMARY KEY, or NOT NULL
	'''
	temp = env.get_template('create_table.txt')
	return temp.render(table_name=table_name, values=values)

def insert_into(table_name: str, values: list, place_holder='?'):
	temp = env.get_template('insert_into.txt')
	return temp.render(table_name=table_name, values=values, p=place_holder)


def update_table(table: str, fields, update_field: str, place_holder='?'):
	'''fields an iterable (list or tuple) that contains the fields to update
		update_field: field that goes after the WHERE clause
	'''
	temp = env.get_template('update_table.txt')
	return temp.render(table=table, fields=fields, 
						update_field=update_field, p=place_holder)
	
def drop_table(table: str):
	return Template('DROP TABLE {{table}};').render(table=table)


def table_fields(table: str):
	return Template('PRAGMA table_info({{table}});').render(table=table)

def delete_field(table: str):
	# General form: DELETE FROM table WHERE search_condition;
	# (optional additions) ORDER BY criteria LIMIT row_count OFFSET offset;
	pass


if __name__ == '__main__':
	print(update_table('test', ['name'], 'id'))
	print(select('test', ('id','name')))

