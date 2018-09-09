'''connection parameters 

database: the name of the database that you want to connect.
user: the username used to authenticate.
password: password used to authenticate.
host: database server address e.g., localhost or an IP address
port: the port number that defaults to 5432 if it is not provided.
'''
# import psycopg2


DATABASE = os.environ['DATABASE']
USER = os.environ['USER']
PASSWORD = os.environ['PASSWORD']
HOST = os.environ['HOST']
PORT = os.environ['PORT']


class Postgres_Connection:
	def __init__(self, db, user, password, host, port):
		self.conn = psycopg2.connect()