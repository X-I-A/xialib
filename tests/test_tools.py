import configparser
import sqlite3
from xialib.tools import *

config = configparser.ConfigParser()
conn = sqlite3.connect(':memory:')

config['adaptor'] = {}
config['adaptor']['module'] = 'xialib'
config['adaptor']['class'] = 'SQLiteAdaptor'
config['adaptor']['password'] = '@secret@.hi'
config['adaptor']['data_encode'] = '@json@.["blob", "flat"]'

config['storer.1'] = {}
config['storer.1']['module'] = 'xialib'
config['storer.1']['class'] = 'BasicStorer'

config['storer.2'] = {}
config['storer.2']['module'] = 'xialib'
config['storer.2']['class'] = 'BasicStorer'

def get_secret(key: str):
    return 'Hello World'

def test_get_single_object():
    storer = get_object(secret_manager=get_secret, db=conn, **config['adaptor'])

def test_get_object_list():
    storers = get_object_list({key: value for key, value in config.items() if key.startswith('storer.')})

def test_get_object_dict():
    storer_dict = get_object_dict({key: value for key, value in config.items() if key.startswith('storer.')})