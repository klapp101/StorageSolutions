import requests
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import xmltodict
import pyodbc
import pymssql
import sqlalchemy 
from sqlalchemy import create_engine,inspect
import urllib
import json
import warnings
warnings.filterwarnings('ignore')

server = 'server\server'
username = 'username'
password = 'pw'
driver = 'ODBC Driver 17 for SQL Server'
database = 'db'
conn_str = (
r'DRIVER={ODBC Driver 17 for SQL Server};'
r'SERVER=server\server;'
r'DATABASE=db;'
r'Trusted_Connection=yes;' )

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn_str, fast_executemany=True)
conn = engine.connect()

class StorageSolutionsBoard:
    def __init__(self):
        pass
    def get_token(self):
        data = {
            'f': 'login',
            'username': 'user',
            'password': 'pw'
        }

        url = 'localhost/?'
        url = requests.get(url,params=data)
        xml_parse = xmltodict.parse(url.text)
        self.api_token = xml_parse['resp']['out']['token']
        print('API Token Received!')

    def board_create(self):
        sql_query = pd.read_sql_query('SELECT * FROM [db].[j].[BC]', conn)

        for i,series in sql_query.iterrows():
        # localhost/#FuncPosboard_create
            data = {
                'f': 'board_create',
                'tkn': self.api_token,
                'board_code': series['board_code'],
                'board_revision': series['board_revision'],
                'board_description': series['board_description'],
                'items': str(series['itemid']) +',' + str(series['quantity']) + ',,,' + '(' + series['manufacturer'] + ',' + series['mpn'] + ')'
            }

            url_item_list = f'localhost/?'
            url = requests.post(url_item_list,params=data)
            xml_parse = xmltodict.parse(url.text)
            print(xml_parse)

    def board_delete(self):
        # localhost/#FuncPosboard_delete
        data = {
            'f': 'board_delete',
            'tkn': self.api_token,
            'board_id': '770'
        }

        url_item_list = f'localhost/?'
        url = requests.post(url_item_list,params=data)
        xml_parse = xmltodict.parse(url.text)
        print(xml_parse)

b = StorageSolutionsBoard()
b.get_token()
b.board_create()
# b.board_delete()
